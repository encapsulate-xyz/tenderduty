package tenderduty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"

	dash "github.com/blockpane/tenderduty/v2/td2/dashboard"
	rpchttp "github.com/tendermint/tendermint/rpc/client/http"
)

// newRpc sets up the rpc client used for monitoring. It will try nodes in order until a working node is found.
// it will also get some initial info on the validator's status.
func (cc *ChainConfig) newRpc() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var anyWorking bool // if healthchecks are running, we will skip to the first known good node.
	for _, endpoint := range cc.Nodes {
		anyWorking = anyWorking || !endpoint.down
	}
	// grab the first working endpoint
	tryUrl := func(u string) (msg string, down, syncing bool) {
		_, err := url.Parse(u)
		if err != nil {
			msg = fmt.Sprintf("❌ could not parse url %s: (%s) %s", cc.name, u, err)
			l(msg)
			down = true
			return
		}
		cc.client, err = rpchttp.New(u, "/websocket")
		if err != nil {
			msg = fmt.Sprintf("❌ could not connect client for %s: (%s) %s", cc.name, u, err)
			l(msg)
			down = true
			return
		}
		status, err := cc.client.Status(ctx)
		if err != nil {
			msg = fmt.Sprintf("❌ could not get status for %s: (%s) %s", cc.name, u, err)
			down = true
			l(msg)
			return
		}
		if status.NodeInfo.Network != cc.ChainId {
			msg = fmt.Sprintf("chain id %s on %s does not match, expected %s, skipping", status.NodeInfo.Network, u, cc.ChainId)
			down = true
			l(msg)
			return
		}
		if status.SyncInfo.CatchingUp {
			msg = fmt.Sprint("🐢 node is not synced, skipping ", u)
			syncing = true
			down = true
			l(msg)
			return
		}
		cc.noNodes = false
		return
	}
	down := func(endpoint *NodeConfig, msg string) {
		if !endpoint.down {
			endpoint.down = true
			endpoint.downSince = time.Now()
		}
		endpoint.lastMsg = msg
	}
	for _, endpoint := range cc.Nodes {
		if anyWorking && endpoint.down {
			continue
		}
		if msg, failed, syncing := tryUrl(endpoint.Url); failed {
			endpoint.syncing = syncing
			down(endpoint, msg)
			continue
		}
		return nil
	}
	if cc.PublicFallback {
		if u, ok := getRegistryUrl(cc.ChainId); ok {
			node := guessPublicEndpoint(u)
			l(cc.ChainId, "⛑ attemtping to use public fallback node", node)
			if _, kk, _ := tryUrl(node); !kk {
				l(cc.ChainId, "⛑ connected to public endpoint", node)
				return nil
			}
		} else {
			l("could not find a public endpoint for", cc.ChainId)
		}
	}
	cc.noNodes = true
	alarms.clearAll(cc.name)
	cc.lastError = "no usable RPC endpoints available for " + cc.ChainId
	if td.EnableDash {
		td.updateChan <- &dash.ChainStatus{
			MsgType:      "status",
			Name:         cc.name,
			ChainId:      cc.ChainId,
			Moniker:      cc.valInfo.Moniker,
			Bonded:       cc.valInfo.Bonded,
			Jailed:       cc.valInfo.Jailed,
			Tombstoned:   cc.valInfo.Tombstoned,
			Missed:       cc.valInfo.Missed,
			Window:       cc.valInfo.Window,
			Nodes:        len(cc.Nodes),
			HealthyNodes: 0,
			ActiveAlerts: 1,
			Height:       0,
			LastError:    cc.lastError,
			Blocks:       cc.blocksResults,
		}
	}
	return errors.New("no usable endpoints available for " + cc.ChainId)
}

func (cc *ChainConfig) monitorHealth(ctx context.Context, chainName string) {
	tick := time.NewTicker(time.Minute)
	if cc.client == nil {
		_ = cc.newRpc()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			var err error
			for _, node := range cc.Nodes {
				go func(node *NodeConfig) {
					alert := func(msg string) {
						node.lastMsg = fmt.Sprintf("%-12s node %s is %s", chainName, node.Url, msg)
						if !node.AlertIfDown {
							// even if we aren't alerting, we want to display the status in the dashboard.
							node.down = true
							return
						}
						if !node.down {
							node.down = true
							node.downSince = time.Now()
						}
						if td.Prom {
							td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, time.Since(node.downSince).Seconds(), node.Url)
						}
						l("⚠️ " + node.lastMsg)
					}
					c, e := rpchttp.New(node.Url, "/websocket")
					if e != nil {
						alert(e.Error())
					}
					cwt, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					status, e := c.Status(cwt)
					cancel()
					if e != nil {
						alert("down")
						return
					}
					if status.NodeInfo.Network != cc.ChainId {
						alert("on the wrong network")
						return
					}
					if status.SyncInfo.CatchingUp {
						alert("not synced")
						node.syncing = true
						return
					}

					// node's OK, clear the note
					if node.down {
						node.lastMsg = ""
						node.wasDown = true
					}
					td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, 0, node.Url)
					node.down = false
					node.syncing = false
					node.downSince = time.Unix(0, 0)
					cc.noNodes = false
					l(fmt.Sprintf("🟢 %-12s node %s is healthy", chainName, node.Url))
				}(node)
			}

			if cc.client == nil {
				e := cc.newRpc()
				if e != nil {
					l("💥", cc.ChainId, e)
				}
			}
			if cc.valInfo != nil {
				cc.lastValInfo = &ValInfo{
					Moniker:    cc.valInfo.Moniker,
					Bonded:     cc.valInfo.Bonded,
					Jailed:     cc.valInfo.Jailed,
					Tombstoned: cc.valInfo.Tombstoned,
					Missed:     cc.valInfo.Missed,
					Window:     cc.valInfo.Window,
					Conspub:    cc.valInfo.Conspub,
					Valcons:    cc.valInfo.Valcons,
				}
			}
			err = cc.GetValInfo(false)
			if err != nil {
				l("❓ refreshing signing info for", cc.ValAddress, err)
			}
		}
	}
}

// healthcheckInterval is how often to ping. ping_rate is a number of seconds, so "60" decodes to
// 60ns and is scaled up; a value already a second or more ("60s") is used as is. A missing or
// zero value would make time.NewTicker panic, so it falls back to 60 seconds.
func healthcheckInterval(rate time.Duration) time.Duration {
	switch {
	case rate <= 0:
		return 60 * time.Second
	case rate < time.Second:
		return rate * time.Second
	default:
		return rate
	}
}

// pingHealthcheckURL makes one ping. The URL is a secret (anyone holding it can keep the check
// green), so the error never contains it: *url.Error is unwrapped to the cause.
func pingHealthcheckURL(client *http.Client, pingURL string) error {
	resp, err := client.Get(pingURL)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			return ue.Err
		}
		return errors.New("request failed")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// pingHealthcheck pings the dead man's switch until tenderduty stops, so the service behind the
// URL alerts when tenderduty itself is down. It logs the first success and every change between
// failing and working, never the URL.
func (c *Config) pingHealthcheck() {
	if !c.Healthcheck.Enabled {
		return
	}
	if c.Healthcheck.PingURL == "" {
		l("❌ healthcheck is enabled but ping_url is empty, not pinging")
		return
	}
	interval := healthcheckInterval(c.Healthcheck.PingRate)
	client := &http.Client{Timeout: 10 * time.Second}
	l(fmt.Sprintf("🏓 healthcheck: pinging every %s", interval))

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		ok, logged := false, false
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				err := pingHealthcheckURL(client, c.Healthcheck.PingURL)
				switch {
				case err != nil && (ok || !logged):
					l("❌ healthcheck ping failed:", err)
				case err == nil && (!ok || !logged):
					l("🏓 healthcheck ping ok")
				}
				ok, logged = err == nil, true
			}
		}
	}()
}

// endpointRex matches the first a tag's hostname and port if present.
var endpointRex = regexp.MustCompile(`//([^/:]+)(:\d+)?`)

// guessPublicEndpoint attempts to deal with a shortcoming in the tendermint RPC client that doesn't allow path prefixes.
// The cosmos.directory requires them. This is a workaround to get the actual URL for the server behind their proxy.
// The RPC base URL will return links endpoints, and we can parse this to guess the original URL.
func guessPublicEndpoint(u string) string {
	resp, err := http.Get(u + "/")
	if err != nil {
		return u
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return u
	}
	_ = resp.Body.Close()
	matches := endpointRex.FindStringSubmatch(string(b))
	if len(matches) < 2 {
		// didn't work
		return u
	}
	proto := "https://"
	port := ":443"
	// will be 3 elements if there is a port no port means listening on https
	if len(matches) == 3 && matches[2] != "" && matches[2] != ":443" {
		proto = "http://"
		port = matches[2]
	}
	return proto + matches[1] + port
}
