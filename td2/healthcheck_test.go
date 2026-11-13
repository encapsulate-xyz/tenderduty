package tenderduty

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const secretPath = "/ping/0b1c2d3e-secret-uuid"

func TestHealthcheckInterval(t *testing.T) {
	for in, want := range map[time.Duration]time.Duration{
		0:                60 * time.Second,
		-5:               60 * time.Second,
		60:               60 * time.Second, // ping_rate: 60
		30:               30 * time.Second,
		45 * time.Second: 45 * time.Second, // ping_rate: 45s
		2 * time.Minute:  2 * time.Minute,
	} {
		if got := healthcheckInterval(in); got != want {
			t.Errorf("healthcheckInterval(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestPingHealthcheckURL(t *testing.T) {
	status := int32(200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/slow") {
			time.Sleep(300 * time.Millisecond)
		}
		w.WriteHeader(int(atomic.LoadInt32(&status)))
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()
	client := &http.Client{Timeout: 100 * time.Millisecond}

	if err := pingHealthcheckURL(client, srv.URL+secretPath); err != nil {
		t.Fatalf("200: got %v, want nil", err)
	}
	atomic.StoreInt32(&status, 500)
	if err := pingHealthcheckURL(client, srv.URL+secretPath); err == nil || err.Error() != "status 500" {
		t.Fatalf("500: got %v, want status 500", err)
	}
	atomic.StoreInt32(&status, 200)
	for name, u := range map[string]string{
		"timeout":     srv.URL + "/slow" + secretPath,
		"refused":     "http://127.0.0.1:1" + secretPath,
		"bad address": "http://no-such-host.invalid" + secretPath,
	} {
		err := pingHealthcheckURL(client, u)
		if err == nil {
			t.Errorf("%s: got nil error", name)
			continue
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "/ping/") {
			t.Errorf("%s: error leaks the ping URL: %q", name, err)
		}
	}
}

func TestPingHealthcheckPingsUntilStopped(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := &Config{ctx: ctx, cancel: cancel, Healthcheck: HealthcheckConfig{Enabled: true, PingURL: srv.URL + secretPath, PingRate: 1}}
	c.pingHealthcheck()
	time.Sleep(2500 * time.Millisecond)
	cancel()
	n := atomic.LoadInt32(&hits)
	if n != 2 {
		t.Fatalf("got %d pings in 2.5s with ping_rate 1, want 2", n)
	}
	time.Sleep(1500 * time.Millisecond)
	if after := atomic.LoadInt32(&hits); after != n {
		t.Fatalf("pings continued after stop: %d then %d", n, after)
	}

	// disabled, or enabled with no URL: nothing is started
	atomic.StoreInt32(&hits, 0)
	for _, hc := range []HealthcheckConfig{{Enabled: false, PingURL: srv.URL, PingRate: 1}, {Enabled: true, PingURL: "", PingRate: 1}} {
		ctx2, cancel2 := context.WithCancel(context.Background())
		(&Config{ctx: ctx2, cancel: cancel2, Healthcheck: hc}).pingHealthcheck()
		time.Sleep(1500 * time.Millisecond)
		cancel2()
	}
	if n := atomic.LoadInt32(&hits); n != 0 {
		t.Fatalf("pinged %d times while disabled or without a URL", n)
	}
}
