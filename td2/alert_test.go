package tenderduty

import (
	"sync"
	"testing"
	"time"
)

func TestPctAlarmChange(t *testing.T) {
	tests := []struct {
		name          string
		active        bool
		missed        int64
		window        int64
		threshold     int
		fire, cleared bool
	}{
		// 13 Nov 2026: agoric-3 after a restart, 3 missed and the slashing params not read yet
		{"window not known yet, agoric", false, 3, 0, 10, false, false},
		// gravity-bridge-3, same restart, 280 missed
		{"window not known yet, gravity", false, 280, 0, 5, false, false},
		{"window not known, alarm open", true, 3, 0, 10, false, false},
		{"nothing missed", false, 0, 5000, 10, false, false},
		{"under threshold", false, 280, 10000, 5, false, false},
		{"over threshold", false, 600, 5000, 10, true, false},
		{"over threshold, already open", true, 600, 5000, 10, false, false},
		{"exactly at threshold", false, 500, 5000, 10, false, false},
		{"back under threshold clears", true, 3, 5000, 10, false, true},
		{"not open, nothing to clear", false, 3, 5000, 10, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fire, cleared := pctAlarmChange(tt.active, tt.missed, tt.window, tt.threshold)
			if fire != tt.fire || cleared != tt.cleared {
				t.Errorf("pctAlarmChange(%v, %d, %d, %d) = (%v, %v), want (%v, %v)",
					tt.active, tt.missed, tt.window, tt.threshold, fire, cleared, tt.fire, tt.cleared)
			}
		})
	}
}

func TestAlarmIsActive(t *testing.T) {
	a := &alarmCache{notifyMux: sync.RWMutex{}}
	if a.isActive("agoric-mainnet", "x") {
		t.Fatal("empty cache reports an open alarm")
	}
	a.AllAlarms = map[string]map[string]time.Time{
		"agoric-mainnet": {"Encapsulate has missed > 10% of the slashing window's blocks on agoric-3": time.Now()},
	}
	if !a.isActive("agoric-mainnet", "Encapsulate has missed > 10% of the slashing window's blocks on agoric-3") {
		t.Error("restored alarm not reported as open")
	}
	if a.isActive("agoric-mainnet", "Encapsulate has missed > 5% of the slashing window's blocks on agoric-3") {
		t.Error("different message reported as open")
	}
	if a.isActive("gravity-mainnet", "Encapsulate has missed > 10% of the slashing window's blocks on agoric-3") {
		t.Error("alarm reported on the wrong chain")
	}
}
