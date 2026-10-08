package dryrun

import (
	"testing"
)

// TestCheckTopology is the P7 architecture confirmation: the build is always
// two-agent, so the check is a constant PASS (the controller holds no data
// connections). There is no single-controller build left to WARN about.
func TestCheckTopology(t *testing.T) {
	r := &Report{Healthy: true}
	r.checkTopology(Options{})
	found := false
	for _, c := range r.Checks {
		if c.Name == "topology" {
			found = true
			if c.Sev != Pass {
				t.Fatalf("topology check sev=%v, want PASS", c.Sev)
			}
			if c.Side != "network" {
				t.Fatalf("topology check side=%q, want network", c.Side)
			}
		}
	}
	if !found {
		t.Fatal("topology check not emitted")
	}
	if !r.Healthy {
		t.Fatal("topology must be PASS, not FAIL")
	}
}
