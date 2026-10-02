package doctor

import (
	"testing"
)

// TestCheckTopologyCrossHost is the ADR-002 debt check: a cross-host
// (single-controller) copy must surface a WARN naming the two-hop,
// uncompressed-leg topology; a same-endpoint copy must not.
func TestCheckTopologyCrossHost(t *testing.T) {
	t.Run("cross-host with compression warns", func(t *testing.T) {
		r := &Report{Healthy: true}
		r.checkTopology(Options{
			SrcDSN:   "postgres://u@10.0.0.1:5432/db",
			DstDSN:   "postgres://u@10.0.0.2:7432/db",
			Compress: "zstd",
		})
		found := false
		for _, c := range r.Checks {
			if c.Name == "topology" {
				found = true
				if c.Sev != Warn {
					t.Fatalf("topology check sev=%v, want WARN", c.Sev)
				}
				if c.Side != "network" {
					t.Fatalf("topology check side=%q, want network", c.Side)
				}
				if r.Healthy != true {
					t.Fatal("topology must be WARN, not FAIL (Healthy must stay true)")
				}
			}
		}
		if !found {
			t.Fatal("topology check not emitted for a cross-host copy")
		}
	})

	t.Run("two-agent cross-host passes", func(t *testing.T) {
		r := &Report{Healthy: true}
		r.checkTopology(Options{
			SrcDSN:   "postgres://u@10.0.0.1:5432/db",
			DstDSN:   "postgres://u@10.0.0.2:7432/db",
			Compress: "zstd",
			Topology: "two-agent",
		})
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
			t.Fatal("topology check not emitted for a cross-host two-agent copy")
		}
	})

	t.Run("same endpoint does not warn", func(t *testing.T) {
		r := &Report{Healthy: true}
		r.checkTopology(Options{
			SrcDSN:   "postgres://u@10.0.0.1:5432/db",
			DstDSN:   "postgres://u@10.0.0.1:5432/db",
			Compress: "zstd",
		})
		for _, c := range r.Checks {
			if c.Name == "topology" {
				t.Fatal("topology check emitted for a same-endpoint copy (want none)")
			}
		}
	})

	t.Run("empty dsn does not warn", func(t *testing.T) {
		r := &Report{Healthy: true}
		r.checkTopology(Options{SrcDSN: "", DstDSN: "", Compress: "zstd"})
		for _, c := range r.Checks {
			if c.Name == "topology" {
				t.Fatal("topology check emitted for empty DSNs (want none)")
			}
		}
	})
}
