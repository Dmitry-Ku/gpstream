package manifest

import (
	"net"
	"strconv"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
)

// inprocPrebindsInRange: the in-proc runner (the lab / integration path)
// pre-binds its loopback ports through --data-port-range: every pre-bound
// port lands inside the range, and the per-unit release/reuse keeps many
// units within a small range.
func TestInProcPrebindsInRange(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable"
	cfg.CopyMode = config.CopyCoordinator
	cfg.DataPortRange = "23000-23002"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	r := NewInProcTwoAgentRunner(&cfg)

	// The source control port is held for the whole run (never released) —
	// like the real runner's Start(). It must land in the range.
	ctrlAddr, releaseCtrl, err := r.prebindData()
	if err != nil {
		t.Fatalf("control prebind: %v", err)
	}
	defer releaseCtrl()
	ctrlPort, _ := portOf(t, ctrlAddr)
	if ctrlPort < 23000 || ctrlPort > 23002 {
		t.Fatalf("control port %d outside 23000-23002 (%s)", ctrlPort, ctrlAddr)
	}

	// Per-unit data ports: each lands in the range, never on the held control
	// port, and is released after the unit (the serial reuse pattern) — so 20
	// units fit in the remaining 2 ports.
	for unit := 0; unit < 20; unit++ {
		addr, release, err := r.prebindData()
		if err != nil {
			t.Fatalf("unit %d: %v", unit, err)
		}
		p, _ := portOf(t, addr)
		if p < 23000 || p > 23002 {
			t.Fatalf("unit %d: port %d outside 23000-23002 (%s)", unit, p, addr)
		}
		if p == ctrlPort {
			t.Fatalf("unit %d: data port %d == held control port", unit, p)
		}
		release()
	}
}

// inprocNoRangeUnchanged: without --data-port-range the in-proc prebind is
// the legacy kernel-pick (any port, no range check).
func TestInProcNoRangeUnchanged(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable"
	r := NewInProcTwoAgentRunner(&cfg)
	addr, release, err := r.prebindData()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := portOf(t, addr); err != nil {
		t.Fatal(err)
	}
}

func portOf(t *testing.T, addr string) (int, error) {
	t.Helper()
	_, ps, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(ps)
}
