// Package chaos holds the fault-detection tests that stay unit-runnable
// after P7 removed the in-controller data path. The data-path chaos tests
// (panic-in-copy recovery, fault-rate recording over a full Manager.Run) are
// deleted: with the two-agent topology the data COPY runs in the source/dest
// agents, and their fault behavior is covered by the gp6lab-tagged
// integration tests (two_agent_crash_matrix) and the lab e2e. What
// remains is the reachability-fault primitive itself (CR-7): a hung segment
// must surface as a failed ping, never as healthy.
package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// delay hook — the doctor/manager must not treat a hung segment as healthy.
func TestHungSegmentIsDetected(t *testing.T) {
	f := pg.NewFakeConn()
	f.SetPingDelay(3 * time.Second)
	// A ping that would block past a short timeout is treated as unreachable.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := f.Ping(ctx); err == nil {
		t.Fatal("expected a hung (delayed) segment to fail ping within the timeout")
	}
}
