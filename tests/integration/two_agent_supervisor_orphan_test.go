//go:build gp6lab

package integration

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
)

// TestSupervisorOrphanExit is the P3 HEALTH acceptance: when a mini-agent
// DIES (its persistent report connection drops) before it reports a unit, the
// supervisor's ServeUnit must FAIL THE UNIT (so the run resumes the unit and
// truncates that segment's staging slice on re-copy) instead of hanging or
// silently dropping the segment's contribution. This is the orphan-exit
// guarantee on the coordinator side: a dead mini = a failed unit, never a
// silent partial result.
//
// The test needs no real pg: it starts D "dead" minis — goroutines that dial
// the supervisor's report listener, send their hello (so AcceptMinis
// completes), and then exit, dropping their report connection. ServeUnit then
// sends mini-unit to all D and tries to read each one's report; the dropped
// connections surface as orphan errors, so ServeUnit must return a unit
// failure naming the orphan. No mini actually loads (they die before serving),
// so the test is a pure report-channel health test.
func TestSupervisorOrphanExit(t *testing.T) {
	const D = 2
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sup, err := agent.NewSupervisor("127.0.0.1:0", nil, "", "", "", "")
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	defer sup.Close()
	reportAddr := sup.Addr()

	mmCh := make(chan agent.MiniMap, 1)
	mmErr := make(chan error, 1)
	go func() {
		mm, err := sup.AcceptMinis(ctx, D)
		if err != nil {
			mmErr <- err
			return
		}
		mmCh <- mm
	}()

	// D dead minis: dial the report listener, send the hello (so the
	// supervisor accepts it), then exit — the report connection drops.
	for seg := 0; seg < D; seg++ {
		go func(seg int) {
			conn, derr := net.Dial("tcp", reportAddr)
			if derr != nil {
				return
			}
			defer conn.Close()
			_ = agent.SendLine(conn, agent.MiniHello{Op: "mini-hello", Seg: seg, DataPort: 1, Ready: true})
			// No serve; the connection drops when this goroutine returns.
		}(seg)
	}

	var mm agent.MiniMap
	select {
	case e := <-mmErr:
		t.Fatalf("supervisor accept minis: %v", e)
	case mm = <-mmCh:
	case <-time.After(20 * time.Second):
		t.Fatalf("supervisor did not accept %d minis in time", D)
	}
	if len(mm) != D {
		t.Fatalf("MiniMap has %d entries, want %d", len(mm), D)
	}
	for i := 0; i < D; i++ {
		sup.SetMiniHost(i, "127.0.0.1")
	}

	// Let the dead minis actually drop their connections, then drive the unit.
	time.Sleep(300 * time.Millisecond)

	_, err = sup.ServeUnit(ctx, agent.UnitReportReq{
		ReqID: 1, UnitID: 1, JobID: 1, Schema: "public",
		Staging: "_gs_orphan", Compress: "zstd", VerifyDigest: false,
	})
	if err == nil {
		t.Fatalf("ServeUnit should fail when mini-agents are orphaned (got success)")
	}
	if !strings.Contains(err.Error(), "orphan") {
		t.Fatalf("ServeUnit error %q does not name the orphan", err)
	}
	t.Logf("orphan detected: %v", err)
}
