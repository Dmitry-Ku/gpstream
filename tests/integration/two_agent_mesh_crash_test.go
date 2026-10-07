//go:build gp6lab

package integration

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
)

// TestMiniOrphanExitOnSupervisorDeath is the P5 mesh crash-matrix scenario
// "kill-supervisor": when the SUPERVISOR dies (its report connection to a
// mini-agent drops), the mini-agent must EXIT IMMEDIATELY (orphan-exit), not
// by its AcceptTimeout/StreamTimeout. This is the coordinator-death side of
// the P2 guarantee: a mini-agent whose report channel is severed (the
// supervisor process is gone) unwinds at once, so the destination cluster is
// not left with a mini-agent wedged on a dead control channel.
//
// The test needs no real pg (no unit is served, so the mini never opens its
// utility COPY). It plays the supervisor: it accepts the real ServeMini's
// report dial, reads its hello, and then CLOSES the report connection
// (simulating the supervisor process dying). The mini's next control read
// (recvLine) sees EOF and ServeMini must return promptly — well within the
// mini's stream timeout — proving the exit is the orphan-exit, not a timeout.
func TestMiniOrphanExitOnSupervisorDeath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The supervisor (this test): a report listener the mini dials.
	reportL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("report listen: %v", err)
	}
	defer reportL.Close()

	// Run a REAL mini-agent. Its PrimaryDSN is a dummy (never dialed: no unit
	// is served, so the utility COPY never runs); it dials the report listener,
	// sends its hello, and waits for a unit.
	resCh := make(chan agent.MiniResult, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := agent.ServeMini(ctx, agent.MiniOptions{
			Seg:           0,
			DataListen:    "127.0.0.1:0",
			PrimaryDSN:    "host=127.0.0.1 port=1 user=gp dbname=dummy sslmode=disable",
			ReportAddr:    reportL.Addr().String(),
			AcceptTimeout: 60 * time.Second, // generous: the exit must NOT be this
			StreamTimeout: 60 * time.Second, // generous: the exit must NOT be this
		})
		if err != nil {
			errCh <- err
			return
		}
		resCh <- res
	}()

	// Accept the mini's report dial + read its hello.
	reportConn, err := reportL.Accept()
	if err != nil {
		t.Fatalf("accept report: %v", err)
	}
	defer reportConn.Close()
	var hello agent.MiniHello
	if err := recvLineFrom(bufio.NewReader(reportConn), &hello); err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if hello.Op != "mini-hello" || hello.Seg != 0 || !hello.Ready {
		t.Fatalf("bad hello: %+v", hello)
	}
	t.Logf("mini hello received: seg=%d dataPort=%d ready=%v", hello.Seg, hello.DataPort, hello.Ready)

	// KILL THE SUPERVISOR: close the report connection. The mini's next
	// control read sees EOF -> orphan-exit.
	reportConn.Close()

	// The mini must exit promptly (the orphan-exit, not the 60s timeouts).
	select {
	case e := <-errCh:
		t.Fatalf("mini agent: %v", e)
	case res := <-resCh:
		// A clean orphan-exit: no units served, no error.
		if res.UnitsServed != 0 {
			t.Fatalf("mini served %d units, want 0 (it should have orphan-exited)", res.UnitsServed)
		}
		t.Logf("mini orphan-exited on supervisor death: units=%d (clean exit)", res.UnitsServed)
	case <-time.After(10 * time.Second):
		t.Fatalf("mini did not orphan-exit within 10s of the supervisor dying (it should exit immediately, not by the 60s timeout)")
	}
}
