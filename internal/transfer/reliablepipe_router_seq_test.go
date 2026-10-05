package transfer

import (
	"net"
	"testing"
	"time"
)

// TestReliablePipeRouterSequentialUnitsOnSharedListener is the long-lived
// dest-agent lifecycle: serveOne runs units SEQUENTIALLY on ONE shared data
// listener (one data port per agent). Each unit's serve builds its own
// ResumeRouter + Run() over that listener, and Stops it on unit end WITHOUT
// closing the listener (the dest server owns it). The bug this guards: a
// per-unit Run() that stays blocked in Accept after its unit ends would
// STEAL the next unit's segreceiver connections and close them (its slots are
// cleared), so unit 2's serve would never see its slices.
//
// After unit 1's Stop, the next unit's router must be the one to accept +
// route. We assert: a sender dialing AFTER r1.Stop() is routed to r2's slot
// (not dropped by r1's stale loop).
func TestReliablePipeRouterSequentialUnitsOnSharedListener(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	// Unit 1: a router over the shared listener, then Stop (unit 1 done).
	r1 := NewResumeRouter(l)
	go r1.Run()
	slot1 := r1.NewPoolSlot()
	_ = slot1
	r1.Stop()

	// Unit 1's Run() loop observes the Stop within one accept poll (~1s)
	// and exits its Accept, releasing the shared listener for unit 2.
	time.Sleep(2 * time.Second)

	// Unit 2: a NEW router over the SAME shared listener.
	r2 := NewResumeRouter(l)
	go r2.Run()
	defer r2.Close()
	slot2 := r2.NewPoolSlot()
	if slot2 == nil {
		t.Fatal("unit 2: pool slot is nil (router closed?)")
	}

	// A segreceiver for unit 2 dials the shared listener.
	cfg := DefaultReliablePipeConfig()
	cfg.ResumeTimeout = 5 * time.Second
	id := StreamID{JobID: 2, UnitID: 2, Content: 0}
	c, err := net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	snd, err := NewReliablePipeSenderOnConnID(cfg, c, func() (dupConn, error) {
		return net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
	}, id)
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	defer snd.Close()

	// Unit 2's slot must receive the connection (the HELLO-replayed pipe
	// transport). If r1's stale loop were still alive it would have accepted
	// + closed this conn (no registered slot), and slot2 would time out.
	select {
	case conn := <-slot2:
		if conn == nil {
			t.Fatal("unit 2: slot delivered a nil conn")
		}
		// Consume + close the handed-off conn (no receiver pipe in this
		// lifecycle test; we only assert correct routing).
		_ = conn.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("unit 2: no conn routed to its slot within 5s (unit 1's stale router likely stole it)")
	}
}
