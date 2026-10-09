package agent

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// TestAcceptMinisReportAfterHelloBound is the regression test for the leaked
// hello deadline (BUG #8, live crash+resume on a 5GB table): AcceptMinis
// bound the mini-hello read with an ABSOLUTE 30s SetReadDeadline but never
// cleared it, so the deadline survived onto the long-lived report
// connection. The per-unit mini-unit-resp arrives only after the mini has
// received its FULL slice, which on a large table is well over 30s after the
// hello — the supervisor's next report read then failed with "i/o timeout"
// and every healthy mini was mis-reported as an orphan (unit_failed, no
// swap). The hello deadline must be cleared once the hello is in; a dead
// mini still surfaces as a connection drop.
func TestAcceptMinisReportAfterHelloBound(t *testing.T) {
	sup, err := NewSupervisor("127.0.0.1:0", nil, "", "", "", "")
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	defer sup.Close()
	const helloBound = 500 * time.Millisecond
	sup.WithHelloTimeout(helloBound)

	// Fake mini: hello, then "receive its slice" (silence), then its
	// per-unit report — all on the SAME persistent report connection.
	go func() {
		c, derr := net.Dial("tcp", sup.Addr())
		if derr != nil {
			return
		}
		defer c.Close()
		if werr := SendLine(c, MiniHello{Op: "mini-hello", Seg: 0, DataPort: 4242, Ready: true, Version: "test"}); werr != nil {
			return
		}
		time.Sleep(helloBound + 400*time.Millisecond)
		_ = SendLine(c, MiniUnitResp{Op: "mini-unit-resp", ReqID: 1, UnitID: 1, Ok: true, Rows: 7})
		time.Sleep(2 * time.Second)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sup.AcceptMinis(ctx, 1); err != nil {
		t.Fatalf("AcceptMinis: %v", err)
	}

	// Wait past the hello bound so the (buggy) leaked deadline would have
	// expired, then read the report EXACTLY as production does: on the
	// persistent connection, with NO fresh deadline (ServeUnit relies on the
	// connection's deadline state). Success = the hello deadline was cleared;
	// an "i/o timeout" error = the leak. A watchdog guards against a hang.
	time.Sleep(helloBound + 600*time.Millisecond)
	conn, ok := sup.ReportConn(0)
	if !ok {
		t.Fatalf("ReportConn(0): no report connection")
	}
	type readOut struct {
		resp MiniUnitResp
		err  error
	}
	res := make(chan readOut, 1)
	go func() {
		var r MiniUnitResp
		e := recvLine(bufio.NewReader(conn), &r)
		res <- readOut{r, e}
	}()
	select {
	case o := <-res:
		if o.err != nil {
			t.Fatalf("report read %s after the hello: %v (hello deadline leaked onto the long-lived report connection)", helloBound, o.err)
		}
		if !o.resp.Ok || o.resp.Rows != 7 {
			t.Fatalf("resp = %+v, want Ok rows=7", o.resp)
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("report read hung (no report arrived and no deadline fired)")
	}
}

// TestAcceptMinisHelloTimeoutBoundsStuckMini: a mini that connects but never
// sends its hello must NOT wedge AcceptMinis — the hello bound fails it.
func TestAcceptMinisHelloTimeoutBoundsStuckMini(t *testing.T) {
	sup, err := NewSupervisor("127.0.0.1:0", nil, "", "", "", "")
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	defer sup.Close()
	sup.WithHelloTimeout(300 * time.Millisecond)

	c, err := net.Dial("tcp", sup.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	// Stuck mini: no hello.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := sup.AcceptMinis(ctx, 1); err == nil {
		t.Fatalf("AcceptMinis: expected a hello-timeout error, got nil")
	}
	if dt := time.Since(start); dt > 4*time.Second {
		t.Fatalf("hello timeout took %s (want ~300ms)", dt)
	}
}
