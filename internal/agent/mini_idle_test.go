package agent

// mini_idle_test.go — L2 data-plane watchdog regressions (DESIGN:
// "таймаут передачи данных", default 5m, flag --data-idle-timeout).
//
// The watchdog is idle-based: a unit's data (sub-)stream that receives NO
// byte for DataIdleTimeout is declared dead (the mini kills the stream, the
// blocked read unblocks, the unit FAILS). A healthy large table streams
// continuously and never trips it; a wedged segreceiver (or a GUC flip
// mid-copy) goes silent and fails after the timeout.
//
// The "silent segreceiver" is reproduced by a trailer-only stream that keeps
// the connection OPEN: the reader consumes the trailer, then blocks in
// conn.Read waiting for EOF (the exact INC8 hang state), and the watchdog
// must close the stream and fail the unit.

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// deadPrimaryDSN points at a closed port: the utility-COPY goroutine fails
// to connect immediately (the tests only exercise the DATA path; the COPY
// result is irrelevant to the watchdog assertion).
const deadPrimaryDSN = "host=127.0.0.1 port=1 dbname=gpstream user=gpadmin sslmode=disable connect_timeout=1"

func freeListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return l
}

// pgtestStagingDSN boots a real ephemeral PG (pgtest) with the unit's staging
// table created, and returns a utility-style DSN to it. The routed serve's
// COPY goroutine must reach c.WriteCopy's pipe read (the merged-header write
// unblocks only once the COPY is reading the pipe); a dead DSN would make the
// COPY fail early and close the pipe before the accept/idle logic runs — a
// test-ordering artifact, not the watchdog behavior under test.
func pgtestStagingDSN(t *testing.T, port uint32, db string) string {
	t.Helper()
	cl, err := pgtest.Start(port, db)
	if err != nil {
		t.Fatalf("pgtest start: %v", err)
	}
	t.Cleanup(cl.Stop)
	// Create the staging table the utility COPY loads (the supervisor does
	// this in production; the test creates it on the local PG).
	c, err := pg.NewConn(context.Background(), cl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("pg conn: %v", err)
	}
	defer c.Close()
	if _, err := c.Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS public._gs_t (k int, v text)`); err != nil {
		t.Fatalf("create staging: %v", err)
	}
	return cl.URL()
}

func miniTestOpts(l net.Listener, idle time.Duration) MiniOptions {
	return MiniOptions{
		Seg:             0,
		PrimaryDSN:      deadPrimaryDSN,
		AcceptTimeout:   5 * time.Second,
		StreamTimeout:   10 * time.Second,
		DataIdleTimeout: idle,
		FrameAck:        false, // raw frames: the idle pipe wraps the raw conn
		SharedListen:    l,
	}
}

func miniTestReq() MiniUnitReq {
	return MiniUnitReq{
		Op:             "mini-unit",
		ReqID:          1,
		UnitID:         7,
		Schema:         "public",
		Staging:        "_gs_t",
		Compress:       "none",
		SourceSegments: 1,
		Workers:        1,
	}
}

// sendTrailerOnly sends the slice trailer and keeps the connection open
// (silent): the reader consumes the trailer, then blocks in conn.Read
// waiting for EOF — the INC8 hang state.
func sendTrailerOnly(t *testing.T, l net.Listener, content int16) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := transfer.WriteFrame(conn, transfer.NewTrailerFrame(1, 7, 0, content, transfer.Trailer{Rows: 1})); err != nil {
		t.Fatalf("write trailer: %v", err)
	}
	return conn
}

// TestIdlePipeDataKeepsStreamAlive: data within the window refreshes the
// watchdog (a healthy stream is never killed); silence beyond the window
// closes the connection and the blocked read unblocks.
func TestIdlePipeDataKeepsStreamAlive(t *testing.T) {
	srvL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer srvL.Close()
	connCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := srvL.Accept()
		if err != nil {
			errCh <- err
			return
		}
		connCh <- c
	}()
	client, err := net.Dial("tcp", srvL.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	serverConn, ok := <-connCh
	if !ok {
		t.Fatalf("accept failed: %v", <-errCh)
	}

	idleTO := 1500 * time.Millisecond
	ip := newIdlePipe(serverConn, serverConn.Close, idleTO)

	// Read in a loop: keep consuming bytes until an error (the watchdog's
	// close) arrives.
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		for {
			if _, err := ip.Read(buf); err != nil {
				readDone <- err
				return
			}
		}
	}()

	// Feed bytes inside the (refreshing) window: the watchdog must NOT fire.
	time.Sleep(400 * time.Millisecond)
	if _, err := client.Write([]byte{'a'}); err != nil {
		t.Fatalf("write a: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	if _, err := client.Write([]byte{'b'}); err != nil {
		t.Fatalf("write b: %v", err)
	}
	// Still no read error: the data kept the stream alive.
	select {
	case err := <-readDone:
		t.Fatalf("read unblocked too early: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	// Now go silent: the watchdog must close the connection and the read
	// must unblock with a transport error.
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatalf("read returned data after the idle close")
		}
		t.Logf("read unblocked after idle: %v", err)
	case <-time.After(3 * idleTO):
		t.Fatalf("idle watchdog did not unblock the read within %v", 3*idleTO)
	}
}

// TestServeMiniUnitDataIdleTimeout: a segreceiver that goes silent (open,
// no bytes) must FAIL the unit after DataIdleTimeout — not wait out the
// 6h stream timeout.
func TestServeMiniUnitDataIdleTimeout(t *testing.T) {
	l := freeListener(t)
	defer l.Close()
	o := miniTestOpts(l, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	respCh := make(chan MiniUnitResp, 1)
	go func() {
		respCh <- serveMiniUnit(ctx, o, l, miniTestReq())
	}()

	// The segreceiver dials, sends the trailer, then goes silent (open).
	time.Sleep(300 * time.Millisecond)
	conn := sendTrailerOnly(t, l, 0)
	defer conn.Close()

	var resp MiniUnitResp
	select {
	case resp = <-respCh:
	case <-time.After(9 * time.Second):
		t.Fatalf("unit did not finish within 9s (watchdog should fail it after ~2s)")
	}
	if resp.Ok {
		t.Fatalf("expected the unit to fail on data idle, got ok (rows=%d)", resp.Rows)
	}
	if !strings.Contains(resp.Error, "slice") {
		t.Fatalf("expected a slice/stream failure, got: %q", resp.Error)
	}
	t.Logf("unit failed as expected: %s", resp.Error)
}

// TestServeMiniUnitAcceptGap: a segreceiver that NEVER dials must not hold
// the unit open for the whole stream timeout — the accept is bounded by
// AcceptTimeout (L2 accept-gap).
func TestServeMiniUnitAcceptGap(t *testing.T) {
	l := freeListener(t)
	defer l.Close()
	o := miniTestOpts(l, 2*time.Second)
	o.AcceptTimeout = 1 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	respCh := make(chan MiniUnitResp, 1)
	go func() {
		respCh <- serveMiniUnit(ctx, o, l, miniTestReq())
	}()

	var resp MiniUnitResp
	select {
	case resp = <-respCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("unit did not fail within 5s (accept-gap should fail it after ~1s)")
	}
	if resp.Ok {
		t.Fatalf("expected the unit to fail on the accept gap, got ok")
	}
	if !strings.Contains(resp.Error, "accept slice") {
		t.Fatalf("expected an accept failure, got: %q", resp.Error)
	}
	t.Logf("unit failed as expected: %s", resp.Error)
}

// TestServeMiniUnitRoutedAcceptGap: in a routed unit (S x W sub-streams) one
// source segment that never dials must fail the unit after the per-accept
// timeout (not the 6h stream timeout); the per-accept budget means the
// late sub-stream (1) times out, not the first one that arrived.
func TestServeMiniUnitRoutedAcceptGap(t *testing.T) {
	l := freeListener(t)
	defer l.Close()
	o := miniTestOpts(l, 2*time.Second)
	o.AcceptTimeout = 1 * time.Second
	o.PrimaryDSN = pgtestStagingDSN(t, 5703, "gp_idle_acc") // the COPY goroutine reaches the pipe read
	o.StreamTimeout = 3 * time.Second                       // bounds the stuck COPY once the unit fails

	req := miniTestReq()
	req.SourceSegments = 2
	req.Workers = 1

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	respCh := make(chan MiniUnitResp, 1)
	go func() {
		respCh <- serveMiniUnitRouted(ctx, o, l, req)
	}()

	// Dial the FIRST sub-stream (segment 0), stay silent (open): its
	// reader blocks in conn.Read; only the SECOND accept (segment 1)
	// times out.
	time.Sleep(300 * time.Millisecond)
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial sub-stream 0: %v", err)
	}
	defer conn.Close()

	var resp MiniUnitResp
	select {
	case resp = <-respCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("unit did not fail within 5s (accept-gap should fail it after ~1s)")
	}
	if resp.Ok {
		t.Fatalf("expected the unit to fail on the accept gap, got ok")
	}
	if !strings.Contains(resp.Error, "accept sub-stream 1") {
		t.Fatalf("expected the second sub-stream accept to time out, got: %q", resp.Error)
	}
	t.Logf("unit failed as expected: %s", resp.Error)
}

// TestServeMiniUnitRoutedDataIdlePerSubStream: in a routed unit the idle
// watchdog is PER sub-stream — the one silent sub-stream fails the unit
// after DataIdleTimeout even while the other sub-stream completes healthy.
func TestServeMiniUnitRoutedDataIdlePerSubStream(t *testing.T) {
	l := freeListener(t)
	defer l.Close()
	o := miniTestOpts(l, 2*time.Second)
	o.PrimaryDSN = pgtestStagingDSN(t, 5704, "gp_idle_idle") // the COPY goroutine reaches the pipe read
	o.StreamTimeout = 6 * time.Second                        // > the 2s idle timeout; bounds the stuck COPY

	req := miniTestReq()
	req.SourceSegments = 2
	req.Workers = 1

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	respCh := make(chan MiniUnitResp, 1)
	go func() {
		respCh <- serveMiniUnitRouted(ctx, o, l, req)
	}()

	time.Sleep(300 * time.Millisecond)
	// Sub-stream 0 (segment 0): trailer, then SILENT (open) — the dead
	// source segment. Its reader blocks in conn.Read after the trailer.
	c0 := sendTrailerOnly(t, l, 0)
	defer c0.Close()
	// Sub-stream 1 (segment 1): healthy — trailer, then a clean close
	// (the reader sees EOF with a trailer present -> completes). It must
	// NOT rescue the dead sub-stream.
	c1 := sendTrailerOnly(t, l, 1)
	c1.Close()

	var resp MiniUnitResp
	select {
	case resp = <-respCh:
	case <-time.After(11 * time.Second):
		t.Fatalf("unit did not fail within 11s (per-sub-stream idle should fail it after ~2s)")
	}
	if resp.Ok {
		t.Fatalf("expected the unit to fail on the silent sub-stream, got ok")
	}
	if !strings.Contains(resp.Error, "sub-stream") {
		t.Fatalf("expected a sub-stream failure, got: %q", resp.Error)
	}
	t.Logf("unit failed as expected: %s", resp.Error)
}

// TestMiniIdleConfigDefaults: the default is 5 minutes (user-specified) and
// a negative value is rejected.
func TestMiniIdleConfigDefaults(t *testing.T) {
	cfg := config.Default()
	if cfg.DataIdleTimeout != 5*time.Minute {
		t.Fatalf("default DataIdleTimeout = %v, want 5m", cfg.DataIdleTimeout)
	}
	cfg.DataIdleTimeout = -1 * time.Minute
	if err := cfg.Validate(); err == nil {
		t.Fatalf("negative DataIdleTimeout must be rejected")
	}
}
