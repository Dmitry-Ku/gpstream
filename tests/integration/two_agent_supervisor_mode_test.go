//go:build gp6lab

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// TestDestServerSupervisorMode is the P3 production-dispatch acceptance for
// the SUPERVISOR MODE of the long-lived dest-agent (the --supervisor flag):
// ONE process on the destination coordinator that (a) coordinates the D
// destination-segment mini-agents (kind="mini" serves are broadcast to the
// minis, each does a local utility COPY on its segment, the reports are
// combined with XOR_D) and (b) keeps the fallback coordinator path (the
// persistent-coordinator listener + the workers+2 pg pool — exercised by the
// P1c/P1d tests).
//
// The test drives the REAL agent.DestServer in supervisor mode over the REAL
// control protocol (DialDestControl: hello -> begin-serve -> unit-done) with
// D real agent.ServeMini mini-agents (each on its own pgtest cluster, the
// per-segment local-slice model). Invariants:
//
//  1. the hello carries the supervisor's REPORT address + MiniCount (the
//     controller launches the minis with --report-addr from it);
//  2. MiniMapReady flips true once the D hellos arrive, and MiniDataAddrs
//     returns the D-entry address book (comma-joined, seg order);
//  3. kind="mini" serve: the combined row count == the seeded rows, the
//     combined dstDigest == the buffered reference (XOR_D == full, FR-02);
//  4. kind="segments" is REJECTED in supervisor mode (the mini path replaces
//     the coordinator-side mesh serves);
//  5. release -> the minis exit cleanly (mini-done).
func TestDestServerSupervisorMode(t *testing.T) {
	const (
		srcPort = 5650
		rows    = 12000
		jobID   = uint32(0x77cd)
		unitID  = uint32(42)
		D       = 3
	)
	dstPorts := []uint32{5652, 5653, 5654}
	if len(dstPorts) != D {
		t.Fatalf("internal: %d dst ports for D=%d", len(dstPorts), D)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "supm_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	srcConn, err := pg.NewConn(ctx, srcCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcConn.Close()

	mustExec(t, srcConn, `DROP TABLE IF EXISTS supm_orders`)
	mustExec(t, srcConn, `CREATE TABLE supm_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO supm_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "supm_orders", rows)
	slices := splitBinarySlices(t, full, D)

	dstCls := make([]*pgtest.Cluster, D)
	dstConns := make([]*pg.Conn, D)
	for i := 0; i < D; i++ {
		cl, err := pgtest.Start(dstPorts[i], fmt.Sprintf("supm_dst_%d", i))
		if err != nil {
			for j := i - 1; j >= 0; j-- {
				dstCls[j].Stop()
			}
			t.Fatalf("start dest seg %d: %v", i, err)
		}
		dstCls[i] = cl
		defer dstCls[i].Stop()
		c, err := pg.NewConn(ctx, cl.URL(), "", "", "", "")
		if err != nil {
			t.Fatalf("connect dest seg %d: %v", i, err)
		}
		dstConns[i] = c
		defer c.Close()
	}

	const staging = "_gs_supm_42"
	// The per-segment staging slices: on a real Greenplum destination the
	// supervisor's coordinator DDL (ensureMiniStaging) replicates the
	// distributed staging table to the segments; on this plain-PG stand
	// (one cluster per "segment", no replication) the test creates each
	// local slice directly — the same shape the replication produces.
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "DROP TABLE IF EXISTS public."+staging)
		mustExec(t, dstConns[i], "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")
	}

	refAcc, refRows, err := transfer.DigestAccumulator(full)
	if err != nil {
		t.Fatalf("reference DigestAccumulator: %v", err)
	}
	if refRows != rows {
		t.Fatalf("reference rows %d != seeded %d", refRows, rows)
	}
	refDigest := transfer.CombinedDigestString(refAcc, rows)

	// 1. The REAL dest server in SUPERVISOR MODE: control + persist listeners
	// (the fallback coordinator path) + the supervisor's report listener; the
	// shared data listener is OFF (the mini path uses the minis' listeners).
	ctrlAddr, releaseCtrl := freePort2(t)
	persistAddr, releasePersist := freePort2(t)
	defer releaseCtrl()
	defer releasePersist()
	srv, err := agent.NewDestServer(agent.DestServerOptions{
		DataListen:       "", // supervisor mode: the shared data listener is off
		ControlListen:    ctrlAddr,
		PersistListen:    persistAddr,
		DestConn:         dstCls[0].URL(), // the fallback coordinator pool
		Workers:          2,               // the run's data-worker count
		AcceptTimeout:    30 * time.Second,
		StreamTimeout:    60 * time.Second,
		SupervisorListen: "127.0.0.1:0", // report listener (auto-pick)
		MiniCount:        D,
		// MiniHosts empty = 127.0.0.1 (single-host test layout).
	})
	if err != nil {
		t.Fatalf("new supervisor dest server: %v", err)
	}
	defer srv.Close()

	// The controller dials the control socket and reads the hello.
	client, err := agent.DialDestControl(ctx, ctrlAddr)
	if err != nil {
		t.Fatalf("dial supervisor control: %v", err)
	}
	defer client.Close()
	if client.Hello.Report == "" {
		t.Fatalf("hello has no report address: %+v", client.Hello)
	}
	if client.Hello.MiniCount != D {
		t.Fatalf("hello mini_count = %d, want %d", client.Hello.MiniCount, D)
	}
	t.Logf("supervisor hello: report=%s minis=%d persist=%s", client.Hello.Report, client.Hello.MiniCount, client.Hello.Persist)

	// 2. Start the D real mini-agents (each dials the report listener).
	miniRes := make(chan agent.MiniResult, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			res, err := agent.ServeMini(ctx, agent.MiniOptions{
				Seg:           i,
				DataListen:    "127.0.0.1:0",
				PrimaryDSN:    dstCls[i].URL(),
				ReportAddr:    client.Hello.Report,
				AcceptTimeout: 30 * time.Second,
				StreamTimeout: 60 * time.Second,
			})
			if err != nil {
				t.Logf("mini %d: %v", i, err)
				return
			}
			miniRes <- res
		}(i)
	}

	// Wait for the MiniMap to become ready (all D hellos collected).
	deadline := time.Now().Add(30 * time.Second)
	for {
		ready, aerr := srv.MiniMapReady()
		if ready {
			break
		}
		if aerr != nil {
			t.Fatalf("mini accept failed: %v", aerr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("MiniMap did not become ready in 30s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	addrs := srv.MiniDataAddrs()
	if addrs == "" {
		t.Fatalf("MiniDataAddrs empty after MiniMapReady")
	}
	addrList := strings.Split(addrs, ",")
	if len(addrList) != D {
		t.Fatalf("MiniDataAddrs has %d entries, want %d (%q)", len(addrList), D, addrs)
	}
	t.Logf("address book (D-entry): %s", addrs)

	// The address book is ALSO fetchable over the real control protocol
	// (the mini-map op) — this is what the controller uses to fold it into
	// the segreceivers' ON SEGMENT ProgramCommand.
	mm, err := client.MiniMap()
	if err != nil {
		t.Fatalf("mini-map op: %v", err)
	}
	if !mm.Ok {
		t.Fatalf("mini-map op not ok: %s", mm.Error)
	}
	if mm.Addrs != addrs {
		t.Fatalf("mini-map op addrs %q != MiniDataAddrs %q", mm.Addrs, addrs)
	}
	t.Logf("mini-map op: %d entries (%s)", len(strings.Split(mm.Addrs, ",")), mm.Addrs)

	// 3. kind="mini" serve over the REAL control protocol: BeginServe ->
	// (fake segreceivers stream the slices) -> WaitUnitDone.
	ack, err := client.BeginServe(agent.DestServeReq{
		Kind:   "mini",
		UnitID: unitID,
		Mini: &agent.MiniLaunch{
			Schema:  "public",
			Staging: staging,
			Cols: []pg.Column{
				{Name: "id", Type: "integer", Oid: 23, Nullable: false},
				{Name: "customer", Type: "text", Oid: 25},
				{Name: "amount", Type: "numeric", Oid: 1700},
			},
			Compress:     "zstd",
			VerifyDigest: true,
			JobID:        jobID,
		},
	})
	if err != nil {
		t.Fatalf("begin serve (mini): %v", err)
	}
	if !ack.Ok {
		t.Fatalf("begin serve (mini) failed: %s", ack.Error)
	}

	errCh := make(chan error, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			conn, derr := dialRetry(addrList[i], 20*time.Second)
			if derr != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial %s: %w", i, addrList[i], derr)
				return
			}
			if _, serr := segstream.SendCompressed(ctx, conn, bytes.NewReader(slices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3, true); serr != nil {
				errCh <- fmt.Errorf("fake segreceiver %d send: %w", i, serr)
				return
			}
			_ = conn.Close()
			errCh <- nil
		}(i)
	}
	for i := 0; i < D; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("fake segreceiver: %v", e)
		}
	}

	done, err := client.WaitUnitDone(ack.ReqID)
	if err != nil {
		t.Fatalf("wait unit-done (mini): %v", err)
	}
	if !done.Ok {
		t.Fatalf("unit-done (mini) failed: %s", done.Error)
	}
	if done.Rows != rows {
		t.Fatalf("mini combined rows %d != seeded %d", done.Rows, rows)
	}
	// ADR-002: the network carried the COMPRESSED volume.
	if done.Wire >= uint64(len(full)) {
		t.Fatalf("on-wire %d >= logical %d: not compressed on the network", done.Wire, len(full))
	}
	// FR-02: the combined dstDigest (XOR_D) == the buffered reference.
	if done.Digest != refDigest {
		t.Fatalf("mini combined dstDigest %q != reference %q", done.Digest, refDigest)
	}
	t.Logf("mini unit: rows=%d wire=%d logical=%d digest=%q (XOR_D == reference)", done.Rows, done.Wire, done.Logical, done.Digest)

	// 4. The old coordinator-side mesh kinds are REJECTED in supervisor mode
	// (the mini path replaces them).
	ack2, err := client.BeginServe(agent.DestServeReq{Kind: "segments", UnitID: unitID + 1, Spec: &agent.UnitSpec{Schema: "public", Table: "x", Staging: "y"}})
	if err != nil {
		t.Fatalf("begin serve (segments): %v", err)
	}
	done2, err := client.WaitUnitDone(ack2.ReqID)
	if err != nil {
		t.Fatalf("wait unit-done (segments): %v", err)
	}
	if done2.Ok {
		t.Fatalf("kind=segments succeeded in supervisor mode, want rejection")
	}
	if !strings.Contains(done2.Error, "not available in supervisor mode") {
		t.Fatalf("kind=segments rejection error = %q, want supervisor-mode notice", done2.Error)
	}

	// 5. Release -> the minis exit cleanly (mini-done).
	if _, err := client.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	for i := 0; i < D; i++ {
		select {
		case mr := <-miniRes:
			if mr.UnitsServed != 1 {
				t.Fatalf("mini seg %d served %d units, want 1", i, mr.UnitsServed)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("mini seg %d did not exit after release", i)
		}
	}

	// Each segment's staging slice holds its slice; together they cover every
	// row once.
	var total int64
	for i := 0; i < D; i++ {
		total += mustCount(t, dstConns[i], "SELECT count(*) FROM public."+staging)
	}
	if total != rows {
		t.Fatalf("sum of D staging slices = %d, want %d", total, rows)
	}
}

// freePort2 learns a free loopback port (bind, learn, close) so the
// supervisor's listeners can re-bind it (the prebindLoopback pattern: the
// tiny race window on loopback is acceptable). The release func is a no-op
// (the port is already freed; the server owns it now).
func freePort2(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close() // free it so the server's bindListener re-binds
	return addr, func() {}
}
