//go:build gp6lab

package integration

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// TestSupervisorDirect1to1Mesh is the P3 (supervisor in the dest-agent)
// acceptance for the DIRECT_1TO1 MESH: a real agent.Supervisor on the
// destination coordinator coordinates D real agent.ServeMini mini-agents (one
// per destination segment), each of which loads ONE compressed slice into ONE
// utility-mode COPY into its own staging slice, and the supervisor COMBINES
// the D per-segment reports into the unit's digests:
//
//	srcDigest = XOR_D(srcContrib),  dstDigest = XOR_D(dstAcc)  over the summed
//	row count.
//
// The per-row XOR accumulator is commutative/associative, so the order of the
// D segments does not matter — this is what makes the direct_1to1 mesh
// verifiable WITHOUT re-reading any table (FR-02). The test plays the SOURCE
// (fake segreceivers that stream the D slices to the mini data endpoints) and
// the SUPERVISOR (real: AcceptMinis -> MiniMap, ServeUnit -> XOR_D combine,
// Stop).
//
// Per-segment model: a real destination segment is a SEPARATE cluster whose
// primary holds its local staging slice; the mini's utility COPY writes a
// LOCAL table. On plain PostgreSQL we emulate D separate segment hosts with
// D separate destination pgtest clusters (each mini's PrimaryDSN is its own
// cluster URL) — the same table name in each (the per-segment local slice),
// exactly as GP utility-mode local slices are the same table on different
// primaries. The "separate segment host" aspect of the production layout (SSH
// launch) is the P3 SSH-launch increment, not the supervisor core; here the
// per-segment routing + the XOR_D combine are what the test exercises.
//
// Invariants checked:
//  1. the supervisor's combined row count == the seeded full row count (sum of
//     the D slices), with the per-segment distribution covering every row once;
//  2. the combined srcDigest AND dstDigest == the buffered reference digest of
//     the whole table (XOR_D of the per-segment accumulators == the full
//     accumulator — the composable-digest invariant);
//  3. a clean Stop makes every mini exit with its unit accounted.
func TestSupervisorDirect1to1Mesh(t *testing.T) {
	const (
		srcPort = 5635
		rows    = 20000
		jobID   = uint32(0x89ab)
		unitID  = uint32(11)
		D       = 3 // three destination "segments" (three separate clusters)
	)
	// D separate destination clusters emulate D separate segment hosts.
	dstPorts := []uint32{5638, 5643, 5644}
	if len(dstPorts) != D {
		t.Fatalf("internal: %d dst ports for D=%d", len(dstPorts), D)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "p3_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()

	dstCls := make([]*pgtest.Cluster, D)
	dstConns := make([]*pg.Conn, D)
	for i := 0; i < D; i++ {
		cl, err := pgtest.Start(dstPorts[i], fmt.Sprintf("p3_dst_%d", i))
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

	srcConn, err := pg.NewConn(ctx, srcCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcConn.Close()

	// Seed a compressible source table and read its binary-COPY stream.
	mustExec(t, srcConn, `DROP TABLE IF EXISTS p3_orders`)
	mustExec(t, srcConn, `CREATE TABLE p3_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO p3_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "p3_orders", rows)

	// Split the stream into D slices (the kernel's per-segment shape): a
	// round-robin partition of the rows, each rebuilt as an independent
	// binary-COPY stream. The XOR of the D slice accumulators == the full
	// table accumulator (XOR is commutative/associative).
	slices := splitBinarySlices(t, full, D)

	// The supervisor (coordinator) creates the staging slice in EACH segment
	// cluster (one per mini). On plain PG each is a local table in that
	// cluster; on GP the supervisor's coordinator DDL replicates it to the
	// segments. Same table name in each = the per-segment local slice.
	const staging = "_gs_p3_11"
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "DROP TABLE IF EXISTS public."+staging+"")
		mustExec(t, dstConns[i], "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")
	}

	// Reference digest of the whole table (buffered, order-independent).
	refAcc, refRows, err := transfer.DigestAccumulator(full)
	if err != nil {
		t.Fatalf("reference DigestAccumulator: %v", err)
	}
	if refRows != rows {
		t.Fatalf("reference rows %d != seeded %d", refRows, rows)
	}
	refDigest := transfer.CombinedDigestString(refAcc, rows)

	// The supervisor: bind its report listener, then accept the D minis.
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

	// Start the D minis (real ServeMini). Each dials the supervisor's report
	// listener, sends its hello, and waits for per-unit instructions. Each
	// mini's PrimaryDSN is its OWN segment cluster (its local slice).
	miniRes := make(chan agent.MiniResult, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			res, err := agent.ServeMini(ctx, agent.MiniOptions{
				Seg:           i,
				DataListen:    "127.0.0.1:0",
				PrimaryDSN:    dstCls[i].URL(),
				ReportAddr:    reportAddr,
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

	// Wait for the D hellos (AcceptMinis completes) and learn each mini's data
	// endpoint. The hello carries the port; the minis run on this host, so the
	// data address host is 127.0.0.1 (SetMiniHost).
	var mm agent.MiniMap
	select {
	case e := <-mmErr:
		t.Fatalf("supervisor accept minis: %v", e)
	case mm = <-mmCh:
	case <-time.After(30 * time.Second):
		t.Fatalf("supervisor did not accept %d minis in time", D)
	}
	if len(mm) != D {
		t.Fatalf("MiniMap has %d entries, want %d", len(mm), D)
	}
	for i := 0; i < D; i++ {
		sup.SetMiniHost(i, "127.0.0.1")
		if addr := sup.MiniDataAddr(i); addr == "" {
			t.Fatalf("mini %d has no data address (MiniMap=%v)", i, mm)
		}
	}
	t.Logf("MiniMap: %v", mm)

	// Drive the unit: the supervisor broadcasts mini-unit to all D minis and
	// collects + combines their reports (concurrent). Meanwhile the fake
	// segreceivers stream each slice to its mini's data endpoint.
	supCh := make(chan agent.CombinedUnitResult, 1)
	supErr := make(chan error, 1)
	go func() {
		res, err := sup.ServeUnit(ctx, agent.UnitReportReq{
			ReqID:        1,
			UnitID:       unitID,
			JobID:        jobID,
			Schema:       "public",
			Staging:      staging,
			Compress:     "zstd",
			VerifyDigest: true,
		})
		if err != nil {
			supErr <- err
			return
		}
		supCh <- res
	}()

	// The D fake segreceivers: each streams its slice COMPRESSED to its mini's
	// data endpoint, content-tagged with its segment id.
	errCh := make(chan error, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			addr := sup.MiniDataAddr(i)
			conn, derr := dialRetry(addr, 20*time.Second)
			if derr != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial %s: %w", i, addr, derr)
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

	var res agent.CombinedUnitResult
	select {
	case e := <-supErr:
		t.Fatalf("supervisor ServeUnit: %v", e)
	case res = <-supCh:
	case <-time.After(60 * time.Second):
		t.Fatalf("supervisor did not finish the unit in time")
	}
	t.Logf("combined: rows=%d srcDigest=%q dstDigest=%q wire=%d logical=%d perSeg=%v",
		res.Rows, res.SrcDigest, res.DstDigest, res.Wire, res.Logical, res.PerSeg)

	// 1. The combined row count == the seeded full row count (sum of D slices).
	if res.Rows != rows {
		t.Fatalf("combined rows %d != seeded %d (perSeg=%v)", res.Rows, rows, res.PerSeg)
	}
	if len(res.PerSeg) != D {
		t.Fatalf("perSeg has %d entries, want %d", len(res.PerSeg), D)
	}
	var perSegSum int64
	for _, c := range res.PerSeg {
		perSegSum += c
	}
	if perSegSum != rows {
		t.Fatalf("perSeg sum %d != rows %d (perSeg=%v)", perSegSum, rows, res.PerSeg)
	}
	// ADR-002: the network carried the COMPRESSED volume (sum over D slices).
	if res.Wire >= uint64(len(full)) {
		t.Fatalf("on-wire %d >= logical %d: not compressed on the network", res.Wire, len(full))
	}

	// 2. The XOR_D combine: both combined digests == the buffered reference of
	// the whole table (the composable-digest invariant).
	if res.SrcDigest != refDigest {
		t.Fatalf("combined srcDigest %q != reference %q", res.SrcDigest, refDigest)
	}
	if res.DstDigest != refDigest {
		t.Fatalf("combined dstDigest %q != reference %q", res.DstDigest, refDigest)
	}

	// 3. Clean Stop -> every mini exits with its unit accounted.
	sup.Stop()
	for i := 0; i < D; i++ {
		select {
		case mr := <-miniRes:
			if mr.UnitsServed != 1 {
				t.Fatalf("mini seg %d served %d units, want 1", i, mr.UnitsServed)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("mini seg %d did not exit after Stop", i)
		}
	}

	// Each segment cluster's staging slice holds its slice; together they
	// cover every row once.
	var total int64
	for i := 0; i < D; i++ {
		got := mustCount(t, dstConns[i], "SELECT count(*) FROM public."+staging+"")
		if got != res.PerSeg[i] {
			t.Fatalf("seg %d staging count = %d, want %d", i, got, res.PerSeg[i])
		}
		total += got
	}
	if total != rows {
		t.Fatalf("sum of D staging slices = %d, want %d", total, rows)
	}
	t.Logf("supervisor direct_1to1 mesh: rows=%d D=%d digest=%q (XOR_D == reference)",
		res.Rows, D, refDigest)
}
