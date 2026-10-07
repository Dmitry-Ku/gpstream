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

// TestTwoAgentMeshRouted is the P4 (ROUTED MESH) acceptance: S source
// "segments" (fake segreceivers running a REAL segstream.RoutedSender) each
// fan their rows to D destination "segments" over W sub-streams, and each
// destination segment's REAL mini-agent (agent.ServeMini) MERGES the S×W
// sub-streams into one binary-COPY stream loaded by a single utility-mode
// COPY into its local staging slice. The supervisor (agent.Supervisor)
// combines the D per-segment reports into the unit's digests:
//
//	srcDigest = XOR_D(srcContrib),  dstDigest = XOR_D(dstAcc)  over the summed
//	row count.
//
// This is the MESH analogue of the P9 (non-mesh) routed path (ServeRouted on
// the coordinator): here the merge runs on the segment host (the mini-agent),
// not the coordinator — design 04.10 (the agent that opens the pg connection
// runs where the connection is made). Neither cluster coordinator is in the
// data path: the source coordinator only dispatched the ON SEGMENT read, the
// destination coordinator only created the staging DDL.
//
// Per-segment model (plain PostgreSQL): each destination "segment" is a
// SEPARATE pgtest cluster whose primary holds the local staging slice; the
// mini's PrimaryDSN is its own cluster URL — the same table name in each (the
// per-segment local slice), exactly as GP utility-mode local slices are the
// same table on different primaries. Each source "segment" is a round-robin
// slice of the source table's binary-COPY stream.
//
// Invariants checked:
//  1. the supervisor's combined row count == the seeded full row count, with
//     the per-segment distribution covering every row once (disjoint and
//     complete across the S×D×W sub-streams — no duplication, no loss);
//  2. the combined srcDigest AND dstDigest == the buffered reference digest of
//     the whole table (XOR_D of the per-segment accumulators == the full
//     accumulator — the composable-digest invariant, FR-02);
//  3. the network carried the COMPRESSED volume (wire < logical);
//  4. each segment cluster's staging slice holds its slice (count == PerSeg);
//  5. a clean Stop makes every mini exit with its unit accounted.
func TestTwoAgentMeshRouted(t *testing.T) {
	const (
		srcPort = 5801
		rows    = 30000
		jobID   = uint32(0x42ab)
		unitID  = uint32(21)
		S       = 3 // three source "segments"
		D       = 3 // three destination "segments"
		W       = 2 // sub-streams per (source-seg, dest-seg) pair
	)
	dstPorts := []uint32{5802, 5803, 5804}
	if len(dstPorts) != D {
		t.Fatalf("internal: %d dst ports for D=%d", len(dstPorts), D)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "p4_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()

	dstCls := make([]*pgtest.Cluster, D)
	dstConns := make([]*pg.Conn, D)
	for i := 0; i < D; i++ {
		cl, err := pgtest.Start(dstPorts[i], fmt.Sprintf("p4_dst_%d", i))
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

	// Seed a compressible source table with an integer distribution key on
	// column 0 (oid 23 = int4) and read its binary-COPY stream.
	mustExec(t, srcConn, `DROP TABLE IF EXISTS p4_orders`)
	mustExec(t, srcConn, `CREATE TABLE p4_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO p4_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "p4_orders", rows)

	// Split the stream into S source slices (the kernel's per-segment shape):
	// a round-robin partition of the rows, each rebuilt as an independent
	// binary-COPY stream. The RoutedSender then places every row on its
	// destination segment (cdbhash/jump over the int4 key) — a disjoint
	// partition across the S×D×W sub-streams.
	slices := splitBinarySlices(t, full, S)

	// The staging slice in EACH destination cluster (one per mini). On plain
	// PG each is a local table; on GP the supervisor's coordinator DDL
	// replicates it to the segments. Same table name in each = the per-segment
	// local slice.
	const staging = "_gs_p4_21"
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "DROP TABLE IF EXISTS public."+staging)
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
				StreamTimeout: 120 * time.Second,
			})
			if err != nil {
				t.Logf("mini %d: %v", i, err)
				return
			}
			miniRes <- res
		}(i)
	}

	// Wait for the D hellos (AcceptMinis completes) and learn each mini's data
	// endpoint. The minis run on this host, so the data address host is
	// 127.0.0.1 (SetMiniHost).
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

	// Drive the ROUTED unit: the supervisor broadcasts mini-unit (S, W) to all
	// D minis and collects + combines their reports. Meanwhile the S fake
	// segreceivers (real RoutedSender) fan their slices to the D minis.
	supCh := make(chan agent.CombinedUnitResult, 1)
	supErr := make(chan error, 1)
	go func() {
		res, err := sup.ServeUnit(ctx, agent.UnitReportReq{
			ReqID:          1,
			UnitID:         unitID,
			JobID:          jobID,
			Schema:         "public",
			Staging:        staging,
			Compress:       "zstd",
			VerifyDigest:   true,
			SourceSegments: int32(S),
			Workers:        W,
		})
		if err != nil {
			supErr <- err
			return
		}
		supCh <- res
	}()

	// The S fake segreceivers: each runs a REAL RoutedSender over its slice
	// (route by the int4 key + fan each (s,d) pair over W sub-streams, each
	// sub-stream content-tagged with its DESTINATION segment id and carrying
	// the per-row digest in its trailer). Each sub-stream dials the matching
	// mini-agent's data endpoint (the mesh address book).
	errCh := make(chan error, S)
	for s := 0; s < S; s++ {
		go func(s int) {
			conns := make([][]transfer.Pipe, D)
			for d := 0; d < D; d++ {
				conns[d] = make([]transfer.Pipe, W)
				for w := 0; w < W; w++ {
					c, derr := dialRetry(sup.MiniDataAddr(d), 90*time.Second)
					if derr != nil {
						errCh <- fmt.Errorf("fake segreceiver %d dial (d%d w%d): %w", s, d, w, derr)
						return
					}
					conns[d][w] = c // net.Conn satisfies transfer.Pipe
				}
			}
			sender := segstream.NewRoutedSender(ctx, bytes.NewReader(slices[s]), []int{0}, []uint32{23}, int32(D), W,
				uint32(jobID), uint32(unitID), int16(s), compress.ModeZstd, 3, true, conns)
			if _, err := sender.Run(); err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d run: %v", s, err)
				return
			}
			errCh <- nil
		}(s)
	}
	for i := 0; i < S; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("fake segreceiver: %v", e)
		}
	}

	var res agent.CombinedUnitResult
	select {
	case e := <-supErr:
		t.Fatalf("supervisor ServeUnit: %v", e)
	case res = <-supCh:
	case <-time.After(120 * time.Second):
		t.Fatalf("supervisor did not finish the unit in time")
	}
	t.Logf("combined: rows=%d srcDigest=%q dstDigest=%q wire=%d logical=%d perSeg=%v",
		res.Rows, res.SrcDigest, res.DstDigest, res.Wire, res.Logical, res.PerSeg)

	// 1. The combined row count == the seeded full row count (disjoint and
	// complete across the S×D×W sub-streams: no duplication, no loss).
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
	// ADR-002: the network carried the COMPRESSED volume.
	if res.Wire >= uint64(len(full)) {
		t.Fatalf("on-wire %d >= logical %d: not compressed on the network", res.Wire, len(full))
	}

	// 2. The XOR_D combine: both combined digests == the buffered reference of
	// the whole table (the composable-digest invariant, FR-02).
	if res.SrcDigest != refDigest {
		t.Fatalf("combined srcDigest %q != reference %q", res.SrcDigest, refDigest)
	}
	if res.DstDigest != refDigest {
		t.Fatalf("combined dstDigest %q != reference %q", res.DstDigest, refDigest)
	}

	// 3. Each segment cluster's staging slice holds its slice; together they
	// cover every row once.
	var total int64
	for i := 0; i < D; i++ {
		got := mustCount(t, dstConns[i], "SELECT count(*) FROM public."+staging)
		if got != res.PerSeg[i] {
			t.Fatalf("seg %d staging count = %d, want %d", i, got, res.PerSeg[i])
		}
		total += got
	}
	if total != rows {
		t.Fatalf("sum of D staging slices = %d, want %d", total, rows)
	}

	// 4. Clean Stop -> every mini exits with its unit accounted.
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

	t.Logf("mesh routed (P4): rows=%d S=%d D=%d W=%d digest=%q (XOR_D == reference)",
		res.Rows, S, D, W, refDigest)
}
