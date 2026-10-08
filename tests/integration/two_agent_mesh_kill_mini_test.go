//go:build gp6lab

package integration

import (
	"bytes"
	"context"
	"fmt"
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

// TestTwoAgentMeshKillMini is the P5 mesh crash-matrix scenario "kill-mini":
// a destination-segment mini-agent DIES MID-UNIT (its process is killed). The
// supervisor detects the death through the mini's persistent REPORT channel
// dropping (an orphan), must FAIL THE UNIT (so the run resumes and re-copies
// the unit, FR-07) instead of hanging or silently dropping the segment's
// contribution. The dead segment's staging slice ends EMPTY (the mini died
// before loading its slice — no partial rows land), the surviving segment's
// slice is loaded in full, and a re-copy after the staging slices are
// truncated (a run-level resume: a fresh supervisor + re-launched mini-agents)
// restores the unit with the combined digest == the reference.
//
// This is the mesh analogue of the kill-segreceiver scenario (same D=2
// "multi-segment" simulation model: two separate pgtest clusters = two segment
// hosts, a real agent.Supervisor, real agent.ServeMini mini-agents) and the
// coordinator-side twin of the already-done kill-supervisor scenario (the
// P2/P3 orphan-exit guarantee on the OTHER side of the report channel). The
// guarantee under test already exists in the code (supervisor.ServeUnit treats
// a dropped report channel as an orphan -> unit failure) — this test proves it
// end-to-end through the real mesh.
//
// The kill is modeled by severing the dead mini's REPORT connection from the
// test side (Supervisor.ReportConn, a test hook): that is exactly the signal
// the supervisor uses to detect a dead mini (the report channel dropping),
// which is what "the mini-agent process died" means to the coordinator. The
// dead mini is not streamed its slice (it died before loading), so its staging
// slice is untouched (empty) — no partial rows.
//
// Invariants checked:
//  1. PHASE 1 (kill): the supervisor's ServeUnit returns a UNIT FAILURE naming
//     the orphan; the killed segment's staging slice is EMPTY (no partial
//     rows); the surviving segment's slice is loaded in full (its mini
//     reported Ok before the supervisor gave up).
//  2. PHASE 2 (resume = FR-07 re-copy): after TRUNCATE of the staging slices,
//     a FRESH mesh (new supervisor + re-launched minis, as a run-level resume
//     does) re-copies the same unit successfully: combined rows == the seeded
//     count, BOTH combined srcDigest and dstDigest == the buffered reference
//     (XOR_D == the full accumulator, FR-02), and each staging slice holds
//     exactly its PerSeg count.
func TestTwoAgentMeshKillMini(t *testing.T) {
	const (
		srcPort = 5695
		rows    = 20000
		jobID   = uint32(0x51a3)
		unitID  = uint32(7)
		D       = 2 // two destination "segments" (two separate clusters)
		staging = "_gs_p5km_7"
	)
	dstPorts := []uint32{5696, 5697}
	if len(dstPorts) != D {
		t.Fatalf("internal: %d dst ports for D=%d", len(dstPorts), D)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Source cluster + D destination "segment" clusters (each a separate
	// pgtest cluster = one segment host holding its local staging slice).
	srcCl, err := pgtest.Start(srcPort, "p5km_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()

	dstCls := make([]*pgtest.Cluster, D)
	dstConns := make([]*pg.Conn, D)
	for i := 0; i < D; i++ {
		cl, err := pgtest.Start(dstPorts[i], fmt.Sprintf("p5km_dst_%d", i))
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
	mustExec(t, srcConn, `DROP TABLE IF EXISTS p5km_orders`)
	mustExec(t, srcConn, `CREATE TABLE p5km_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO p5km_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "p5km_orders", rows)

	// Split the stream into D slices (round-robin rows, each a complete
	// binary-COPY stream). The XOR of the D slice accumulators == the full
	// table accumulator (XOR is commutative/associative).
	slices := splitBinarySlices(t, full, D)

	// Reference digest of the whole table (buffered, order-independent).
	refAcc, refRows, err := transfer.DigestAccumulator(full)
	if err != nil {
		t.Fatalf("reference DigestAccumulator: %v", err)
	}
	if refRows != rows {
		t.Fatalf("reference rows %d != seeded %d", refRows, rows)
	}
	refDigest := transfer.CombinedDigestString(refAcc, rows)

	// Staging slice in EACH segment cluster (the supervisor creates/truncates
	// it; the minis only COPY into the existing slice).
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "DROP TABLE IF EXISTS public."+staging)
		mustExec(t, dstConns[i], "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")
	}

	const killedSeg = 1
	const cleanSeg = 0

	// ---- PHASE 1: kill one mini-agent mid-unit ----
	// Supervisor + the D real mini-agents (one per segment host). The killed
	// mini gets a short StreamTimeout: it is never streamed its slice (it dies
	// before loading), so it bails out of its data-slice Accept quickly rather
	// than lingering on the default 6h timeout.
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

	cleanRes := make(chan agent.MiniResult, 1)
	for i := 0; i < D; i++ {
		go func(i int) {
			streamTO := 60 * time.Second
			if i == killedSeg {
				streamTO = 5 * time.Second // dies before loading: bail from Accept fast
			}
			res, err := agent.ServeMini(ctx, agent.MiniOptions{
				Seg:           i,
				DataListen:    "127.0.0.1:0",
				PrimaryDSN:    dstCls[i].URL(),
				ReportAddr:    reportAddr,
				AcceptTimeout: 30 * time.Second,
				StreamTimeout: streamTO,
			})
			if err != nil {
				if i == cleanSeg {
					cleanRes <- agent.MiniResult{} // exited via error; the wait below still unblocks
				}
				return
			}
			if i == cleanSeg {
				cleanRes <- res
			}
		}(i)
	}

	var mm agent.MiniMap
	select {
	case e := <-mmErr:
		t.Fatalf("supervisor accept minis: %v", e)
	case mm = <-mmCh:
	case <-time.After(30 * time.Second):
		t.Fatalf("supervisor did not accept %d minis in time", D)
	}
	if len(mm) != D {
		t.Fatalf("MiniMap has %d entries, want %d (mm=%v)", len(mm), D, mm)
	}
	for i := 0; i < D; i++ {
		sup.SetMiniHost(i, "127.0.0.1")
		if addr := sup.MiniDataAddr(i); addr == "" {
			t.Fatalf("mini %d has no data address (MiniMap=%v)", i, mm)
		}
	}

	// Drive the unit on the supervisor (concurrently).
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

	// Stream ONLY the clean segment's slice (the killed mini died before
	// loading, so it is not streamed — its staging slice stays untouched).
	cleanConn, derr := dialRetry(sup.MiniDataAddr(cleanSeg), 20*time.Second)
	if derr != nil {
		t.Fatalf("fake segreceiver dial clean seg: %v", derr)
	}
	if _, serr := segstream.SendCompressed(ctx, cleanConn, bytes.NewReader(slices[cleanSeg]), jobID, unitID, int16(cleanSeg), compress.ModeZstd, 3, true); serr != nil {
		t.Fatalf("fake segreceiver clean seg send: %v", serr)
	}
	_ = cleanConn.Close()

	// Wait until the clean mini has LOADED its slice (its staging holds its
	// full slice). That proves the supervisor has already sent mini-unit to
	// BOTH minis (the clean mini only loads after its mini-unit; the send to
	// the killed mini ran concurrently, microseconds earlier, on localhost)
	// and is now blocked reading the D reports. Only then do we KILL the dead
	// mini: sever its report connection (= the mini-agent process died). The
	// supervisor's next report read on that channel surfaces the orphan
	// (deterministically — not a send failure).
	wantClean := mustCount(t, srcConn, "SELECT count(*) FROM p5km_orders WHERE (id-1) % "+strconv.Itoa(D)+" = 0")
	deadline := time.Now().Add(30 * time.Second)
	for {
		if got := mustCount(t, dstConns[cleanSeg], "SELECT count(*) FROM public."+staging); got == wantClean {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("clean seg %d staging did not reach its slice (%d) within 30s (clean mini stuck?)", cleanSeg, wantClean)
		}
		time.Sleep(100 * time.Millisecond)
	}
	killConn, ok := sup.ReportConn(killedSeg)
	if !ok {
		t.Fatalf("no report connection for seg %d to kill", killedSeg)
	}
	killConn.Close()

	var err1 error
	select {
	case e := <-supErr:
		err1 = e // expected: a UNIT FAILURE naming the orphan
	case res := <-supCh:
		t.Fatalf("phase1 (kill): the unit SUCCEEDED (rows=%d), want a unit failure (seg %d's mini died)", res.Rows, killedSeg)
	case <-time.After(90 * time.Second):
		t.Fatalf("phase1 (kill): the supervisor did not finish the unit in time (hang?)")
	}
	if err1 == nil {
		t.Fatalf("phase1 (kill): ServeUnit returned nil error, want a unit failure (orphan)")
	}
	if !strings.Contains(err1.Error(), "orphan") {
		t.Fatalf("phase1 (kill): unit failure does not name the orphan: %v", err1)
	}
	t.Logf("phase1 (kill): unit FAILED as expected: %v", err1)

	// The killed segment's staging slice is EMPTY (the mini died before
	// loading — no partial rows); the surviving segment's slice is loaded in
	// full (its mini reported Ok before the supervisor gave up). splitBinarySlices
	// deals round-robin, so the clean segment's count = source rows with
	// (id-1)%D==0.
	if got := mustCount(t, dstConns[killedSeg], "SELECT count(*) FROM public."+staging); got != 0 {
		t.Fatalf("killed seg %d staging holds %d rows, want 0 (the mini died before loading)", killedSeg, got)
	}
	// The surviving segment's slice is loaded in full (its mini reported Ok
	// before the supervisor gave up on the orphaned segment). The wait loop
	// above already established wantClean == its full slice.
	if got := mustCount(t, dstConns[cleanSeg], "SELECT count(*) FROM public."+staging); got != wantClean {
		t.Fatalf("clean seg %d staging count %d, want %d (its full slice)", cleanSeg, got, wantClean)
	}
	t.Logf("phase1: killed slice empty (seg %d), clean slice loaded (seg %d = %d rows)", killedSeg, cleanSeg, wantClean)

	// Stop the phase-1 supervisor: the surviving mini's report channel drops
	// -> it must NOT be wedged on the dead channel (it exits).
	sup.Stop()
	select {
	case <-cleanRes: // the clean mini exited (clean or via error) — not wedged
	case <-time.After(15 * time.Second):
		t.Fatalf("clean mini did not exit within 15s of the phase-1 supervisor stopping (wedged?)")
	}
	t.Logf("phase2: clean mini exited on supervisor stop (not wedged)")

	// ---- PHASE 2: resume — FR-07 re-copy with a FRESH mesh ----
	// A run-level resume re-copies the unit from a clean staging state with a
	// fresh set of mini-agents (the dead one is re-launched). Truncate the
	// staging slices, then re-copy with a new supervisor + re-launched minis.
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "TRUNCATE public."+staging)
	}

	sup2, err := agent.NewSupervisor("127.0.0.1:0", nil, "", "", "", "")
	if err != nil {
		t.Fatalf("new supervisor (phase2): %v", err)
	}
	defer sup2.Close()
	reportAddr2 := sup2.Addr()

	mmCh2 := make(chan agent.MiniMap, 1)
	mmErr2 := make(chan error, 1)
	go func() {
		mm, err := sup2.AcceptMinis(ctx, D)
		if err != nil {
			mmErr2 <- err
			return
		}
		mmCh2 <- mm
	}()
	miniRes2 := make(chan agent.MiniResult, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			res, err := agent.ServeMini(ctx, agent.MiniOptions{
				Seg:           i,
				DataListen:    "127.0.0.1:0",
				PrimaryDSN:    dstCls[i].URL(),
				ReportAddr:    reportAddr2,
				AcceptTimeout: 30 * time.Second,
				StreamTimeout: 60 * time.Second,
			})
			if err != nil {
				t.Logf("phase2 mini %d: %v", i, err)
				return
			}
			miniRes2 <- res
		}(i)
	}
	var mm2 agent.MiniMap
	select {
	case e := <-mmErr2:
		t.Fatalf("phase2 supervisor accept minis: %v", e)
	case mm2 = <-mmCh2:
	case <-time.After(30 * time.Second):
		t.Fatalf("phase2 supervisor did not accept %d minis in time", D)
	}
	if len(mm2) != D {
		t.Fatalf("phase2 MiniMap has %d entries, want %d (mm=%v)", len(mm2), D, mm2)
	}
	for i := 0; i < D; i++ {
		sup2.SetMiniHost(i, "127.0.0.1")
		if addr := sup2.MiniDataAddr(i); addr == "" {
			t.Fatalf("phase2 mini %d has no data address (MiniMap=%v)", i, mm2)
		}
	}

	supCh2 := make(chan agent.CombinedUnitResult, 1)
	supErr2 := make(chan error, 1)
	go func() {
		res, err := sup2.ServeUnit(ctx, agent.UnitReportReq{
			ReqID:        2,
			UnitID:       unitID,
			JobID:        jobID,
			Schema:       "public",
			Staging:      staging,
			Compress:     "zstd",
			VerifyDigest: true,
		})
		if err != nil {
			supErr2 <- err
			return
		}
		supCh2 <- res
	}()
	for i := 0; i < D; i++ {
		go func(i int) {
			conn, derr := dialRetry(sup2.MiniDataAddr(i), 20*time.Second)
			if derr != nil {
				t.Errorf("phase2 fake segreceiver %d dial %s: %v", i, sup2.MiniDataAddr(i), derr)
				return
			}
			if _, serr := segstream.SendCompressed(ctx, conn, bytes.NewReader(slices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3, true); serr != nil {
				t.Errorf("phase2 fake segreceiver %d send: %v", i, serr)
			}
			_ = conn.Close()
		}(i)
	}

	var res agent.CombinedUnitResult
	select {
	case e := <-supErr2:
		t.Fatalf("phase2 (resume): the re-copy FAILED: %v", e)
	case res = <-supCh2:
	case <-time.After(90 * time.Second):
		t.Fatalf("phase2 (resume): the supervisor did not finish the unit in time")
	}
	t.Logf("phase2 (resume): rows=%d srcDigest=%q dstDigest=%q perSeg=%v", res.Rows, res.SrcDigest, res.DstDigest, res.PerSeg)

	if res.Rows != rows {
		t.Fatalf("phase2 rows %d != seeded %d (perSeg=%v)", res.Rows, rows, res.PerSeg)
	}
	if res.SrcDigest != refDigest {
		t.Fatalf("phase2 srcDigest %q != reference %q", res.SrcDigest, refDigest)
	}
	if res.DstDigest != refDigest {
		t.Fatalf("phase2 dstDigest %q != reference %q", res.DstDigest, refDigest)
	}
	var total int64
	for i := 0; i < D; i++ {
		got := mustCount(t, dstConns[i], "SELECT count(*) FROM public."+staging)
		if got != res.PerSeg[i] {
			t.Fatalf("phase2 seg %d staging count = %d, want %d", i, got, res.PerSeg[i])
		}
		total += got
	}
	if total != rows {
		t.Fatalf("phase2 sum of D staging slices = %d, want %d", total, rows)
	}

	// Clean stop: both phase-2 minis exit with the unit accounted.
	sup2.Stop()
	for i := 0; i < D; i++ {
		select {
		case mr := <-miniRes2:
			if mr.UnitsServed != 1 {
				t.Fatalf("phase2 mini seg %d served %d units, want 1", i, mr.UnitsServed)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("phase2 mini seg %d did not exit after Stop", i)
		}
	}
	t.Logf("kill-mini crash-matrix: unit failed on the dead mini's orphan, re-copy after truncate restored rows=%d digest=%q", res.Rows, refDigest)
}
