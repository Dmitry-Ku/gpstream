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

// TestTwoAgentMeshKillSegreceiver is the P5 mesh crash-matrix scenario
// "kill-segreceiver": a source segreceiver dies MID-UNIT (its data connection
// drops before the trailer is sent — no trailer = the slice is incomplete).
// The destination segment's mini-agent must FAIL THE UNIT (not hang, not
// load a partial slice, not silently keep the partial COPY), and the unit's
// recovery must be a RE-COPY after the staging slices are truncated (FR-07):
// a re-serve of the same unit with intact slices then completes with the
// combined digest == the reference.
//
// The guarantee under test already exists in the code (segstream.
// ReadStreamCompressed returns ErrTruncatedStream on a stream that ends
// without a trailer; serveMiniUnit fails the unit on it) — this test proves
// it end-to-end through the real mesh: a real agent.Supervisor coordinating D
// real agent.ServeMini mini-agents on D separate pgtest clusters (each a
// "segment host" holding its local staging slice), with one FAKE segreceiver
// killed mid-stream (two compressed data frames, then the connection dies —
// the exact wire shape of a segreceiver whose process was killed before the
// trailer) and the other streaming its slice cleanly.
//
// Invariants checked:
//  1. PHASE 1 (kill): the supervisor's ServeUnit returns a UNIT FAILURE
//     naming the truncated slice (stream ended without trailer); the killed
//     segment's staging slice stays EMPTY (the failed utility COPY rolls
//     back — no partial rows land); the clean segment's slice is loaded in
//     full; the mini-agents stay ALIVE (a dead segreceiver is not a dead
//     mini).
//  2. PHASE 2 (resume): after TRUNCATE of the staging slices, a re-serve of
//     the same unit with intact slices succeeds: combined rows == the seeded
//     count, BOTH combined srcDigest and dstDigest == the buffered reference
//     digest (XOR_D == the full accumulator), and each staging slice holds
//     exactly its PerSeg count.
func TestTwoAgentMeshKillSegreceiver(t *testing.T) {
	const (
		srcPort = 5685
		rows    = 20000
		jobID   = uint32(0x51a2)
		unitID  = uint32(3)
		D       = 2 // two destination "segments" (two separate clusters)
	)
	dstPorts := []uint32{5686, 5687}
	if len(dstPorts) != D {
		t.Fatalf("internal: %d dst ports for D=%d", len(dstPorts), D)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "p5kr_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()

	dstCls := make([]*pgtest.Cluster, D)
	dstConns := make([]*pg.Conn, D)
	for i := 0; i < D; i++ {
		cl, err := pgtest.Start(dstPorts[i], fmt.Sprintf("p5kr_dst_%d", i))
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
	mustExec(t, srcConn, `DROP TABLE IF EXISTS p5kr_orders`)
	mustExec(t, srcConn, `CREATE TABLE p5kr_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO p5kr_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "p5kr_orders", rows)

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

	// Staging slice in EACH segment cluster (the supervisor/coordinator
	// creates/truncates it; the minis only COPY into the existing slice).
	const staging = "_gs_p5kr_3"
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "DROP TABLE IF EXISTS public."+staging)
		mustExec(t, dstConns[i], "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")
	}

	// The supervisor + the D real mini-agents.
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

	// ---- PHASE 1: kill one segreceiver mid-unit (no trailer) ----
	// The supervisor broadcasts mini-unit to both minis; seg 0 streams its
	// slice cleanly, seg 1's "segreceiver" sends two compressed data frames
	// and then the connection DIES (the process was killed) — no trailer.
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

	const killedSeg = 1
	errCh := make(chan error, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			addr := sup.MiniDataAddr(i)
			conn, derr := dialRetry(addr, 20*time.Second)
			if derr != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial %s: %w", i, addr, derr)
				return
			}
			if i == killedSeg {
				// KILL: two compressed data frames (the slice's head),
				// then the connection dies — no trailer frame.
				if kerr := killSegStream(conn, slices[i], jobID, unitID, int16(i)); kerr != nil {
					errCh <- fmt.Errorf("killed segreceiver %d: %w", i, kerr)
					return
				}
				errCh <- nil
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

	select {
	case e := <-supErr:
		// Expected: a UNIT FAILURE naming the truncated slice.
		t.Logf("phase1 (kill): unit FAILED as expected: %v", e)
		if !strings.Contains(e.Error(), "stream ended without trailer") {
			t.Fatalf("unit failure does not name the truncated slice: %v", e)
		}
	case res := <-supCh:
		t.Fatalf("phase1 (kill): the unit SUCCEEDED (rows=%d), want a unit failure (the killed slice has no trailer)", res.Rows)
	case <-time.After(90 * time.Second):
		t.Fatalf("phase1 (kill): the supervisor did not finish the unit in time (hang?)")
	}

	// The killed segment's staging slice must be EMPTY: the failed utility
	// COPY rolls back — no partial rows land (the mini wrote a truncated
	// stream into the COPY, the COPY aborted, the slice is untouched).
	if got := mustCount(t, dstConns[killedSeg], "SELECT count(*) FROM public."+staging); got != 0 {
		t.Fatalf("killed seg %d staging holds %d rows, want 0 (the failed COPY must roll back)", killedSeg, got)
	}
	// The clean segment's slice is loaded in full. splitBinarySlices deals
	// the rows round-robin, so segment 0's expected count is the source
	// table's rows with (id-1) % D == 0 (id 1 = the first row).
	wantClean := mustCount(t, srcConn, "SELECT count(*) FROM p5kr_orders WHERE (id-1) % "+strconv.Itoa(D)+" = 0")
	if got := mustCount(t, dstConns[0], "SELECT count(*) FROM public."+staging); got != wantClean {
		t.Fatalf("clean seg 0 staging count %d, want %d (its full slice)", got, wantClean)
	}
	t.Logf("phase1: killed slice rolled back (seg %d empty), clean slice loaded (seg 0 = %d rows)", killedSeg, wantClean)

	// ---- PHASE 2: resume — truncate the staging slices, re-copy ----
	// FR-07 recovery: the unit is re-copied from a clean staging state. The
	// mini-agents are the SAME long-lived processes (a dead segreceiver is
	// not a dead mini) — the re-serve reuses their report + data channels.
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "TRUNCATE public."+staging)
	}

	supCh2 := make(chan agent.CombinedUnitResult, 1)
	supErr2 := make(chan error, 1)
	go func() {
		res, err := sup.ServeUnit(ctx, agent.UnitReportReq{
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
	errCh2 := make(chan error, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			conn, derr := dialRetry(sup.MiniDataAddr(i), 20*time.Second)
			if derr != nil {
				errCh2 <- fmt.Errorf("fake segreceiver %d dial %s: %w", i, sup.MiniDataAddr(i), derr)
				return
			}
			if _, serr := segstream.SendCompressed(ctx, conn, bytes.NewReader(slices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3, true); serr != nil {
				errCh2 <- fmt.Errorf("fake segreceiver %d send: %w", i, serr)
				return
			}
			_ = conn.Close()
			errCh2 <- nil
		}(i)
	}
	for i := 0; i < D; i++ {
		if e := <-errCh2; e != nil {
			t.Fatalf("phase2 fake segreceiver: %v", e)
		}
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

	// The re-copy completes with the full row count and the combined digest
	// == the buffered reference (XOR_D == the full accumulator, FR-02).
	if res.Rows != rows {
		t.Fatalf("phase2 rows %d != seeded %d (perSeg=%v)", res.Rows, rows, res.PerSeg)
	}
	if res.SrcDigest != refDigest {
		t.Fatalf("phase2 srcDigest %q != reference %q", res.SrcDigest, refDigest)
	}
	if res.DstDigest != refDigest {
		t.Fatalf("phase2 dstDigest %q != reference %q", res.DstDigest, refDigest)
	}
	// Each staging slice holds exactly its PerSeg count; together they cover
	// every row once.
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

	// Clean stop: both mini-agents exit with the unit accounted (they served
	// the failed phase-1 unit AND the phase-2 re-copy = 2 units each).
	sup.Stop()
	for i := 0; i < D; i++ {
		select {
		case mr := <-miniRes:
			if mr.UnitsServed != 2 {
				t.Fatalf("mini seg %d served %d units, want 2 (phase1 + phase2)", i, mr.UnitsServed)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("mini seg %d did not exit after Stop", i)
		}
	}
	t.Logf("kill-segreceiver crash-matrix: unit failed on the trailer-less slice, re-copy after truncate restored rows=%d digest=%q", res.Rows, refDigest)
}

// killSegStream plays a segreceiver that is KILLED mid-stream: it writes two
// COMPRESSED data frames (the slice's head, zstd level 3 — the same
// compressor the clean SendCompressed path uses) and then the connection
// dies. No trailer frame is ever sent: the receiver must flag the stream as
// truncated (ErrTruncatedStream) instead of treating it as a complete slice.
func killSegStream(conn transfer.Pipe, slice []byte, jobID, unitID uint32, content int16) error {
	defer conn.Close()
	comp := compress.New(compress.ModeZstd, 3)
	const chunk = 16 * 1024
	off := 0
	for i := 0; i < 2 && off < len(slice); i++ {
		end := off + chunk
		if end > len(slice) {
			end = len(slice)
		}
		if werr := transfer.WriteFrame(conn, &transfer.Frame{
			JobID: jobID, UnitID: unitID, Seq: uint64(i), ContentID: content,
			Data: comp.Encode(slice[off:end]),
		}); werr != nil {
			return fmt.Errorf("frame %d: %w", i, werr)
		}
		off = end
	}
	conn.Close() // the segreceiver process dies: no trailer
	return nil
}
