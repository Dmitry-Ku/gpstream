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

// TestTwoAgentMeshDirectE2E is the P3 production-dispatch (INC4) acceptance for
// the mesh mini WRITE side at D=1 and D=3 — the dev A→A (single destination
// segment) and A→D (three destination segments) shapes. It drives the REAL
// production mesh mini path end-to-end on a plain-PostgreSQL stand:
//
//   - a REAL supervisor (agent.Supervisor) + D REAL mini-agents
//     (agent.ServeMini), which is exactly what the controller launches in
//     production (one mini per destination segment host);
//   - D fake segreceivers streaming real table bytes, split into D
//     round-robin content-tagged COMPRESSED slices, each to its destination
//     segment's mini data address (the wire form of the ON SEGMENT dispatch,
//     which is Greenplum-only and is validated live on the A/D/B matrix);
//   - each mini performs a LOCAL utility COPY into its segment's staging slice
//     (fanIn=1: each mini accepts exactly ONE slice — the direct_1to1 shape
//     the INC4 MiniLaunch.SourceSegments fix selects);
//   - the supervisor combines the D reports (XOR_D) and reports the summed
//     rows + the composable content digest.
//
// This is the write-side counterpart of the P5 crash tests (which cover the
// SAME path at D=2 plus a killed mini); here the run is CLEAN (no crash) and
// runs at the two D values the dev matrix actually uses.
func TestTwoAgentMeshDirectE2E(t *testing.T) {
	const rows = 20000
	// Dedicated port block (clear of the existing tests' blocks).
	cases := []struct {
		D        int
		srcPort  uint32
		dstPorts []uint32
		jobID    uint32
		unitID   uint32
		staging  string
	}{
		{D: 1, srcPort: 5711, dstPorts: []uint32{5712}, jobID: 0x6101, unitID: 1, staging: "_gs_cm1_1"},
		{D: 3, srcPort: 5713, dstPorts: []uint32{5714, 5715, 5716}, jobID: 0x6102, unitID: 2, staging: "_gs_cm3_2"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("D=%d", tc.D), func(t *testing.T) {
			runCleanMeshDirectE2E(t, tc.D, tc.srcPort, tc.dstPorts, tc.jobID, tc.unitID, tc.staging, rows)
		})
	}
}

func runCleanMeshDirectE2E(t *testing.T, D int, srcPort uint32, dstPorts []uint32, jobID, unitID uint32, staging string, rows int) {
	t.Helper()
	if len(dstPorts) != D {
		t.Fatalf("internal: %d dst ports for D=%d", len(dstPorts), D)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Source cluster + D destination "segment" clusters (each a separate
	// pgtest cluster = one segment host holding its local staging slice).
	srcCl, err := pgtest.Start(srcPort, fmt.Sprintf("cm%d_src", D))
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	dstCls := make([]*pgtest.Cluster, D)
	dstConns := make([]*pg.Conn, D)
	for i := 0; i < D; i++ {
		cl, err := pgtest.Start(dstPorts[i], fmt.Sprintf("cm%d_dst_%d", D, i))
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
	const table = "cm_orders"
	mustExec(t, srcConn, `DROP TABLE IF EXISTS `+table)
	mustExec(t, srcConn, `CREATE TABLE `+table+` (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO `+table+` SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, table, rows)

	// Split into D round-robin slices (the kernel's per-segment shape). The
	// XOR of the D slice accumulators == the full table accumulator (XOR is
	// commutative/associative) — so the supervisor's combined digest must
	// match the whole-table reference.
	slices := splitBinarySlices(t, full, D)
	refAcc, refRows, err := transfer.DigestAccumulator(full)
	if err != nil {
		t.Fatalf("reference DigestAccumulator: %v", err)
	}
	if refRows != int64(rows) {
		t.Fatalf("reference rows %d != seeded %d", refRows, rows)
	}
	refDigest := transfer.CombinedDigestString(refAcc, int64(rows))

	// Staging slice in EACH segment cluster (the supervisor creates/truncates
	// it; the minis only COPY into the existing slice). On a real GP6 the
	// supervisor's coordinator DDL replicates the distributed staging table to
	// the segments; on this plain-PG stand each is a local table.
	for i := 0; i < D; i++ {
		mustExec(t, dstConns[i], "DROP TABLE IF EXISTS public."+staging)
		mustExec(t, dstConns[i], "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")
	}

	// Supervisor + the D real mini-agents (one per segment host).
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
	for i := 0; i < D; i++ {
		go func(i int) {
			_, _ = agent.ServeMini(ctx, agent.MiniOptions{
				Seg:           i,
				DataListen:    "127.0.0.1:0",
				PrimaryDSN:    dstCls[i].URL(),
				ReportAddr:    reportAddr,
				AcceptTimeout: 30 * time.Second,
				StreamTimeout: 60 * time.Second,
			})
			// A mini exits once its unit is done (the supervisor sends it
			// mini-done at the end); the supervisor's ServeUnit result is
			// the authoritative one.
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
		if sup.MiniDataAddr(i) == "" {
			t.Fatalf("mini %d has no data address (MiniMap=%v)", i, mm)
		}
	}

	// Drive the CLEAN unit on the supervisor (concurrently): it broadcasts
	// mini-unit (fanIn=1) to all D minis and reads the D reports (XOR_D).
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

	// Stream ALL D slices (fake segreceivers), each to its destination
	// segment's mini data address, content-tagged with its segment id.
	errCh := make(chan error, D)
	for i := 0; i < D; i++ {
		go func(i int) {
			conn, err := dialRetry(sup.MiniDataAddr(i), 20*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial: %w", i, err)
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

	// Wait for the supervisor's combined result.
	var res agent.CombinedUnitResult
	select {
	case e := <-supErr:
		t.Fatalf("supervisor ServeUnit (clean mesh): %v", e)
	case res = <-supCh:
	case <-time.After(60 * time.Second):
		t.Fatalf("supervisor ServeUnit did not return within 60s")
	}
	t.Logf("D=%d clean mesh: rows=%d srcDigest=%q dstDigest=%q wire=%d logical=%d perSeg=%v",
		D, res.Rows, res.SrcDigest, res.DstDigest, res.Wire, res.Logical, res.PerSeg)

	// 1. The combined row count == the seeded rows (sum of the D utility COPYs).
	if res.Rows != int64(rows) {
		t.Fatalf("D=%d combined rows %d != seeded %d (perSeg=%v)", D, res.Rows, rows, res.PerSeg)
	}
	// 2. The combined content digest == the whole-table reference (XOR_D of the
	//    per-segment accumulators over the summed row count).
	if res.SrcDigest != refDigest {
		t.Fatalf("D=%d srcDigest %q != reference %q", D, res.SrcDigest, refDigest)
	}
	// 3. The on-wire volume is COMPRESSED (< logical).
	if res.Wire >= res.Logical {
		t.Fatalf("D=%d on-wire %d >= logical %d: not compressed on the network", D, res.Wire, res.Logical)
	}
	// 4. The per-segment distribution covers every row exactly once, and each
	//    segment's staging slice holds exactly its share (the F-15 balance
	//    cross-check, read back from the destination segments).
	var total int64
	for i := 0; i < D; i++ {
		got := mustCount(t, dstConns[i], "SELECT count(*) FROM public."+staging)
		if got != res.PerSeg[i] {
			t.Fatalf("D=%d seg %d staging count = %d, want PerSeg[%d]=%d", D, i, got, i, res.PerSeg[i])
		}
		total += got
	}
	if total != int64(rows) {
		t.Fatalf("D=%d sum of D staging slices = %d, want %d", D, total, rows)
	}
	if len(res.PerSeg) != D {
		t.Fatalf("D=%d PerSeg has %d entries, want %d", D, len(res.PerSeg), D)
	}
}
