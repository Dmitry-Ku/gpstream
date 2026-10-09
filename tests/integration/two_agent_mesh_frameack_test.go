//go:build gp6lab

package integration

import (
	"bytes"
	"context"
	"net"
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

// TestTwoAgentMeshFrameAck is the regression for the first live A→A mesh run
// (INC7): the REAL mesh segreceiver sends each slice over the frame-ack
// transport (DESIGN §4.6) — a ReliablePipe that greets with an identity
// HELLO (magic "GPSA") — but the mini-agent accepted the data connection and
// read it RAW (segstream.ReadStreamCompressed). The mini's first read was the
// "GPSA" control magic, not a data frame: "stream: bad magic 0x47505341".
// The other mesh integration tests never caught it because they feed the mini
// a FAKE segreceiver that streams raw segstream frames (no frame-ack).
//
// This test drives the REAL ServeMini with FrameAck on and feeds it with a
// REAL ReliablePipe sender (the exact wrapFrameAck form: OnConnID + StreamID,
// compressed payload). It fails with "bad magic" before the mini's receiver
// wrap and passes only when the mini reads the slice through its own
// ReliablePipe receiver (the coordinator-dest pattern).
func TestTwoAgentMeshFrameAck(t *testing.T) {
	const (
		rows    = 20000
		srcPort = uint32(5761)
		dstPort = uint32(5762)
		jobID   = uint32(0x6103)
		unitID  = uint32(3)
		staging = "_gs_cmfa_3"
		table   = "cmfa_orders"
	)
	// D=1: the A→A self-mesh shape (one destination segment).
	const D = 1

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "cmfa_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	dstCl, err := pgtest.Start(dstPort, "cmfa_dst")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	defer dstCl.Stop()

	srcConn, err := pg.NewConn(ctx, srcCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcConn.Close()
	dstConn, err := pg.NewConn(ctx, dstCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect dest: %v", err)
	}
	defer dstConn.Close()

	mustExec(t, srcConn, `DROP TABLE IF EXISTS `+table)
	mustExec(t, srcConn, `CREATE TABLE `+table+` (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO `+table+` SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, table, rows)

	// D=1: a single slice = the whole table.
	slices := splitBinarySlices(t, full, D)
	refAcc, refRows, err := transfer.DigestAccumulator(full)
	if err != nil {
		t.Fatalf("reference DigestAccumulator: %v", err)
	}
	if refRows != int64(rows) {
		t.Fatalf("reference rows %d != seeded %d", refRows, rows)
	}
	refDigest := transfer.CombinedDigestString(refAcc, int64(rows))

	mustExec(t, dstConn, "DROP TABLE IF EXISTS public."+staging)
	mustExec(t, dstConn, "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")

	// Supervisor + the one real mini-agent, WITH FRAME-ACK ON (the production
	// default; launchDestMinis forwards the supervisor's --frame-ack).
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
	go func() {
		_, _ = agent.ServeMini(ctx, agent.MiniOptions{
			Seg:           0,
			DataListen:    "127.0.0.1:0",
			PrimaryDSN:    dstCl.URL(),
			ReportAddr:    reportAddr,
			AcceptTimeout: 30 * time.Second,
			StreamTimeout: 60 * time.Second,
			// THE FIX under test: receive the slice on the frame-ack
			// transport (the segreceiver's --frame-ack default on). Without
			// this the mini reads the "GPSA" HELLO as a data frame.
			FrameAck:     true,
			ReliablePipe: transfer.NewReliablePipeConfig("30s", 20, 1<<30),
		})
	}()
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
	sup.SetMiniHost(0, "127.0.0.1")
	if sup.MiniDataAddr(0) == "" {
		t.Fatalf("mini 0 has no data address (MiniMap=%v)", mm)
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

	// The REAL mesh segreceiver form (cmd/gpstream-segreceiver wrapFrameAck):
	// dial the mini's data listener, wrap the accepted conn in a ReliablePipe
	// SENDER with the slice's StreamID identity HELLO, and stream the
	// compressed slice through it. This is exactly what put "GPSA" on the wire
	// in the failing live A→A run.
	addr := sup.MiniDataAddr(0)
	conn, err := dialRetry(addr, 20*time.Second)
	if err != nil {
		t.Fatalf("fake segreceiver dial: %v", err)
	}
	dialOnce := func() (transfer.DupConn, error) {
		return net.DialTimeout("tcp", addr, 10*time.Second)
	}
	rp, err := transfer.NewReliablePipeSenderOnConnID(
		transfer.NewReliablePipeConfig("30s", 20, 1<<30), conn, dialOnce,
		transfer.StreamID{JobID: jobID, UnitID: unitID, Content: 0})
	if err != nil {
		t.Fatalf("frame-ack sender: %v", err)
	}
	if _, serr := segstream.SendCompressed(ctx, rp, bytes.NewReader(slices[0]), jobID, unitID, 0, compress.ModeZstd, 3, true); serr != nil {
		t.Fatalf("fake segreceiver send (frame-ack): %v", serr)
	}
	_ = rp.Close()

	var res agent.CombinedUnitResult
	select {
	case e := <-supErr:
		t.Fatalf("supervisor ServeUnit (frame-ack mesh): %v", e)
	case res = <-supCh:
	case <-time.After(60 * time.Second):
		t.Fatalf("supervisor ServeUnit did not return within 60s")
	}
	t.Logf("frame-ack mesh: rows=%d srcDigest=%q dstDigest=%q wire=%d logical=%d",
		res.Rows, res.SrcDigest, res.DstDigest, res.Wire, res.Logical)

	if res.Rows != int64(rows) {
		t.Fatalf("combined rows %d != seeded %d (perSeg=%v)", res.Rows, rows, res.PerSeg)
	}
	if res.SrcDigest != refDigest {
		t.Fatalf("srcDigest %q != reference %q", res.SrcDigest, refDigest)
	}
	if res.DstDigest != refDigest {
		t.Fatalf("dstDigest %q != reference %q", res.DstDigest, refDigest)
	}
	if got := mustCount(t, dstConn, "SELECT count(*) FROM public."+staging); got != int64(rows) {
		t.Fatalf("staging count = %d, want %d", got, rows)
	}
}
