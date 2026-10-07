//go:build gp6lab

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// recvLineFrom reads one newline-terminated control message from r and
// unmarshals it into out (the test's stand-in for the mini-agent's recvLine,
// which is unexported). It returns io.EOF at a clean end of stream.
func recvLineFrom(r *bufio.Reader, out any) error {
	b, err := r.ReadBytes('\n')
	if len(b) > 0 {
		if uerr := json.Unmarshal(b, out); uerr != nil {
			return fmt.Errorf("unmarshal %q: %w", b, uerr)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

// TestTwoAgentMiniServe is the P2 (direct_1to1 mesh) acceptance for the
// MINI-AGENT: a real agent.ServeMini on a destination segment loads ONE
// compressed slice into ONE utility-mode COPY into the local staging slice,
// and reports {unit, srcContrib, dstAcc, rows, ok} to the supervisor over a
// persistent report channel.
//
// The test plays the SUPERVISOR (the dest-agent on the coordinator): it runs
// the report listener (accepts the mini-agent's persistent dial, reads its
// hello {seg,dataPort,ready}), creates the staging table (the coordinator's
// job, not the mini's), sends the per-unit mini-unit request, streams the
// slice to the mini's data endpoint as the fake segreceiver, and reads the
// mini's report. On plain PostgreSQL every "segment" is the same cluster, so
// the mini's PrimaryDSN is the cluster URL — the per-segment routing is what
// ServeMini exercises (one slice -> one utility COPY), exactly as the P8
// direct_1to1 test exercises it for ServeDirect (D segments).
//
// Invariants checked:
//  1. the mini wrote exactly the seeded row count (authoritative COPY tag),
//     matching the trailer cross-check;
//  2. the staging table on the destination has the source's content (count +
//     order-independent digest);
//  3. the on-wire volume is the COMPRESSED volume (< logical, ADR-002);
//  4. srcContrib (the slice trailer's raw per-row XOR accumulator) equals
//     dstAcc (the destination's own data-path accumulator) for a single-segment
//     unit, and both equal the buffered reference — the composable-digest
//     invariant the P3 supervisor relies on to XOR_D across segments;
//  5. a clean "mini-done" makes ServeMini exit with the unit accounted.
func TestTwoAgentMiniServe(t *testing.T) {
	const (
		srcPort = 5633
		dstPort = 5634
		rows    = 20000
		jobID   = uint32(0x89ab)
		unitID  = uint32(11)
		staging = "_gs_mini_11"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "mini_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	dstCl, err := pgtest.Start(dstPort, "mini_dst")
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

	// Seed a compressible source table and read its binary-COPY stream (the
	// single slice for this one-segment unit).
	mustExec(t, srcConn, `DROP TABLE IF EXISTS mini_orders`)
	mustExec(t, srcConn, `CREATE TABLE mini_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO mini_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "mini_orders", rows)

	// The supervisor creates the staging table (the coordinator's job) so the
	// mini's utility COPY has a target. Plain PG: same shape as the source.
	mustExec(t, dstConn, "DROP TABLE IF EXISTS public."+staging+"")
	mustExec(t, dstConn, "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")

	// Reference digests (buffered, order-independent) of the slice payload.
	refAcc, refRows, err := transfer.DigestAccumulator(full)
	if err != nil {
		t.Fatalf("reference DigestAccumulator: %v", err)
	}
	if refRows != rows {
		t.Fatalf("reference rows %d != seeded %d", refRows, rows)
	}
	refDigest, err := transfer.ContentDigest(full)
	if err != nil {
		t.Fatalf("reference ContentDigest: %v", err)
	}

	// The supervisor's report listener (the mini dials it once, persistently).
	reportL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("report listen: %v", err)
	}
	defer reportL.Close()

	// Run the mini-agent in the background.
	dataListen := freePort(t)
	miniResCh := make(chan agent.MiniResult, 1)
	miniErrCh := make(chan error, 1)
	go func() {
		res, err := agent.ServeMini(ctx, agent.MiniOptions{
			Seg:           0,
			DataListen:    dataListen,
			PrimaryDSN:    dstCl.URL(),
			ReportAddr:    reportL.Addr().String(),
			AcceptTimeout: 30 * time.Second,
			StreamTimeout: 60 * time.Second,
		})
		if err != nil {
			miniErrCh <- err
			return
		}
		miniResCh <- res
	}()

	// Supervisor: accept the mini's report dial + read its hello.
	reportConn, err := reportL.Accept()
	if err != nil {
		t.Fatalf("accept report: %v", err)
	}
	defer reportConn.Close()
	rr := bufio.NewReader(reportConn)
	var hello agent.MiniHello
	if err := recvLineFrom(rr, &hello); err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if hello.Op != "mini-hello" || hello.Seg != 0 || !hello.Ready || hello.DataPort <= 0 {
		t.Fatalf("bad hello: %+v", hello)
	}
	dataAddr := fmt.Sprintf("127.0.0.1:%d", hello.DataPort)
	t.Logf("mini hello: seg=%d dataPort=%d ready=%v", hello.Seg, hello.DataPort, hello.Ready)

	// Supervisor: send the per-unit request, then stream the slice as the fake
	// segreceiver to the mini's data endpoint.
	if err := agent.SendLine(reportConn, agent.MiniUnitReq{
		Op:           "mini-unit",
		ReqID:        1,
		UnitID:       unitID,
		JobID:        jobID,
		Schema:       "public",
		Staging:      staging,
		Compress:     "zstd",
		VerifyDigest: true,
	}); err != nil {
		t.Fatalf("send mini-unit: %v", err)
	}
	sconn, err := dialRetry(dataAddr, 10*time.Second)
	if err != nil {
		t.Fatalf("dial mini data %s: %v", dataAddr, err)
	}
	if _, err := segstream.SendCompressed(ctx, sconn, bytes.NewReader(full), jobID, unitID, 0, compress.ModeZstd, 3, true); err != nil {
		t.Fatalf("fake segreceiver send: %v", err)
	}
	_ = sconn.Close()

	// Supervisor: read the mini's per-unit report.
	var resp agent.MiniUnitResp
	if err := recvLineFrom(rr, &resp); err != nil {
		t.Fatalf("read mini-unit-resp: %v", err)
	}
	t.Logf("mini report: ok=%v rows=%d trailer=%d wire=%d logical=%d srcContrib=%q dstAcc=%q err=%q",
		resp.Ok, resp.Rows, resp.TrailerRows, resp.Wire, resp.Logical, resp.SrcContrib, resp.DstAcc, resp.Error)

	// 1. Authoritative row count + trailer cross-check.
	if !resp.Ok {
		t.Fatalf("mini unit failed: %s", resp.Error)
	}
	if resp.Rows != rows || resp.TrailerRows != rows {
		t.Fatalf("row count mismatch: dst=%d trailer=%d want=%d", resp.Rows, resp.TrailerRows, rows)
	}
	// 3. ADR-002: the network carried the COMPRESSED volume.
	if resp.Wire >= uint64(len(full)) {
		t.Fatalf("on-wire %d >= logical %d: not compressed on the network", resp.Wire, len(full))
	}
	// 4. Composable-digest invariant (single segment: srcContrib == dstAcc ==
	//    the reference accumulator; the combined digest matches the reference).
	refHex := hexOf(refAcc)
	if resp.SrcContrib != refHex {
		t.Fatalf("srcContrib %q != reference %q", resp.SrcContrib, refHex)
	}
	if resp.DstAcc != refHex {
		t.Fatalf("dstAcc %q != reference %q", resp.DstAcc, refHex)
	}
	if combined := transfer.CombinedDigestString(refAcc, rows); combined != refDigest {
		t.Fatalf("combined digest %q != reference %q", combined, refDigest)
	}

	// 5. Clean "mini-done" -> the mini exits with the unit accounted.
	if err := agent.SendLine(reportConn, agent.MiniDoneReq{Op: "mini-done"}); err != nil {
		t.Fatalf("send mini-done: %v", err)
	}
	var mres agent.MiniResult
	select {
	case e := <-miniErrCh:
		t.Fatalf("mini agent: %v", e)
	case mres = <-miniResCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("mini did not exit after mini-done")
	}
	if mres.UnitsServed != 1 || mres.Rows != rows {
		t.Fatalf("mini result: units=%d rows=%d (want 1 / %d)", mres.UnitsServed, mres.Rows, rows)
	}

	// 2. The staging table on the destination has the source's content.
	if got := mustCount(t, dstConn, "SELECT count(*) FROM public."+staging+""); got != rows {
		t.Fatalf("dest staging count = %d, want %d", got, rows)
	}
	wantDigest := mustText(t, srcConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM mini_orders r`)
	gotDigest := mustText(t, dstConn,
		"SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM public."+staging+" r")
	if gotDigest != wantDigest {
		t.Fatalf("dest staging content digest %q != source %q", gotDigest, wantDigest)
	}
	t.Logf("mini serve: rows=%d wire=%d logical=%d digest=%q (single-segment direct_1to1 mesh)",
		mres.Rows, mres.Wire, mres.Logical, refDigest)
}

// hexOf renders a raw 32-byte accumulator as lower-case hex (mirrors the
// mini-agent's composable-digest form).
func hexOf(b [32]byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hexd[c>>4], hexd[c&0x0f])
	}
	return string(out)
}
