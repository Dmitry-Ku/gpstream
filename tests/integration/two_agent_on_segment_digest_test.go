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
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// TestTwoAgentOnSegmentDigests is the P0b acceptance (the mesh srcDigest/
// dstDigest groundwork, local + in-proc): a REAL destination agent
// (agent.ServeSegments) with Spec.VerifyDigest on computes BOTH content
// digests on the on_segment data path:
//
//   - SrcDigest: the XOR of the per-slice trailer digests (each fake
//     segreceiver carried its slice's per-row XOR accumulator in the 72-byte
//     trailer, exactly as the kernel-launched segreceivers do with --digest on)
//     combined with the total row count — order- and split-independent;
//   - DstDigest: the MERGED (SliceMux) binary-COPY stream tee'd through the
//     O(1) digest writer as it flows into the COPY FROM.
//
// Both must equal the reference digest of the FULL source binary-COPY stream
// (the row multiset the kernel would have produced), which proves the
// destination loaded exactly the source's rows. The manager's verify stays on
// the re-read fallback in P0b (the plan: "verify-on пока re-read fallback");
// this test exercises the groundwork the mesh verify (P3) will consume.
func TestTwoAgentOnSegmentDigests(t *testing.T) {
	const (
		srcPort  = 5636
		dstPort  = 5637
		rows     = 20000
		jobID    = 0x5679
		unitID   = 8
		finalTab = "segdig_orders"
		staging  = "_gs_testjob_8"
		nSeg     = 2
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	defer dstCl.Stop()

	srcConn, err := pg.NewConn(ctx, srcCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcConn.Close()

	// Seed a source table and read its full binary-COPY stream (the row
	// multiset the kernel's ON SEGMENT dispatch would produce).
	mustExec(t, srcConn, `DROP TABLE IF EXISTS segdig_orders`)
	mustExec(t, srcConn, `CREATE TABLE segdig_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO segdig_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryN(t, srcConn, rows)

	// Reference digest of the full stream (rows-only, order-independent):
	// the value both on-the-data-path digests must reproduce.
	wantDigest, err := transfer.ContentDigest(full)
	if err != nil {
		t.Fatalf("reference digest: %v", err)
	}

	// The unit shape, captured from the source.
	sa := source.New(srcConn)
	cols, err := sa.Columns(ctx, "public", finalTab)
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	dist := sa.Distribution(ctx, "public", finalTab)
	part, _ := pg.CapturePartitionSpec(srcConn, ctx, "public", finalTab) // non-fatal (plain PG)

	// Split the stream into N slices (the kernel's per-segment shape: each
	// slice is a self-contained binary-COPY stream with its own header/EOF).
	slices := splitBinarySlices(t, full, nSeg)

	// Run the on_segment unit once with the digest ON and once with it OFF,
	// to prove the wire form is driven by Spec.VerifyDigest (the segreceivers
	// are the same; only the trailer digest + the dest tee differ).
	run := func(verifyDigest bool, stageSfx string) agent.SegmentsResult {
		stage := staging + stageSfx
		listen := freePort(t)
		sresCh := make(chan agent.SegmentsResult, 1)
		serrCh := make(chan error, 1)
		go func() {
			res, err := agent.ServeSegments(ctx, agent.SegmentsOptions{
				Listen:   listen,
				DestConn: dstCl.URL(),
				Spec: agent.UnitSpec{
					JobID: jobID, UnitID: unitID,
					Schema: "public", Table: finalTab, Staging: stage, Overwrite: true,
					Compress:     "zstd",
					Cols:         cols, Dist: dist, Part: part,
					VerifyDigest: verifyDigest,
				},
				Segments:      nSeg,
				Swap:          true,
				AcceptTimeout: 30 * time.Second,
				StreamTimeout: 60 * time.Second,
			})
			if err != nil {
				serrCh <- err
				return
			}
			sresCh <- res
		}()

		time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
		errCh := make(chan error, nSeg)
		for i := 0; i < nSeg; i++ {
			go func(i int) {
				conn, err := dialRetry(listen, 90*time.Second)
				if err != nil {
					errCh <- fmt.Errorf("fake segreceiver %d dial: %w", i, err)
					return
				}
				if _, err := segstream.SendCompressed(ctx, conn, bytes.NewReader(slices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3, verifyDigest); err != nil {
					errCh <- fmt.Errorf("fake segreceiver %d send: %w", i, err)
					return
				}
				errCh <- nil
			}(i)
		}
		for i := 0; i < nSeg; i++ {
			if e := <-errCh; e != nil {
				t.Fatalf("fake segreceiver: %v", e)
			}
		}

		var sres agent.SegmentsResult
		select {
		case serr := <-serrCh:
			t.Fatalf("dest agent serve segments (verifyDigest=%v): %v", verifyDigest, serr)
		case sres = <-sresCh:
		}
		if sres.Rows != rows || sres.TrailerRows != rows {
			t.Fatalf("row count mismatch (verifyDigest=%v): dst=%d trailer=%d want=%d",
				verifyDigest, sres.Rows, sres.TrailerRows, rows)
		}
		if !sres.Swapped {
			t.Fatalf("dest agent did not swap (verifyDigest=%v)", verifyDigest)
		}
		return sres
	}

	// 1. Digest ON: both on-the-data-path digests must equal the reference.
	sres := run(true, "")
	if sres.SrcDigest != wantDigest {
		t.Errorf("SrcDigest = %q, want reference %q (XOR of per-slice trailer digests != full-stream digest)",
			sres.SrcDigest, wantDigest)
	}
	if sres.DstDigest != wantDigest {
		t.Errorf("DstDigest = %q, want reference %q (merged-stream tee != full-stream digest)",
			sres.DstDigest, wantDigest)
	}
	if sres.SrcDigest != sres.DstDigest {
		t.Errorf("SrcDigest %q != DstDigest %q", sres.SrcDigest, sres.DstDigest)
	}
	t.Logf("digests ok: src=%s dst=%s (rows=%d, wire=%d)", sres.SrcDigest, sres.DstDigest, sres.Rows, sres.Wire)

	// 2. Digest OFF: the groundwork is off by default (no cost when
	// --verify is off) — both fields must be empty.
	sresOff := run(false, "_off")
	if sresOff.SrcDigest != "" || sresOff.DstDigest != "" {
		t.Errorf("digest off: SrcDigest=%q DstDigest=%q, want both empty", sresOff.SrcDigest, sresOff.DstDigest)
	}

	// 3. Negative: flipping ONE data byte in a slice must change BOTH the
	// source-side (trailer XOR) and the destination-side (merged tee) digest,
	// and both must diverge from the reference (the digests cover the payload).
	corrupted := make([]byte, len(slices[0]))
	copy(corrupted, slices[0])
	// Offset 20 is inside row 1's first field data (19B header + fieldCount
	// 2B + field length 4B => data starts at 25; 20 is the field length's
	// last byte... use 26 to be safely inside the field data).
	corrupted[26] ^= 0x01
	negSlices := [][]byte{corrupted, slices[1]}
	listen := freePort(t)
	sresCh := make(chan agent.SegmentsResult, 1)
	serrCh := make(chan error, 1)
	go func() {
		res, err := agent.ServeSegments(ctx, agent.SegmentsOptions{
			Listen:   listen,
			DestConn: dstCl.URL(),
			Spec: agent.UnitSpec{
				JobID: jobID, UnitID: unitID,
				Schema: "public", Table: finalTab, Staging: staging + "_neg", Overwrite: true,
				Compress:     "zstd",
				Cols:         cols, Dist: dist, Part: part,
				VerifyDigest: true,
			},
			Segments:      nSeg,
			Swap:          true,
			AcceptTimeout: 30 * time.Second,
			StreamTimeout: 60 * time.Second,
		})
		if err != nil {
			serrCh <- err
			return
		}
		sresCh <- res
	}()
	time.Sleep(50 * time.Millisecond)
	errCh := make(chan error, nSeg)
	for i := 0; i < nSeg; i++ {
		go func(i int) {
			conn, err := dialRetry(listen, 90*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial: %w", i, err)
				return
			}
			if _, err := segstream.SendCompressed(ctx, conn, bytes.NewReader(negSlices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3, true); err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d send: %w", i, err)
				return
			}
			errCh <- nil
		}(i)
	}
	for i := 0; i < nSeg; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("fake segreceiver (negative): %v", e)
		}
	}
	var neg agent.SegmentsResult
	select {
	case serr := <-serrCh:
		t.Fatalf("dest agent serve segments (negative): %v", serr)
	case neg = <-sresCh:
	}
	if neg.SrcDigest == wantDigest {
		t.Errorf("negative: SrcDigest unchanged after a 1-bit flip (want a different digest)")
	}
	if neg.DstDigest == wantDigest {
		t.Errorf("negative: DstDigest unchanged after a 1-bit flip (want a different digest)")
	}
	if neg.SrcDigest != neg.DstDigest {
		t.Errorf("negative: SrcDigest %q != DstDigest %q (both saw the same corrupted bytes)",
			neg.SrcDigest, neg.DstDigest)
	}
	t.Logf("negative ok: corrupted src=%s dst=%s (reference %s)", neg.SrcDigest, neg.DstDigest, wantDigest)
}

// readCopyBinaryN reads a binary COPY stream fully from the source and checks
// the row count (the same path the production source agent uses).
func readCopyBinaryN(t *testing.T, q *pg.Conn, want int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	rows, err := source.New(q).CopyToStream(context.Background(), "public", "segdig_orders", &buf)
	if err != nil {
		t.Fatalf("read binary copy: %v", err)
	}
	if rows != want {
		t.Fatalf("binary copy rows = %d, want %d", rows, want)
	}
	return buf.Bytes()
}
