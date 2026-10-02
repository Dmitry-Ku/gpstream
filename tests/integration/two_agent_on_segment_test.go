//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// bcopySig is the binary COPY signature (11 bytes).
const bcopySig = "PGCOPY\n\xff\r\n\x00"

// splitBinarySlices splits a full binary-COPY stream (one header + rows +
// one EOF) into n self-contained slice streams, each with its own header and
// EOF — exactly the shape the GPDB kernel produces per segment under
// COPY ... TO PROGRAM ... ON SEGMENT. Rows are dealt round-robin so every
// slice is non-empty (when rows >= n).
func splitBinarySlices(t *testing.T, full []byte, n int) [][]byte {
	t.Helper()
	// Parse the full stream: header, then rows, then the int16 -1 EOF.
	if len(full) < 19 || string(full[:11]) != bcopySig {
		t.Fatalf("splitBinarySlices: bad binary-COPY stream (len %d)", len(full))
	}
	rows := make([][]byte, 0, 1024)
	off := 19 // header: 11 sig + 4 rem + 4 flags
	for off < len(full) {
		if off+2 > len(full) {
			t.Fatal("splitBinarySlices: truncated row header")
		}
		fc := int16(binary.BigEndian.Uint16(full[off : off+2]))
		if fc == -1 {
			off += 2
			break
		}
		if fc < 0 {
			t.Fatalf("splitBinarySlices: bad fieldCount %d", fc)
		}
		rowStart := off
		off += 2
		for j := 0; j < int(fc); j++ {
			ln := int32(binary.BigEndian.Uint32(full[off : off+4]))
			off += 4
			if ln >= 0 {
				off += int(ln)
			}
		}
		rows = append(rows, full[rowStart:off])
	}
	if off != len(full) {
		t.Fatalf("splitBinarySlices: trailing bytes %d (EOF not at the end)", len(full)-off)
	}
	if len(rows) < n {
		t.Fatalf("splitBinarySlices: %d rows for %d slices", len(rows), n)
	}
	// Deal the rows round-robin into n slices.
	buckets := make([][]byte, n)
	for i, r := range rows {
		buckets[i%n] = append(buckets[i%n], r...)
	}
	out := make([][]byte, n)
	for i := range buckets {
		s := []byte(bcopySig)
		s = append(s, 0, 0, 0, 0) // rem = 0
		s = append(s, 0, 0, 0, 0) // flags = 0
		s = append(s, buckets[i]...)
		s = append(s, 0xFF, 0xFF) // int16 -1 EOF
		out[i] = s
	}
	return out
}

// TestTwoAgentOnSegmentServe is the P4.2 acceptance: a REAL destination agent
// (agent.ServeSegments) merges N COMPRESSED segment slices over real TCP into
// one binary-COPY stream and loads a REAL pgtest destination (the SliceMux
// moved from the single-controller manager into the dest agent). The N fake
// segreceivers stream real table bytes (read from a pgtest source) split into
// N slices, compressed with zstd — the exact wire form the kernel-launched
// segreceivers will use.
func TestTwoAgentOnSegmentServe(t *testing.T) {
	const (
		srcPort  = 5616
		dstPort  = 5617
		rows     = 20000
		jobID    = 0x5678
		unitID   = 7
		finalTab = "seg_orders"
		staging  = "_gs_testjob_7"
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
	dstConn, err := pg.NewConn(ctx, dstCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect dest: %v", err)
	}
	defer dstConn.Close()

	// Seed a compressible source table and read its binary-COPY stream.
	mustExec(t, srcConn, `DROP TABLE IF EXISTS seg_orders`)
	mustExec(t, srcConn, `CREATE TABLE seg_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO seg_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinary(t, srcConn)

	// The unit shape, captured from the source (what the controller would
	// hand the dest agent at launch).
	sa := source.New(srcConn)
	cols, err := sa.Columns(ctx, "public", finalTab)
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	dist := sa.Distribution(ctx, "public", finalTab)
	part, _ := pg.CapturePartitionSpec(srcConn, ctx, "public", finalTab) // non-fatal (plain PG)

	// Split the stream into N slices (the kernel's per-segment shape).
	slices := splitBinarySlices(t, full, nSeg)

	// Destination agent: serve the on_segment unit (with the swap).
	listen := freePort(t)
	sresCh := make(chan agent.SegmentsResult, 1)
	serrCh := make(chan error, 1)
	go func() {
		res, err := agent.ServeSegments(ctx, agent.SegmentsOptions{
			Listen:   listen,
			DestConn: dstCl.URL(),
			Spec: agent.UnitSpec{
				JobID: jobID, UnitID: unitID,
				Schema: "public", Table: finalTab, Staging: staging, Overwrite: true,
				Compress: "zstd",
				Cols:     cols, Dist: dist, Part: part,
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

	// N fake segreceivers: each streams its slice COMPRESSED over real TCP.
	time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
	errCh := make(chan error, nSeg)
	var logical uint64
	for i := 0; i < nSeg; i++ {
		go func(i int) {
			conn, err := net.Dial("tcp", listen)
			if err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial: %w", i, err)
				return
			}
			if _, err := segstream.SendCompressed(ctx, conn, bytes.NewReader(slices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3); err != nil {
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
	logical = uint64(len(full))

	var sres agent.SegmentsResult
	select {
	case serr := <-serrCh:
		t.Fatalf("dest agent serve segments: %v", serr)
	case sres = <-sresCh:
		t.Logf("serve segments: rows=%d trailer=%d wire=%d logical=%d swapped=%v",
			sres.Rows, sres.TrailerRows, sres.Wire, sres.Logical, sres.Swapped)
		// 1. The destination wrote exactly the seeded row count (the
		// authoritative count), matching the trailer cross-check.
		if sres.Rows != rows || sres.TrailerRows != rows {
			t.Fatalf("row count mismatch: dst=%d trailer=%d want=%d", sres.Rows, sres.TrailerRows, rows)
		}
		// 2. The swap landed (staging renamed to the final name).
		if !sres.Swapped {
			t.Fatalf("dest agent did not swap")
		}
		// 3. ADR-002 acceptance: the network carried the COMPRESSED volume
		// (the raw bytes the agent read from the N segment connections).
		if sres.Wire >= logical {
			t.Fatalf("on-wire %d >= logical %d: not compressed on the network", sres.Wire, logical)
		}
	}

	// 4. The final table has the source's content (order-independent digest).
	if got := mustCount(t, dstConn, `SELECT count(*) FROM public.seg_orders`); got != rows {
		t.Fatalf("dest seg_orders count = %d, want %d", got, rows)
	}
	wantDigest := mustText(t, srcConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM seg_orders r`)
	if got := mustText(t, dstConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM public.seg_orders r`); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q", got, wantDigest)
	}
	// 5. The staging table is gone.
	if got := mustCount(t, dstConn,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE c.relname = '_gs_testjob_7' AND n.nspname = 'public'`); got != 0 {
		t.Fatalf("staging %s still present after swap", staging)
	}
	t.Logf("P4.2 ok: on_segment serve, rows=%d wire=%d logical=%d ratio=%.1fx",
		rows, sres.Wire, uint64(len(full)), float64(len(full))/float64(sres.Wire))
}

// readCopyBinary reads a binary COPY stream fully from the source (the same
// path the production source agent uses: CopyToStream into a buffer).
func readCopyBinary(t *testing.T, q *pg.Conn) []byte {
	t.Helper()
	var buf bytes.Buffer
	rows, err := source.New(q).CopyToStream(context.Background(), "public", "seg_orders", &buf)
	if err != nil {
		t.Fatalf("read binary copy: %v", err)
	}
	if rows != 20000 {
		t.Fatalf("binary copy rows = %d, want 20000", rows)
	}
	return buf.Bytes()
}
