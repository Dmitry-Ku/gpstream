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
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// TestTwoAgentDirect1to1Serve is the P8 (M4/T4b) acceptance for the direct_1to1
// fast-path WRITE side: the destination agent loads N content-tagged segment
// slices with N per-segment utility-mode COPYs (agent.ServeDirect) instead of
// merging them into one coordinator COPY (agent.ServeSegments) — no SliceMux,
// no destination re-distribution. This is the exact wire form the on_segment
// READ side (kernel ON SEGMENT dispatch, unchanged) streams to the destination
// when the plan proves the route is direct_1to1 (S==D + equal hash keys + same
// major).
//
// The READ side (ON SEGMENT kernel dispatch) is Greenplum-only, so it is
// validated live on GP6 (e2e); here, on a plain-PostgreSQL destination, we
// validate the WRITE side end-to-end: N fake segreceivers stream real table
// bytes (read from a pgtest source) split into N content-tagged COMPRESSED
// slices, and a REAL destination agent (ServeDirect) opens D per-segment COPYs.
// On plain PG every DstDSN points at the same coordinator, so the D COPYs union
// into one table — the per-segment routing is what ServeDirect exercises
// (content-id -> the matching DstDSN).
func TestTwoAgentDirect1to1Serve(t *testing.T) {
	const (
		srcPort  = 5618
		dstPort  = 5619
		rows     = 20000
		jobID    = 0x89ab
		unitID   = 11
		finalTab = "d1to1_orders"
		staging  = "_gs_d1to1_11"
		nSeg     = 4
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "d1to1_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	dstCl, err := pgtest.Start(dstPort, "d1to1_dst")
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
	mustExec(t, srcConn, `DROP TABLE IF EXISTS d1to1_orders`)
	mustExec(t, srcConn, `CREATE TABLE d1to1_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO d1to1_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, finalTab, rows)

	// The unit shape, captured from the source (what the controller would
	// hand the dest agent at launch).
	sa := source.New(srcConn)
	cols, err := sa.Columns(ctx, "public", finalTab)
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	dist := sa.Distribution(ctx, "public", finalTab)
	part, _ := pg.CapturePartitionSpec(srcConn, ctx, "public", finalTab) // non-fatal (plain PG)

	// Split the stream into N slices (the kernel's per-segment shape). On
	// direct_1to1, source segment s's slice == destination segment s's slice,
	// so the destination routes each slice to segment s by content-id.
	slices := splitBinarySlices(t, full, nSeg)

	// Destination agent: serve the direct_1to1 unit (per-segment COPYs, swap).
	listen := freePort(t)
	dresCh := make(chan agent.DirectResult, 1)
	derrCh := make(chan error, 1)
	// On plain PG every per-segment DSN is the coordinator; on GP each would
	// be a utility-mode segment DSN. D = the source's primary-segment count.
	dsn := dstCl.URL()
	dstDSNs := make([]string, nSeg)
	for i := range dstDSNs {
		dstDSNs[i] = dsn
	}
	go func() {
		res, err := agent.ServeDirect(ctx, agent.DirectOptions{
			Listen:   listen,
			DestConn: dsn,
			Launch: agent.DirectLaunch{
				UnitID:    uint32(unitID),
				Schema:    "public",
				Table:     finalTab,
				Staging:   staging,
				Overwrite: true,
				Compress:  "zstd",
				Cols:      cols,
				Dist:      dist,
				Part:      part,
				D:         int32(nSeg),
				DstDSNs:   dstDSNs,
			},
			Swap:          true,
			AcceptTimeout: 30 * time.Second,
			StreamTimeout: 60 * time.Second,
		})
		if err != nil {
			derrCh <- err
			return
		}
		dresCh <- res
	}()

	// N fake segreceivers: each streams its slice COMPRESSED over real TCP,
	// content-tagged with its segment id (the wire form of ON SEGMENT dispatch).
	time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
	errCh := make(chan error, nSeg)
	for i := 0; i < nSeg; i++ {
		go func(i int) {
			conn, err := dialRetry(listen, 90*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial: %w", i, err)
				return
			}
			if _, err := segstream.SendCompressed(ctx, conn, bytes.NewReader(slices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3, false); err != nil {
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
	logical := uint64(len(full))

	var dres agent.DirectResult
	select {
	case serr := <-derrCh:
		t.Fatalf("dest agent serve direct: %v", serr)
	case dres = <-dresCh:
		t.Logf("serve direct: rows=%d trailer=%d wire=%d logical=%d perSeg=%v swapped=%v",
			dres.Rows, dres.TrailerRows, dres.Wire, dres.Logical, dres.PerSeg, dres.Swapped)
		// 1. The destination wrote exactly the seeded row count (authoritative),
		//    matching the trailer cross-check.
		if dres.Rows != rows || dres.TrailerRows != rows {
			t.Fatalf("row count mismatch: dst=%d trailer=%d want=%d", dres.Rows, dres.TrailerRows, rows)
		}
		// 2. The swap landed (staging renamed to the final name).
		if !dres.Swapped {
			t.Fatalf("dest agent did not swap")
		}
		// 3. The per-segment distribution covers every row exactly once (the
		//    N slices routed to N segments without loss or duplication).
		var perSegSum int64
		for _, c := range dres.PerSeg {
			perSegSum += c
		}
		if perSegSum != rows {
			t.Fatalf("per-segment sum %d != rows %d (perSeg=%v)", perSegSum, rows, dres.PerSeg)
		}
		if len(dres.PerSeg) != nSeg {
			t.Fatalf("per-segment count %d, want %d", len(dres.PerSeg), nSeg)
		}
		// 4. ADR-002 acceptance: the network carried the COMPRESSED volume.
		if dres.Wire >= logical {
			t.Fatalf("on-wire %d >= logical %d: not compressed on the network", dres.Wire, logical)
		}
	}

	// 5. The final table has the source's content (order-independent digest).
	if got := mustCount(t, dstConn, `SELECT count(*) FROM public.d1to1_orders`); got != rows {
		t.Fatalf("dest d1to1_orders count = %d, want %d", got, rows)
	}
	wantDigest := mustText(t, srcConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM d1to1_orders r`)
	if got := mustText(t, dstConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM public.d1to1_orders r`); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q", got, wantDigest)
	}
	// 6. The staging table is gone.
	if got := mustCount(t, dstConn,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE c.relname = '_gs_d1to1_11' AND n.nspname = 'public'`); got != 0 {
		t.Fatalf("staging %s still present after swap", staging)
	}
	t.Logf("P8 ok: direct_1to1 write side, rows=%d wire=%d logical=%d ratio=%.1fx perSeg=%v",
		rows, dres.Wire, logical, float64(logical)/float64(dres.Wire), dres.PerSeg)
}

// readCopyBinaryTable reads a binary COPY stream fully from the source for the
// named table (the same path the production source agent uses: CopyToStream
// into a buffer). A table-parameterised copy of the on_segment test's
// readCopyBinary, which is bound to its own table.
func readCopyBinaryTable(t *testing.T, q *pg.Conn, table string, wantRows int) []byte {
	t.Helper()
	var buf bytes.Buffer
	n, err := source.New(q).CopyToStream(context.Background(), "public", table, &buf)
	if err != nil {
		t.Fatalf("read binary copy: %v", err)
	}
	if n != int64(wantRows) {
		t.Fatalf("binary copy rows = %d, want %d", n, wantRows)
	}
	return buf.Bytes()
}
