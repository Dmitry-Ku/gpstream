//go:build gp6lab

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestTwoAgentRoundTripVerifyDigest is the P9-E (FR-02) end-to-end
// acceptance: a real two-agent coordinator round trip (pgtest source ->
// source agent Push -> zstd + framing over TCP -> destination agent Serve ->
// pgtest destination) with --verify-style inline digests ON. It proves the
// release behavior:
//   - the source computes the row-order-independent content digest ON THE
//     DATA PATH (O(1) memory, as the rows stream) and reports it,
//   - the destination does the same as the rows flow into the COPY FROM,
//   - the two inline digests AGREE (so the manager can verify by comparing
//     them, re-reading neither table), and
//   - both equal a reference digest re-derived directly from the databases
//     (the same digest the legacy re-read verify path computes).
func TestTwoAgentRoundTripVerifyDigest(t *testing.T) {
	const (
		srcPort  = 5616
		dstPort  = 5617
		rows     = 30000
		jobID    = 0x5678
		unitID   = 7
		finalTab = "orders"
		staging  = "_gs_p9e_7"
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

	mustExec(t, srcConn, `DROP TABLE IF EXISTS orders`)
	mustExec(t, srcConn, `CREATE TABLE orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	if n := mustCount(t, srcConn, `SELECT count(*) FROM orders`); n != rows {
		t.Fatalf("orders seeded %d, want %d", n, rows)
	}

	// Destination agent: serve one unit on a free port, with the swap.
	listen := freePort(t)
	dresCh := make(chan agent.DestResult, 1)
	derrCh := make(chan error, 1)
	go func() {
		res, err := agent.Serve(ctx, agent.DestOptions{
			Listen:        listen,
			DestConn:      dstCl.URL(),
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

	// Source agent: push the table with the inline verify digest ON.
	time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
	sres, serr := agent.Push(ctx, agent.SourceOptions{
		SrcConn:      srcCl.URL(),
		Schema:       "public",
		Table:        finalTab,
		DstSchema:    "public",
		DstTable:     finalTab,
		Staging:      staging,
		JobID:        jobID,
		UnitID:       unitID,
		DestAddr:     listen,
		Compress:     "zstd",
		Snapshot:     agent.SnapDB,
		VerifyDigest: true,
	})
	if serr != nil {
		t.Fatalf("source agent push: %v", serr)
	}
	if sres.Digest == "" {
		t.Fatalf("source agent did not report an inline digest (VerifyDigest on)")
	}
	t.Logf("push: rows=%d wire=%d logical=%d digest=%s", sres.Rows, sres.Wire, sres.Logical, sres.Digest)

	// 1+2. The two inline digests agree with each other and with a reference
	//    digest re-derived directly from the databases (the same value the
	//    legacy re-read verify path computes).
	var dstInline string
	select {
	case derr := <-derrCh:
		t.Fatalf("dest agent serve: %v", derr)
	case dres := <-dresCh:
		if !dres.Swapped {
			t.Fatalf("dest agent did not swap")
		}
		if dres.Digest == "" {
			t.Fatalf("dest agent did not report an inline digest (spec carried VerifyDigest)")
		}
		t.Logf("serve: rows=%d logical=%d digest=%s", dres.Rows, dres.Logical, dres.Digest)
		// 1. The two inline digests agree (the manager's verify compare).
		if sres.Digest != dres.Digest {
			t.Fatalf("inline digest mismatch: source=%q dest=%q", sres.Digest, dres.Digest)
		}
		dstInline = dres.Digest
	}

	// 2. Both inline digests equal a reference digest re-derived DIRECTLY
	//    from the databases (the same value the legacy re-read verify path
	//    computes), on the source and on the (swapped) destination.
	srcRef := p9eReferenceDigest(t, srcConn, "public.orders")
	if sres.Digest != srcRef {
		t.Fatalf("source inline digest %q != source reference %q", sres.Digest, srcRef)
	}
	dstRef := p9eReferenceDigest(t, dstConn, "public.orders")
	if dstInline != dstRef {
		t.Fatalf("dest inline digest %q != dest reference %q", dstInline, dstRef)
	}
	t.Logf("P9-E ok: inline digests agree and match the reference re-read: %s (rows=%d)", sres.Digest, rows)
}

// p9eReferenceDigest re-derives the row-order-independent content digest
// directly from the table (a streaming O(1)-memory re-read, the same digest
// the legacy verify path computes).
func p9eReferenceDigest(t *testing.T, c *pg.Conn, table string) string {
	t.Helper()
	dw := transfer.NewContentDigest()
	if _, err := c.ReadCopy(context.Background(), dw, `COPY (SELECT * FROM `+table+`) TO STDOUT (FORMAT binary)`); err != nil {
		t.Fatalf("reference ReadCopy %s: %v", table, err)
	}
	d, err := dw.Sum()
	if err != nil {
		t.Fatalf("reference digest %s: %v", table, err)
	}
	return d
}
