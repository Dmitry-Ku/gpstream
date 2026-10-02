//go:build integration

package integration

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// freePort binds a throwaway listener to learn a free loopback port, then
// releases it (the destination agent re-binds it; the tiny race window is
// acceptable for a test).
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// TestTwoAgentRoundTrip is the P2 acceptance: a REAL two-agent coordinator
// round trip over a real TCP socket — a pgtest source, the source agent
// (agent.Push: COPY TO STDOUT + compress + frame over the wire), and the
// destination agent (agent.Serve: decompress + COPY FROM + atomic swap) into a
// pgtest destination. It proves the ADR-002 debt is closed end to end:
//   - the destination table is correct (row count + content) after the swap,
//   - the staging table is gone (the rename landed),
//   - the source's reported on-wire volume is the COMPRESSED volume (<
//     logical), and
//   - the destination's reported row count matches the source.
func TestTwoAgentRoundTrip(t *testing.T) {
	const (
		srcPort  = 5614
		dstPort  = 5615
		rows     = 30000
		jobID    = 0x1234
		unitID   = 3
		finalTab = "orders"
		staging  = "_gs_testjob_3"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Two real clusters.
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

	// Seed a compressible source table.
	mustExec(t, srcConn, `DROP TABLE IF EXISTS orders`)
	mustExec(t, srcConn, `CREATE TABLE orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	if n := mustCount(t, srcConn, `SELECT count(*) FROM orders`); n != rows {
		t.Fatalf("orders seeded %d, want %d", n, rows)
	}
	// Reference content digest (order-independent) for the identity check.
	wantDigest := mustText(t, srcConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM orders r`)

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

	// Source agent: push the table to the destination agent over real TCP.
	time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
	start := time.Now()
	sres, serr := agent.Push(ctx, agent.SourceOptions{
		SrcConn:   srcCl.URL(),
		Schema:    "public",
		Table:     finalTab,
		DstSchema: "public",
		DstTable:  finalTab,
		Staging:   staging,
		JobID:     jobID,
		UnitID:    unitID,
		DestAddr:  listen,
		Compress:  "zstd",
		Snapshot:  agent.SnapDB,
	})
	if serr != nil {
		t.Fatalf("source agent push: %v", serr)
	}
	t.Logf("push: rows=%d wire=%d logical=%d (%v)", sres.Rows, sres.Wire, sres.Logical, time.Since(start))

	select {
	case derr := <-derrCh:
		t.Fatalf("dest agent serve: %v", derr)
	case dres := <-dresCh:
		t.Logf("serve: rows=%d logical=%d swapped=%v", dres.Rows, dres.Logical, dres.Swapped)
		if !dres.Swapped {
			t.Fatalf("dest agent did not swap")
		}
		// 1. The destination wrote the same row count the source read.
		if dres.Rows != sres.Rows || sres.Rows != rows {
			t.Fatalf("row count mismatch: src=%d dst=%d want=%d", sres.Rows, dres.Rows, rows)
		}
		// 2. The destination's logical volume equals the source's.
		if dres.Logical != sres.Logical {
			t.Fatalf("logical mismatch: src=%d dst=%d", sres.Logical, dres.Logical)
		}
	}

	// 3. The final table exists on the destination with the source's content.
	if got := mustCount(t, dstConn, `SELECT count(*) FROM public.orders`); got != rows {
		t.Fatalf("dest orders count = %d, want %d", got, rows)
	}
	if got := mustText(t, dstConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM public.orders r`); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q", got, wantDigest)
	}
	// 4. The staging table is gone (the rename landed).
	if got := mustCount(t, dstConn,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE c.relname = '_gs_testjob_3' AND n.nspname = 'public'`); got != 0 {
		t.Fatalf("staging %s still present after swap", staging)
	}
	// 5. The on-wire volume is the compressed volume (< logical).
	if sres.Wire >= sres.Logical {
		t.Fatalf("wire %d >= logical %d: not compressed on the wire", sres.Wire, sres.Logical)
	}
	t.Logf("P2 ok: two-agent round trip, rows=%d wire=%d logical=%d ratio=%.1fx",
		rows, sres.Wire, sres.Logical, float64(sres.Logical)/float64(sres.Wire))
}
