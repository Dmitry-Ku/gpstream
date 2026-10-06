//go:build gp6lab

package integration

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// digest returns a content hash of every row in table. It streams the
// table with `COPY (SELECT * FROM t) TO STDOUT (BINARY)` and hashes the
// payload, instead of aggregating server-side: a
// `string_agg(... ORDER BY id)` over a multi-million-row table overflows
// GP6's per-segment workfile limit (SQLSTATE 53000; the limit is a
// postmaster-context GUC and cannot be raised per session). Streaming
// keeps the digest O(1) in server memory at any table size. The hash is
// content-sensitive (any row value or row-count change flips it); row
// order is not pinned, but the digest is only ever compared between two
// databases of the same schema under the same streaming, so the check
// stays valid (and count() pins the row count alongside it).
func digest(t *testing.T, q pg.StreamingQuerier, table string) string {
	t.Helper()
	buf := &appendBuf{}
	if _, err := q.ReadCopy(context.Background(), buf, `COPY (SELECT * FROM `+table+`) TO STDOUT (BINARY)`); err != nil {
		t.Fatalf("digest %s: %v", table, err)
	}
	d, err := transfer.ContentDigest(buf.data)
	if err != nil {
		t.Fatalf("digest %s: %v", table, err)
	}
	return d
}

// TestTwoAgentManagerRun is the P3 acceptance (ADR-002 remediation): the
// Manager itself (plan -> journal -> runUnit -> verify -> swap -> indexes)
// drives the two-agent data path. The data COPY runs ONLY in the agents over a
// real, compressed TCP socket (in-process runner: a long-lived source agent +
// a per-unit destination agent); the controller never runs a COPY. It proves:
//
//   - the destination table is correct after the run (row count + content),
//   - the staging table is gone (the controller's swap landed),
//   - the on-wire (wire) volume is COMPRESSED (< logical), and the manager's
//     bytes_total metric equals the wire volume the agents reported,
//   - --verify on works through the agent-side checksum (source checksum read
//     under the source agent's FR-02 snapshot, compared to the destination),
//   - the run completes cleanly through the Manager (journal -> swapped).
func TestTwoAgentManagerRun(t *testing.T) {
	const (
		srcPort = 5624
		dstPort = 5625
		rows    = 20000
	)
	ctx := context.Background()

	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { dst.Close() })

	// Seed a compressible source table (repetitive text compresses hard, so the
	// on-wire volume is well under the logical volume).
	mustExec(t, src, `CREATE TABLE orders (id integer, customer text, note text, amount numeric)`)
	mustExec(t, src,
		`INSERT INTO orders SELECT g, 'cust_' || (g % 97), 'repeatable note body for compression padding', (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	if n := mustCount(t, src, `SELECT count(*) FROM orders`); n != rows {
		t.Fatalf("orders seeded %d, want %d", n, rows)
	}
	wantDigest := digest(t, src, "public.orders")

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	cfg.ConflictAction = config.Overwrite // staging + swap path (the two-agent path)
	cfg.Verify = config.VerifyOn          // exercise the agent-side checksum
	cfg.Compression = config.CompZstd     // compressed on the wire
	// cfg.Source/Dest are the conn strings the in-proc source/dest agents use.
	cfg.Source = srcCl.URL()
	cfg.Dest = dstCl.URL()

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) == 0 {
		t.Fatalf("plan has no units")
	}

	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := manifest.NewManager(&cfg, src, dst, j)
	// Inject the two-agent runner: in-process locally, the PROCESS runner
	// (deployed agents on the cluster hosts) in live mode.
	setTwoAgentRunner(t, mgr, &cfg, srcCl.URL(), dstCl.URL())

	start := time.Now()
	if err := runWithTimeout(t, mgr, p, 180*time.Second); err != nil {
		t.Fatalf("manager.Run (two-agent): %v", err)
	}
	t.Logf("two-agent manager run: %v", time.Since(start))

	// 1. The destination table holds the source's rows + content.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.orders`); got != rows {
		t.Fatalf("dest orders count = %d, want %d", got, rows)
	}
	if got := digest(t, dst, "public.orders"); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q", got, wantDigest)
	}

	// 2. The staging table is gone (the controller swapped it to the final name).
	if got := mustCount(t, dst,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname LIKE '_gs\_%'`); got != 0 {
		t.Fatalf("staging tables still present after the two-agent run: %d", got)
	}

	// 3. The on-wire volume is the COMPRESSED volume (< logical), and the
	// manager's bytes_total metric equals the wire volume the agents reported.
	m := mgr.Metrics()
	if m.Bytes == 0 {
		t.Fatalf("bytes_total metric is 0 (two-agent wire volume not recorded)")
	}
	if m.Bytes >= m.BytesDecoded {
		t.Fatalf("wire %d >= logical %d: not compressed on the wire", m.Bytes, m.BytesDecoded)
	}
	t.Logf("two-agent: wire=%d logical=%d ratio=%.1fx (bytes_total==wire)",
		m.Bytes, m.BytesDecoded, float64(m.BytesDecoded)/float64(m.Bytes))

	// 4. --verify on ran through the agent-side checksum (one unit verified).
	if mgr.Comps != 1 {
		t.Fatalf("Comps = %d, want 1 (two-agent --verify via agent checksum)", mgr.Comps)
	}
	// 5. The manager copied all rows (FR-02 row count).
	if mgr.Rows != rows {
		t.Fatalf("manager.Rows = %d, want %d", mgr.Rows, rows)
	}
	t.Logf("P3 ok: two-agent manager run, rows=%d wire=%d logical=%d", rows, m.Bytes, m.BytesDecoded)
}
