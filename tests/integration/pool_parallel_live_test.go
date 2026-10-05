//go:build gp6lab

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// pool_parallel_live_test.go uses the live cluster URLs (the pgtest override
// gives each pool its live coordinator DSN). The pools are built from the
// cluster URLs below (srcCl.URL()/dstCl.URL()), so they work both locally and
// live — a hardcoded 127.0.0.1:5611 DSN would break the live layout.
func TestPoolParallelWorkersLive(t *testing.T) {
	ctx := context.Background()

	srcCl, src := startPG(t, 5611, "srcdb")
	defer src.Close()
	dstCl, dst := startPG(t, 5612, "dstdb")
	defer dst.Close()

	// Three tables of very different sizes so that, under 4 workers, at least
	// two COPYs are in flight simultaneously (the "conn busy" precondition).
	const ordersRows, custRows, invRows = 5000, 3000, 20000
	mustExec(t, src, "DROP TABLE IF EXISTS orders, customers, invoices")
	mustExec(t, src, `CREATE TABLE orders (id integer, customer text, amount numeric)`)
	mustExec(t, src, fmt.Sprintf(
		`INSERT INTO orders SELECT g, 'cust_' || (g %% 97), (g %% 100) * 10.5 FROM generate_series(1, %d) g`, ordersRows))
	mustExec(t, src, `CREATE TABLE customers (id integer, name text, email text)`)
	mustExec(t, src, fmt.Sprintf(
		`INSERT INTO customers SELECT g, 'c_' || g, 'u' || g || '@ex.com' FROM generate_series(1, %d) g`, custRows))
	mustExec(t, src, `CREATE TABLE invoices (id integer, order_id integer, total numeric)`)
	mustExec(t, src, fmt.Sprintf(
		`INSERT INTO invoices SELECT g, (g %% %d) + 1, (g %% 500) * 0.75 FROM generate_series(1, %d) g`, ordersRows, invRows))
	if n := mustCount(t, src, `SELECT count(*) FROM orders`); n != ordersRows {
		t.Fatalf("orders seeded %d, want %d", n, ordersRows)
	}

	// Plan against the real clusters, selecting the three tables explicitly.
	// The DB segment is the live cluster's actual database name (srcdb_<port>
	// in live mode); matchTable treats it as a glob that must match exactly.
	cfg := config.Default()
	cfg.Tables = []string{
		srcCl.DB + ".public.orders",
		srcCl.DB + ".public.customers",
		srcCl.DB + ".public.invoices",
	}
	cfg.ConflictAction = config.Overwrite // staging + swap path (E1 layout)
	cfg.WorkersPerPair = 4                // parallel stream workers (FR-03)
	p, err := plan.Build(ctx, &cfg, src, dst)
	if err != nil {
		t.Fatalf("plan.Build: %v", err)
	}
	if len(p.Units) != 3 {
		t.Fatalf("plan has %d units, want 3 (got %+v)", len(p.Units), p.Units)
	}

	// Run the Manager over POOLS (the E1 path). This is what used to fail with
	// "read schema: conn busy" / "source copy: conn busy". The pools use the
	// cluster URLs (live DSNs in live mode).
	srcPool, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 8)
	if err != nil {
		t.Fatalf("src pool: %v", err)
	}
	defer srcPool.Close()
	dstPool, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 8)
	if err != nil {
		t.Fatalf("dst pool: %v", err)
	}
	defer dstPool.Close()

	j := manifest.NewJournal(dstPool, t.TempDir()+"/journal.jsonl")
	mgr := newTwoAgentManager(t, &cfg, srcPool, dstPool, j,
		srcCl.URL(), dstCl.URL())
	start := time.Now()
	if err := mgr.Run(ctx, p); err != nil {
		t.Fatalf("manager.Run (pool, 4 workers): %v", err)
	}
	t.Logf("pool run completed in %v (%d rows, %d units)", time.Since(start), mgr.Rows, mgr.Units)

	// Counts must match the source exactly (parallel workers, pool).
	if n := mustCount(t, dst, `SELECT count(*) FROM orders`); n != ordersRows {
		t.Fatalf("dest orders = %d, want %d", n, ordersRows)
	}
	if n := mustCount(t, dst, `SELECT count(*) FROM customers`); n != custRows {
		t.Fatalf("dest customers = %d, want %d", n, custRows)
	}
	if n := mustCount(t, dst, `SELECT count(*) FROM invoices`); n != invRows {
		t.Fatalf("dest invoices = %d, want %d", n, invRows)
	}
	// Content spot-checks (bytes survived the framed, compressed pipe intact).
	if v := mustText(t, dst, `SELECT customer FROM orders WHERE id=1`); v != "cust_1" {
		t.Fatalf("orders id=1 customer=%q, want cust_1", v)
	}
	if v := mustText(t, dst, `SELECT email FROM customers WHERE id=2`); v != "u2@ex.com" {
		t.Fatalf("customers id=2 email=%q, want u2@ex.com", v)
	}
	// The manager must report real work and every unit must reach the terminal
	// swapped state (a "conn busy" would have left units failed).
	if mgr.Rows != int64(ordersRows+custRows+invRows) {
		t.Fatalf("manager reported %d rows, want %d", mgr.Rows, ordersRows+custRows+invRows)
	}
	for id := int64(1); id <= 3; id++ {
		u := j.Get(id)
		if u == nil {
			t.Fatalf("journal missing unit %d", id)
		}
		if u.State != manifest.StateSwapped {
			t.Fatalf("unit %d state=%s, want swapped", id, u.State)
		}
	}
}
