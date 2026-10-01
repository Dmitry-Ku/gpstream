//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/plan"
)

// TestIndexRenameCollisionOnLivePG is the F-17/F-11 regression for the
// wrong-table REINDEX bug: the destination schema already contains a table
// (the source table's name, from a prior full copy) whose indexes carry the
// same names (orders_pkey, orders_c_idx). A map-table copy of orders ->
// orders_v2 must build orders_v2's indexes under rewritten names
// (orders_v2_pkey, orders_v2_c_idx) instead of silently REINDEXing the
// foreign table's indexes and leaving orders_v2 index-less.
func TestIndexRenameCollisionOnLivePG(t *testing.T) {
	src := startPG(t, 5621, "srcdb")
	defer src.Close()
	dst := startPG(t, 5622, "dstdb")
	defer dst.Close()

	ctx := context.Background()

	// Source: orders with a PK and a secondary index.
	mustExec(t, src, `CREATE TABLE public.orders (
		id integer PRIMARY KEY,
		customer text,
		amount numeric)`)
	mustExec(t, src, `CREATE INDEX orders_c_idx ON public.orders (customer)`)
	mustExec(t, src, `INSERT INTO public.orders VALUES
		(1,'alice',10.50),(2,'bob',20.25),(3,'carol',30.00)`)

	// Destination: a prior "full copy" of orders — same table name, same
	// index names. This is what makes the name-only existence check match
	// the wrong table.
	mustExec(t, dst, `CREATE TABLE public.orders (
		id integer PRIMARY KEY,
		customer text,
		amount numeric)`)
	mustExec(t, dst, `CREATE INDEX orders_c_idx ON public.orders (customer)`)

	// Copy orders -> orders_v2 (map-table) with the data + index path.
	cfg := config.Default()
	cfg.Source = "src"
	cfg.Dest = "dst"
	cfg.RenameTable = map[string]string{"public.orders": "public.orders_v2"}
	p, err := plan.Build(ctx, &cfg, src, dst)
	if err != nil {
		t.Fatalf("plan.Build: %v", err)
	}
	if len(p.Units) != 1 {
		t.Fatalf("plan has %d units, want 1", len(p.Units))
	}
	if p.Units[0].DstName != "orders_v2" {
		t.Fatalf("unit DstName = %q, want orders_v2", p.Units[0].DstName)
	}

	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")
	mgr := manifest.NewManager(&cfg, src, dst, j)
	if err := mgr.Run(ctx, p); err != nil {
		t.Fatalf("manager.Run: %v", err)
	}

	// The renamed table must have BOTH indexes, under rewritten names.
	rows, err := dst.Query(ctx, `
		SELECT c.relname FROM pg_index ix
		JOIN pg_class c ON c.oid = ix.indexrelid
		JOIN pg_class tb ON tb.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE tb.relname = 'orders_v2' AND n.nspname = 'public'
		ORDER BY c.relname`)
	if err != nil {
		t.Fatalf("query indexes: %v", err)
	}
	var got []string
	for _, r := range rows {
		if len(r) > 0 {
			if s, ok := r[0].(string); ok {
				got = append(got, s)
			}
		}
	}
	if len(got) != 2 {
		t.Fatalf("orders_v2 has %d indexes (%v), want 2 — the index build must not REINDEX the foreign table's indexes", len(got), got)
	}
	for _, want := range []string{"orders_v2_c_idx", "orders_v2_pkey"} {
		found := false
		for _, g := range got {
			if g == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("orders_v2 missing %q (got %v)", want, got)
		}
	}

	// The prior full copy's indexes must be intact (the build must not have
	// touched them).
	if n := mustCount(t, dst, `
		SELECT count(*) FROM pg_index ix
		JOIN pg_class c ON c.oid = ix.indexrelid
		JOIN pg_class tb ON tb.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE tb.relname = 'orders' AND n.nspname = 'public'`); n != 2 {
		t.Fatalf("prior copy orders has %d indexes, want 2 (untouched)", n)
	}

	// Data must be present in the renamed table.
	if n := mustCount(t, dst, `SELECT count(*) FROM public.orders_v2`); n != 3 {
		t.Fatalf("orders_v2 has %d rows, want 3", n)
	}
}
