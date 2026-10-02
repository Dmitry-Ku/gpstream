//go:build gp6lab

package integration

import (
	"path/filepath"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
)

// TestPKConstraintRestoredOnLivePG is the F-11/PK regression: the source
// table's PRIMARY KEY constraint (not just its unique index) must be
// restored on the destination. Without a rename the constraint keeps its
// name; the FR-17 pool builds the PK index first and ADD CONSTRAINT adopts
// it (exactly one index, no duplicate). With a map-table rename the
// constraint is restored under the rewritten index name
// (orders -> orders_v2: orders_pkey -> orders_v2_pkey).
func TestPKConstraintRestoredOnLivePG(t *testing.T) {
	t.Run("no rename", func(t *testing.T) {
		src, dst, _, _ := startPGPair(t, 5721, 5722)
		defer func() { src.Close(); dst.Close() }()

		mustExec(t, src, `CREATE TABLE public.orders (
			id integer PRIMARY KEY,
			customer text,
			amount numeric)`)
		mustExec(t, src, `INSERT INTO public.orders VALUES
			(1,'alice',10.50),(2,'bob',20.25),(3,'carol',30.00)`)

		cfg := config.Default()
		cfg.WorkersPerPair = 1
		p := buildPlan(t, cfg, src, dst)
		if len(p.Units) != 1 {
			t.Fatalf("plan has %d units, want 1", len(p.Units))
		}
		j := manifest.NewJournal(dst, filepath.Join(t.TempDir(), "journal.jsonl"))
		mgr := newTwoAgentManager(t, &cfg, src, dst, j, src.DSN(), dst.DSN())
		if err := mgr.Run(t.Context(), p); err != nil {
			t.Fatalf("manager.Run: %v", err)
		}

		// The PK constraint is restored...
		if n := mustCount(t, dst, `
			SELECT count(*) FROM pg_constraint c
			JOIN pg_class tb ON tb.oid = c.conrelid
			WHERE tb.relname = 'orders' AND c.contype = 'p'
			  AND c.conname = 'orders_pkey'`); n != 1 {
			t.Fatalf("PK constraint orders_pkey not restored (got %d)", n)
		}
		// ...by ADOPTING the pool's unique index: exactly one index on the
		// table, and it is the PK's index (no duplicate created by
		// ADD CONSTRAINT).
		if n := mustCount(t, dst, `
			SELECT count(*) FROM pg_index ix
			JOIN pg_class c ON c.oid = ix.indexrelid
			JOIN pg_class tb ON tb.oid = ix.indrelid
			WHERE tb.relname = 'orders'`); n != 1 {
			t.Fatalf("orders has %d indexes, want exactly 1 (the PK index, adopted not duplicated)", n)
		}
		if n := mustCount(t, dst, `
			SELECT count(*) FROM pg_constraint c
			JOIN pg_class ix ON ix.oid = c.conindid
			JOIN pg_index i ON i.indexrelid = c.conindid
			JOIN pg_class tb ON tb.oid = i.indrelid
			WHERE tb.relname = 'orders' AND c.contype = 'p' AND ix.relkind = 'i'`); n != 1 {
			t.Fatalf("PK constraint not backed by the table's unique index (got %d)", n)
		}
	})

	t.Run("map-table rename", func(t *testing.T) {
		src, dst, _, _ := startPGPair(t, 5723, 5724)
		defer func() { src.Close(); dst.Close() }()

		mustExec(t, src, `CREATE TABLE public.orders (
			id integer PRIMARY KEY,
			customer text)`)
		mustExec(t, src, `INSERT INTO public.orders VALUES (1,'alice'),(2,'bob')`)

		cfg := config.Default()
		cfg.WorkersPerPair = 1
		cfg.RenameTable = map[string]string{"public.orders": "public.orders_v2"}
		p := buildPlan(t, cfg, src, dst)
		if len(p.Units) != 1 {
			t.Fatalf("plan has %d units, want 1", len(p.Units))
		}
		j := manifest.NewJournal(dst, filepath.Join(t.TempDir(), "journal.jsonl"))
		mgr := newTwoAgentManager(t, &cfg, src, dst, j, src.DSN(), dst.DSN())
		if err := mgr.Run(t.Context(), p); err != nil {
			t.Fatalf("manager.Run: %v", err)
		}

		// The PK constraint is restored under the REWRITTEN name (matching
		// the rewritten index the pool built)...
		if n := mustCount(t, dst, `
			SELECT count(*) FROM pg_constraint c
			JOIN pg_class tb ON tb.oid = c.conrelid
			WHERE tb.relname = 'orders_v2' AND c.contype = 'p'
			  AND c.conname = 'orders_v2_pkey'`); n != 1 {
			t.Fatalf("PK constraint orders_v2_pkey not restored (got %d)", n)
		}
		// ...and is backed by the single unique index (adopted, not
		// duplicated).
		if n := mustCount(t, dst, `
			SELECT count(*) FROM pg_index ix
			JOIN pg_class c ON c.oid = ix.indexrelid
			JOIN pg_class tb ON tb.oid = ix.indrelid
			WHERE tb.relname = 'orders_v2'`); n != 1 {
			t.Fatalf("orders_v2 has %d indexes, want exactly 1 (the PK index, adopted not duplicated)", n)
		}
	})
}
