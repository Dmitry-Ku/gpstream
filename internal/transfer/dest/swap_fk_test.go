package dest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestSwapOverwriteDropsDependentFKs guards the overwrite DROP against a
// destination table that is referenced by foreign keys on OTHER destination
// tables (live GP6, 01.10 T30 run 2 B->C: a re-run over a dest seeded with an
// FK graph — t_s09 -> t_s01, t_s01 -> t_s01 self-FK — made the overwrite DROP
// fail with 2BP01 "other objects depend on it", non-deterministically, because
// with parallel workers the referenced table can be swapped before its
// referrers). The fix drops the dependent FK constraints (surgically, not
// blanket CASCADE) before the DROP; restoreConstraints re-applies the source
// FK set after the data path, so it is lossless (F-11).
func TestSwapOverwriteDropsDependentFKs(t *testing.T) {
	f := pg.NewFakeConn()
	// The FK-catalog query (dropDependentFKs) returns two dependent
	// constraints: a cross-table FK (t_s09 -> t_s01) and a self-FK (t_s01 ->
	// t_s01). Both must be dropped before the table itself.
	f.On("pg_constraint", [][]any{
		{"public", "t_s01", "t_s01_fk_parent"}, // self-FK
		{"public", "t_s09", "t_s09_fk_parent"}, // cross-table FK
	})
	a := mkAgent(f)
	if err := a.Swap(context.Background(), "public", "t_s01", "_gs_j1_5", true); err != nil {
		t.Fatalf("swap overwrite with dependent FKs: %v", err)
	}
	q := f.Queried()
	selfFK := `ALTER TABLE "public"."t_s01" DROP CONSTRAINT "t_s01_fk_parent"`
	crossFK := `ALTER TABLE "public"."t_s09" DROP CONSTRAINT "t_s09_fk_parent"`
	drop := `DROP TABLE IF EXISTS "public"."t_s01"`
	rename := `RENAME TO "t_s01"`

	idx := map[string]int{}
	for i, s := range q {
		if strings.Contains(s, selfFK) {
			idx["selfFK"] = i
		}
		if strings.Contains(s, crossFK) {
			idx["crossFK"] = i
		}
		if strings.Contains(s, drop) {
			idx["drop"] = i
		}
		if strings.Contains(s, rename) {
			idx["rename"] = i
		}
	}
	for _, k := range []string{"selfFK", "crossFK", "drop", "rename"} {
		if _, ok := idx[k]; !ok {
			t.Fatalf("expected statement %q in query log: %v", k, q)
		}
	}
	// Ordering: both FK drops must precede the table drop, and the drop the
	// rename.
	if idx["selfFK"] >= idx["drop"] || idx["crossFK"] >= idx["drop"] {
		t.Fatalf("dependent FK drops must precede DROP TABLE; log: %v", q)
	}
	if idx["drop"] >= idx["rename"] {
		t.Fatalf("DROP TABLE must precede RENAME; log: %v", q)
	}
}

// TestSwapOverwriteNoDependentFKs confirms the fast path: no dependent FKs
// means no ALTER ... DROP CONSTRAINT statements, only DROP + RENAME.
func TestSwapOverwriteNoDependentFKs(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_constraint", [][]any{}) // no dependent FKs
	a := mkAgent(f)
	if err := a.Swap(context.Background(), "public", "t_s01", "_gs_j1_5", true); err != nil {
		t.Fatalf("swap overwrite: %v", err)
	}
	for _, s := range f.Queried() {
		if strings.Contains(s, "DROP CONSTRAINT") {
			t.Fatalf("unexpected DROP CONSTRAINT with no dependent FKs: %v", f.Queried())
		}
	}
}
