package plan

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// columnFakes returns src/dst fakes whose pg_database rows are identical (no
// DB-level diff) and whose gs-collation-cols answers carry the given explicit
// column collations ("" = database default). Used by the F-16 column-level
// tests.
func columnFakes(srcCols, dstCols [][]any) (*pg.FakeConn, *pg.FakeConn) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"srcdb", "C", "C"}})
	src.On("gs-collation-cols", srcCols)
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"srcdb", "C", "C"}})
	dst.On("gs-collation-cols", dstCols)
	return src, dst
}

// TestDetectColumnCollationsDiff verifies (F-16) that a differing explicit
// column collation is reported as a column-level CollationDiff, while a
// matching column and a database-default column are not.
func TestDetectColumnCollationsDiff(t *testing.T) {
	src, dst := columnFakes(
		[][]any{{"id", ""}, {"name", "C"}, {"other", "C.utf8"}},
		[][]any{{"id", ""}, {"name", "C.utf8"}, {"other", "C.utf8"}},
	)
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst)
	if len(out) != 1 {
		t.Fatalf("got %d diffs, want 1: %+v", len(out), out)
	}
	d := out[0]
	if !d.IsColumn() {
		t.Fatalf("expected a column-level diff, got %+v", d)
	}
	if d.Table != "public.t1" || d.Column != "name" {
		t.Fatalf("diff table/column = %q/%q, want public.t1/name", d.Table, d.Column)
	}
	if d.SrcCollation != "C" || d.DstCollation != "C.utf8" {
		t.Fatalf("collations = %q -> %q, want C -> C.utf8", d.SrcCollation, d.DstCollation)
	}
}

// TestDetectColumnCollationsSame verifies that identical explicit collations
// (and database-default columns) produce no column-level diff (F-16).
func TestDetectColumnCollationsSame(t *testing.T) {
	src, dst := columnFakes(
		[][]any{{"id", ""}, {"name", "C"}, {"other", "C.utf8"}},
		[][]any{{"id", ""}, {"name", "C"}, {"other", "C.utf8"}},
	)
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst)
	if len(out) != 0 {
		t.Fatalf("got %d diffs, want 0: %+v", len(out), out)
	}
}

// TestDetectColumnCollationsAbsentDest verifies that when the destination
// table does not exist (the dst gs-collation-cols answer is empty — an
// existing table always has >=1 column), no column-level diff is reported:
// gpstream will CREATE the table from the source DDL (which reproduces the
// COLLATE clause), so there is nothing to warn about (F-16).
func TestDetectColumnCollationsAbsentDest(t *testing.T) {
	src, dst := columnFakes(
		[][]any{{"id", ""}, {"name", "C"}},
		[][]any{}, // destination table absent
	)
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst)
	if len(out) != 0 {
		t.Fatalf("got %d diffs, want 0 (absent dest table): %+v", len(out), out)
	}
}

// TestDetectColumnCollationsFault verifies that a query fault on either side
// degrades to no column-level diff (never a plan failure, F-16) — the dst
// fake has no gs-collation-cols answer, so the query errors.
func TestDetectColumnCollationsFault(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"srcdb", "C", "C"}})
	src.On("gs-collation-cols", [][]any{{"name", "C"}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"srcdb", "C", "C"}})
	// no gs-collation-cols on dst -> Query errors -> columnCollations nil
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst)
	if len(out) != 0 {
		t.Fatalf("got %d diffs, want 0 (query fault degrades): %+v", len(out), out)
	}
}
