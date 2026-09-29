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
	out := detectCollations(context.Background(), tabs, src, dst, "")
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
	out := detectCollations(context.Background(), tabs, src, dst, "")
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
	out := detectCollations(context.Background(), tabs, src, dst, "")
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
	out := detectCollations(context.Background(), tabs, src, dst, "")
	if len(out) != 0 {
		t.Fatalf("got %d diffs, want 0 (query fault degrades): %+v", len(out), out)
	}
}

// TestDetectCollationsDestDBFromConnString verifies (F-16) that when the
// source DB and the destination database differ in name (the common case:
// --dest connstring points at a differently-named database), the DB-level
// collation comparison is made against the --dest database — and a diff is
// reported when the two databases differ in LC_COLLATE. Without the fix the
// pair was {t.DB, t.DstDB} = {t_coll_src, t_coll_src}; t_coll_src does not
// exist on the destination, so the diff was silently dropped.
func TestDetectCollationsDestDBFromConnString(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"t_coll_src", "C", "C"}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"t_coll_dst", "en_US.utf8", "en_US.utf8"}})
	tabs := []Table{{DB: "t_coll_src", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/t_coll_dst?sslmode=disable")
	if len(out) != 1 {
		t.Fatalf("got %d diffs, want 1: %+v", len(out), out)
	}
	if out[0].Table != "" || out[0].IsColumn() {
		t.Fatalf("expected a DB-level diff, got %+v", out[0])
	}
	if out[0].DB != "t_coll_src" {
		t.Fatalf("diff.DB = %q, want t_coll_src", out[0].DB)
	}
	if out[0].LCCollate != "C -> en_US.utf8" {
		t.Fatalf("LCCollate = %q, want C -> en_US.utf8", out[0].LCCollate)
	}
}

// TestDetectCollationsDestDBSameName verifies (F-16) that when the --dest
// database has the same collation as the source database, no DB-level diff
// is reported (same-name or different-name dest DB alike).
func TestDetectCollationsDestDBSameName(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"db1", "en_US.utf8", "en_US.utf8"}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"db2", "en_US.utf8", "en_US.utf8"}})
	tabs := []Table{{DB: "db1", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/db2?sslmode=disable")
	if len(out) != 0 {
		t.Fatalf("got %d diffs, want 0: %+v", len(out), out)
	}
}
