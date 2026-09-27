package dest

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// mkAgent returns a destination agent over a fake streaming conn.
func mkAgent(f *pg.FakeConn) *Agent { return New(f) }

// TestStagingName checks the staging-name scheme (FR-02).
func TestStagingName(t *testing.T) {
	if got := StagingName("j1", 5); got != "_gs_j1_5" {
		t.Fatalf("staging name = %q", got)
	}
}

// TestCreateStaging issues a CREATE TABLE IF NOT EXISTS with faithful columns
// (FR-02/FR-03) and swallows "already exists" (FR-07 resume).
func TestCreateStaging(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	cols := []pg.Column{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "customer", Type: "text", Nullable: true, Default: `''::text`},
	}
	if err := a.CreateStaging(context.Background(), "public", cols, "_gs_j1_5"); err != nil {
		t.Fatalf("create staging: %v", err)
	}
	q := f.Queried()
	last := q[len(q)-1]
	if !strings.Contains(last, "CREATE TABLE IF NOT EXISTS") {
		t.Fatalf("expected a CREATE TABLE IF NOT EXISTS, got %v", q)
	}
	if !strings.Contains(last, "NOT NULL") {
		t.Fatalf("expected NOT NULL for non-null col in %q", last)
	}
}

// TestDropStaging issues DROP TABLE IF EXISTS (FR-02/FR-07).
func TestDropStaging(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	if err := a.DropStaging(context.Background(), "public", "_gs_j1_5"); err != nil {
		t.Fatalf("drop staging: %v", err)
	}
	q := f.Queried()
	last := q[len(q)-1]
	if !strings.Contains(last, "DROP TABLE IF EXISTS") {
		t.Fatalf("expected DROP TABLE IF EXISTS, got %v", q)
	}
}

// TestCopyIntoStream streams the binary COPY payload into the staging table
// (FR-01). The fake records what was written.
func TestCopyIntoStream(t *testing.T) {
	f := pg.NewFakeConn()
	f.OnWriteCopy("FROM STDIN")
	a := mkAgent(f)
	n, err := a.CopyIntoStream(context.Background(), "public", "_gs_j1_5",
		bytes.NewReader([]byte("BINARY-ROW-BATCH")))
	if err != nil {
		t.Fatalf("copy into stream: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero rows written")
	}
	if got := f.WrittenCopy("FROM STDIN"); string(got) != "BINARY-ROW-BATCH" {
		t.Fatalf("wrote %q, want BINARY-ROW-BATCH", got)
	}
}

// TestVerifyMismatch fails when the content checksum differs (FR-02/FR-07).
func TestVerifyMismatch(t *testing.T) {
	f := pg.NewFakeConn()
	f.OnReadCopy("TO STDOUT", []byte("some-data"))
	a := mkAgent(f)
	ok, rows, err := a.Verify(context.Background(), "public", "_gs_j1_5", "deadbeef")
	if err == nil {
		t.Fatal("expected a mismatch error")
	}
	if ok {
		t.Fatal("verify should have failed on mismatch")
	}
	if rows == 0 {
		t.Fatal("expected a non-zero row count on the re-derived payload")
	}
}

// TestVerifyMatch succeeds when the derived checksum equals want (FR-02).
func TestVerifyMatch(t *testing.T) {
	f := pg.NewFakeConn()
	payload := []byte("verify-payload")
	f.OnReadCopy("TO STDOUT", payload)
	a := mkAgent(f)
	want, err := transfer.ContentDigest(payload)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	ok, rows, err := a.Verify(context.Background(), "public", "_gs_j1_5", want)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !ok {
		t.Fatal("verify should match")
	}
	if rows == 0 {
		t.Fatal("expected non-zero rows")
	}
}

// TestSwapNoOverwriteFails refuses to clobber an existing final table
// (FR-11 skip-existing default).
func TestSwapNoOverwriteFails(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{{int64(1)}}) // exists -> true
	a := mkAgent(f)
	err := a.Swap(context.Background(), "public", "orders", "_gs_j1_5", false)
	if err == nil {
		t.Fatal("swap should fail when target exists and overwrite is not set")
	}
}

// TestSwapOverwrite drops the existing table then renames (FR-11).
func TestSwapOverwrite(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{}) // exists -> false (guard passes)
	a := mkAgent(f)
	if err := a.Swap(context.Background(), "public", "orders", "_gs_j1_5", true); err != nil {
		t.Fatalf("swap overwrite: %v", err)
	}
	q := f.Queried()
	last := q[len(q)-1]
	if !strings.Contains(last, "RENAME") {
		t.Fatalf("expected a RENAME in %v", q)
	}
}

// TestExists checks the existence probe (true/false).
func TestExists(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{{int64(1)}})
	a := mkAgent(f)
	if !a.Exists(context.Background(), "public", "orders") {
		t.Fatal("expected exists=true")
	}

	f2 := pg.NewFakeConn()
	f2.On("pg_class", [][]any{})
	a2 := mkAgent(f2)
	if a2.Exists(context.Background(), "public", "orders") {
		t.Fatal("expected exists=false")
	}
}

// TestEnsureSchema creates a schema (FR-11) and is a no-op for "".
func TestEnsureSchema(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	if err := a.EnsureSchema(context.Background(), ""); err != nil {
		t.Fatalf("empty schema should be a no-op: %v", err)
	}
	if err := a.EnsureSchema(context.Background(), "sales"); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	q := f.Queried()
	last := q[len(q)-1]
	if !strings.Contains(last, "CREATE SCHEMA IF NOT EXISTS") {
		t.Fatalf("expected CREATE SCHEMA IF NOT EXISTS, got %v", q)
	}
}

// TestIsAlreadyExists checks the lenient 42P01 error detection (FR-07).
func TestIsAlreadyExists(t *testing.T) {
	if isAlreadyExists(nil) {
		t.Fatal("nil is not already-exists")
	}
	if !isAlreadyExists(errWith("42P01")) {
		t.Fatal("42P01 should match")
	}
	if isAlreadyExists(errWith("some other error")) {
		t.Fatal("unrelated error should not match")
	}
}

// TestCreateIndexesNew issues a CREATE INDEX for each definition (F-17),
// rewriting the source names to the destination names on a rename.
func TestCreateIndexesNew(t *testing.T) {
	f := pg.NewFakeConn()
	// No index exists yet (empty pg_class lookup).
	f.On("pg_class", [][]any{})
	a := mkAgent(f)
	defs := []pg.IndexDef{
		{Name: "orders_customer_idx", Def: `CREATE INDEX orders_customer_idx ON public.orders USING btree (customer)`},
		{Name: "orders_pkey", PK: true, Def: `CREATE UNIQUE INDEX orders_pkey ON public.orders USING btree (id)`},
	}
	if err := a.CreateIndexes(context.Background(), "s_new", "orders_v2", "public", "orders", defs); err != nil {
		t.Fatalf("create indexes: %v", err)
	}
	q := f.Queried()
	sawCreate := 0
	for _, sql := range q {
		if strings.Contains(sql, "CREATE INDEX") || strings.Contains(sql, "CREATE UNIQUE INDEX") {
			// A rename must be rewritten to the destination names.
			if !strings.Contains(sql, "s_new.orders_v2") {
				t.Fatalf("index definition not rewritten to destination names: %q", sql)
			}
			sawCreate++
		}
	}
	if sawCreate != 2 {
		t.Fatalf("expected 2 CREATE INDEX statements, saw %d in %v", sawCreate, q)
	}
}

// TestCreateIndexesSameName verifies that when src and dst names match, the
// definition is applied verbatim (no rewrite).
func TestCreateIndexesSameName(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{})
	a := mkAgent(f)
	defs := []pg.IndexDef{{Name: "i1", Def: `CREATE INDEX i1 ON public.orders (id)`}}
	if err := a.CreateIndexes(context.Background(), "public", "orders", "public", "orders", defs); err != nil {
		t.Fatalf("create indexes: %v", err)
	}
	q := f.Queried()
	last := q[len(q)-1]
	if last != `CREATE INDEX i1 ON public.orders (id)` {
		t.Fatalf("definition altered on a non-rename: %q", last)
	}
}

// TestCreateIndexesIdempotent verifies resume safety (F-17): an index that
// already exists is REINDEXed by name instead of being re-created, so a
// crash mid-build leaves no duplicates.
func TestCreateIndexesIdempotent(t *testing.T) {
	f := pg.NewFakeConn()
	// The index already exists (pg_class lookup returns a row).
	f.On("pg_class", [][]any{{1}})
	a := mkAgent(f)
	defs := []pg.IndexDef{{Name: "orders_pkey", PK: true, Def: `CREATE UNIQUE INDEX orders_pkey ON public.orders (id)`}}
	if err := a.CreateIndexes(context.Background(), "public", "orders", "public", "orders", defs); err != nil {
		t.Fatalf("create indexes: %v", err)
	}
	q := f.Queried()
	sawReindex, sawCreate := false, false
	for _, sql := range q {
		if strings.Contains(sql, "REINDEX INDEX") {
			sawReindex = true
		}
		if strings.Contains(sql, "CREATE INDEX") || strings.Contains(sql, "CREATE UNIQUE INDEX") {
			sawCreate = true
		}
	}
	if !sawReindex {
		t.Fatalf("expected REINDEX INDEX for an existing index, got %v", q)
	}
	if sawCreate {
		t.Fatalf("must not re-create an existing index (duplicates): %v", q)
	}
}

// TestCreateIndexesEmpty verifies an empty definition list is a no-op
// (a table without indexes).
func TestCreateIndexesEmpty(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	if err := a.CreateIndexes(context.Background(), "public", "orders", "public", "orders", nil); err != nil {
		t.Fatalf("create indexes (empty): %v", err)
	}
	if len(f.Queried()) != 0 {
		t.Fatalf("expected no SQL for an empty definition list, got %v", f.Queried())
	}
}

func errWith(s string) error { return stringsError(s) }

type stringsError string

func (e stringsError) Error() string { return string(e) }

var _ = io.EOF
