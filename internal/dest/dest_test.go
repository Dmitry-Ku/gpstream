package dest

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/pg"
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
	sum := md5.Sum(payload)
	want := hex.EncodeToString(sum[:])
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

func errWith(s string) error { return stringsError(s) }

type stringsError string

func (e stringsError) Error() string { return string(e) }

var _ = io.EOF
