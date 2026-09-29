package source

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestColumnsCollation verifies (F-16) that Columns carries the explicit
// column collation name (pg_collation.collname), and that the database
// default collation is reported as "" (only explicitly COLLATEd columns
// carry a name). Live-verified on 9.4.26: int -> "", text -> "",
// text COLLATE "C" -> "C", text COLLATE "C.utf8" -> "C.utf8".
func TestColumnsCollation(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("format_type", [][]any{
		{"id", "integer", uint32(23), true, "", ""},
		{"name", "text", uint32(25), false, "", "C"},
		{"other", "text", uint32(25), false, "", "C.utf8"},
	})
	a := New(f)
	cols, err := a.Columns(context.Background(), "public", "t1")
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("got %d cols, want 3: %+v", len(cols), cols)
	}
	if cols[0].Collation != "" {
		t.Errorf("id collation = %q, want \"\" (non-string col / default)", cols[0].Collation)
	}
	if cols[1].Collation != "C" {
		t.Errorf("name collation = %q, want C", cols[1].Collation)
	}
	if cols[2].Collation != "C.utf8" {
		t.Errorf("other collation = %q, want C.utf8", cols[2].Collation)
	}
	// The query must read the collation NAME via pg_collation (F-16), not a
	// raw OID.
	joined := strings.Join(f.Queried(), "\n")
	if !strings.Contains(joined, "pg_collation") || !strings.Contains(joined, "attcollation") {
		t.Fatalf("query must resolve attcollation via pg_collation; got:\n%s", joined)
	}
}
