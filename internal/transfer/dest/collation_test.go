package dest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestCreateStagingCollation verifies (F-16) that the destination DDL
// reproduces the source column's explicit COLLATE clause, so the destination
// column keeps the source's collation semantics (sorting / indexes over
// string columns). A column with the database-default collation (Collation
// "") emits no COLLATE clause.
func TestCreateStagingCollation(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	cols := []pg.Column{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "name", Type: "text", Nullable: true, Collation: "C"},
		{Name: "other", Type: "text", Nullable: true, Collation: "C.utf8"},
	}
	if err := a.CreateStaging(context.Background(), "public", cols, "_gs_j1_1", pg.Distribution{}); err != nil {
		t.Fatalf("create staging: %v", err)
	}
	last := f.Queried()[len(f.Queried())-1]
	if !strings.Contains(last, `"name" text COLLATE "C"`) {
		t.Fatalf("expected a COLLATE \"C\" clause for name, got %q", last)
	}
	if !strings.Contains(last, `"other" text COLLATE "C.utf8"`) {
		t.Fatalf("expected a COLLATE \"C.utf8\" clause for other, got %q", last)
	}
	// The id column (no explicit collation) must NOT carry a COLLATE clause.
	if strings.Contains(last, `"id" integer COLLATE`) {
		t.Fatalf("id must not carry a COLLATE clause, got %q", last)
	}
}

// TestCreateStagingNoCollation verifies that when no column carries an
// explicit collation the DDL is unchanged (no COLLATE clause anywhere) —
// the pre-F-16 behaviour.
func TestCreateStagingNoCollation(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	cols := []pg.Column{
		{Name: "id", Type: "integer", Nullable: false},
		{Name: "name", Type: "text", Nullable: true},
	}
	if err := a.CreateStaging(context.Background(), "public", cols, "_gs_j1_2", pg.Distribution{}); err != nil {
		t.Fatalf("create staging: %v", err)
	}
	last := f.Queried()[len(f.Queried())-1]
	if strings.Contains(last, "COLLATE") {
		t.Fatalf("no COLLATE clause expected, got %q", last)
	}
}
