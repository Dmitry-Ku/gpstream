package dest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestDropTable issues DROP TABLE IF EXISTS for a final table (FR-02
// cleanup: gpstream-owned tables must be removable).
func TestDropTable(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	if err := a.DropTable(context.Background(), "public", "t_final"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	last := lastQueried(f)
	if !strings.Contains(last, `DROP TABLE IF EXISTS "public"."t_final"`) {
		t.Fatalf("want DROP TABLE IF EXISTS \"public\".\"t_final\", got %s", last)
	}
}

// TestDropSchemaIfEmptyNonPublicDropsWhenEmpty: no objects remain -> DROP
// SCHEMA IF EXISTS is issued.
func TestDropSchemaIfEmptyNonPublicDropsWhenEmpty(t *testing.T) {
	f := pg.NewFakeConn().On("FROM pg_class c", [][]any{{int64(0)}})
	a := mkAgent(f)
	if err := a.DropSchemaIfEmpty(context.Background(), "s_created"); err != nil {
		t.Fatalf("drop schema if empty: %v", err)
	}
	q := f.Queried()
	if !hasQuery(q, `DROP SCHEMA IF EXISTS "s_created"`) {
		t.Fatalf("want DROP SCHEMA IF EXISTS \"s_created\", got %v", q)
	}
}

// TestDropSchemaIfEmptyKeepsNonEmpty: any remaining object -> the schema is
// left in place (pre-existing data protection, FR-02).
func TestDropSchemaIfEmptyKeepsNonEmpty(t *testing.T) {
	f := pg.NewFakeConn().On("FROM pg_class c", [][]any{{int64(3)}})
	a := mkAgent(f)
	if err := a.DropSchemaIfEmpty(context.Background(), "s_busy"); err != nil {
		t.Fatalf("drop schema if empty: %v", err)
	}
	for _, q := range f.Queried() {
		if strings.Contains(q, "DROP SCHEMA") {
			t.Fatalf("non-empty schema must not be dropped: %v", f.Queried())
		}
	}
}

// TestDropSchemaIfEmptyNeverPublic: public and "" are no-ops (they
// pre-exist on every cluster and are never gpstream-created).
func TestDropSchemaIfEmptyNeverPublic(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	if err := a.DropSchemaIfEmpty(context.Background(), "public"); err != nil {
		t.Fatalf("public no-op: %v", err)
	}
	if err := a.DropSchemaIfEmpty(context.Background(), ""); err != nil {
		t.Fatalf("empty no-op: %v", err)
	}
	if n := len(f.Queried()); n != 0 {
		t.Fatalf("no SQL expected for public/empty, got %v", f.Queried())
	}
}

// TestDropSchemaIfEmptyQueryFault: a catalog fault propagates (the caller
// reports it as a failed drop, never a silent pass).
func TestDropSchemaIfEmptyQueryFault(t *testing.T) {
	f := pg.NewFakeConn() // no canned result -> Query errors
	a := mkAgent(f)
	if err := a.DropSchemaIfEmpty(context.Background(), "s"); err == nil {
		t.Fatal("catalog fault must propagate")
	}
}

func lastQueried(f *pg.FakeConn) string {
	q := f.Queried()
	return q[len(q)-1]
}

func hasQuery(q []string, sub string) bool {
	for _, s := range q {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
