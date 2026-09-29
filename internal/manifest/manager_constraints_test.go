package manifest

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// constraintSrcStub returns the source-fake canned rows for the F-11
// constraint-capture query (source.Constraints). The SOH (\x01) delimiter
// separates multi-column keys, mirroring the live pg_get_constraintdef shape.
func constraintSrcStub(t *testing.T, f *pg.FakeConn) {
	t.Helper()
	f.On("pg_get_constraintdef", [][]any{
		// name, contype, def, localcols, refcols, refschemaname, reftablename, deltype, uptype
		{"ch_pos", "c", "CHECK ((id > 0))", "id", "", "", "", "", ""},
		{"fk_child_parent", "f", "FOREIGN KEY (p) REFERENCES parents(id)",
			"parent_id", "id", "public", "parents", "c", "a"},
	})
}

// eventsContaining reads the run's events.jsonl and reports whether any line
// contains substr.
func eventsContaining(t *testing.T, dir, substr string) bool {
	t.Helper()
	b, err := os.ReadFile(dir + "/events.jsonl")
	if err != nil {
		return false
	}
	return strings.Contains(string(b), substr)
}

// TestManagerRestoresConstraints verifies the F-11 post-swap constraint pass:
// after the data swap the source's foreign key and user CHECK are re-applied on
// the destination table (the staging+swap path creates a fresh table whose
// constraints would otherwise be lost). The FK references a table OUTSIDE the
// migration set, so it keeps its source name and an external-ref event is
// logged (the operator decides whether that table exists on the destination).
func TestManagerRestoresConstraints(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	constraintSrcStub(t, src)
	// The destination reports the constraint is not yet present (fresh table).
	dst.On("pg_constraint", [][]any{})

	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)

	p := &plan.Plan{Units: []plan.Table{
		{DB: "db", Schema: "public", Name: "children", DstSchema: "public", DstName: "children", Bytes: 100},
	}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	u := j.Units()[0]
	if u.State != StateSwapped {
		t.Fatalf("unit state = %s, want %s", u.State, StateSwapped)
	}
	q := dst.Queried()
	if !hasQuery(q, `ADD CONSTRAINT "ch_pos" CHECK ((id > 0))`) {
		t.Fatalf("CHECK constraint not restored, got %v", q)
	}
	if !hasQuery(q, `ADD CONSTRAINT "fk_child_parent" FOREIGN KEY ("parent_id") REFERENCES "public"."parents"("id") ON DELETE CASCADE`) {
		t.Fatalf("FK constraint not restored, got %v", q)
	}
	// The referenced table (public.parents) is outside the migration set, so an
	// external-ref diagnostic is recorded (F-04) for the operator.
	if !eventsContaining(t, dir, "constraint_external_ref") {
		t.Fatalf("expected a constraint_external_ref event, events:\n%s", readFile(t, dir+"/events.jsonl"))
	}
}

// TestManagerRestoresConstraintWithRename verifies that when the owning table
// AND the referenced table are both renamed (FR-11), the restored FK references
// the REFERENCED table's destination name (the rename map), not its source
// name. This is the search_path-independent structural rewrite: the source's
// pg_get_constraintdef may or may not have qualified the referenced table, but
// the destination DDL is always schema-qualified and points at the renamed
// referenced table.
func TestManagerRestoresConstraintWithRename(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	constraintSrcStub(t, src)
	dst.On("pg_constraint", [][]any{})

	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)

	// Both the FK owner and the referenced table are renamed: public.children
	// -> s_new.children_v2 and public.parents -> s_new.parents_v2.
	p := &plan.Plan{Units: []plan.Table{
		{DB: "db", Schema: "public", Name: "children", DstSchema: "s_new", DstName: "children_v2", Bytes: 100},
		{DB: "db", Schema: "public", Name: "parents", DstSchema: "s_new", DstName: "parents_v2", Bytes: 100},
	}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	q := dst.Queried()
	// The FK on the renamed owner references the RENAMED referenced table.
	if !hasQuery(q, `ADD CONSTRAINT "fk_child_parent" FOREIGN KEY ("parent_id") REFERENCES "s_new"."parents_v2"("id") ON DELETE CASCADE`) {
		t.Fatalf("FK not rewritten to the renamed referenced table, got %v", q)
	}
}

// TestManagerNoConstraintsDisables verifies --no-constraints turns the F-11
// restore pass off: no ADD CONSTRAINT is issued even though the source has
// constraints.
func TestManagerNoConstraintsDisables(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	constraintSrcStub(t, src)
	dst.On("pg_constraint", [][]any{})

	cfg := configDefault()
	cfg.NoConstraints = true
	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(cfg, src, dst, j)

	p := &plan.Plan{Units: []plan.Table{
		{DB: "db", Schema: "public", Name: "children", DstSchema: "public", DstName: "children", Bytes: 100},
	}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasQuery(dst.Queried(), "ADD CONSTRAINT") {
		t.Fatalf("--no-constraints must not restore constraints, got %v", dst.Queried())
	}
}

// TestManagerConstraintResumeIdempotent verifies a resume re-run does not
// re-apply an already-restored constraint: the destination reports the
// constraint present, so the pass skips the ALTER (the F-11 idempotency guard
// that makes a T5 resume safe to repeat).
func TestManagerConstraintResumeIdempotent(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	constraintSrcStub(t, src)
	// The constraint already exists on the destination (a previous run applied
	// it): the existence probe returns a row, so no ADD CONSTRAINT is issued.
	dst.On("pg_constraint", [][]any{{int64(1)}})

	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)

	p := &plan.Plan{Units: []plan.Table{
		{DB: "db", Schema: "public", Name: "children", DstSchema: "public", DstName: "children", Bytes: 100},
	}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasQuery(dst.Queried(), "ADD CONSTRAINT") {
		t.Fatalf("an already-restored constraint must not be re-applied, got %v", dst.Queried())
	}
}

func configDefault() *config.Config {
	c := config.Default()
	return &c
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return "<missing>"
	}
	return string(b)
}
