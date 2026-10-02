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

// TestManagerRestoresPKConstraintRename verifies that on a renamed table the
// primary key is restored under the name of the index the FR-17 pool already
// built (dest.DestIndexName), so ADD CONSTRAINT adopts that pre-built unique
// index instead of creating a duplicate. The source carries both a PK
// constraint (whose unique index is orders_pkey) and its index definition.
func TestManagerRestoresPKConstraintRename(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	// Source: the PK index (the FR-17 pool SKIPS it — the PK index is built
	// atomically by ADD CONSTRAINT in the constraint pass, not by the pool).
	src.On("pg_index", [][]any{
		{"orders_pkey", false, true, "CREATE UNIQUE INDEX orders_pkey ON public.orders (id)"},
	})
	// Source: the PK constraint (contype p).
	src.On("pg_get_constraintdef", [][]any{
		// name, contype, def, localcols, refcols, refschemaname, reftablename, deltype, uptype
		{"orders_pkey", "p", "PRIMARY KEY (id)", "id", "", "", "", "", ""},
	})
	// Destination: no constraint yet (constraintExists -> empty); the swap
	// guard matches "pg_class" (empty) from newPair.
	dst.On("pg_constraint", [][]any{})

	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)
	if err := m.Run(context.Background(), renamedPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	q := dst.Queried()
	// The pool must NOT build the PK index (ADD CONSTRAINT builds it; a
	// pre-built one would be a 42P07 collision).
	for _, sql := range q {
		if strings.Contains(sql, "CREATE UNIQUE INDEX orders_pkey") {
			t.Fatalf("the pool must not build the PK index, got %q in %v", sql, q)
		}
	}
	// The PK constraint is restored under the REWRITTEN name (destination
	// table naming), building its index atomically.
	if !hasQuery(q, `ADD CONSTRAINT "orders_v2_pkey" PRIMARY KEY (id)`) {
		t.Fatalf("PK constraint not restored under the rewritten name, got %v", q)
	}
}

// TestManagerRestoresPKConstraintNoRename verifies that without a rename the
// PK constraint keeps its source name (idempotent re-apply on a re-run).
func TestManagerRestoresPKConstraintNoRename(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	src.On("pg_index", [][]any{
		{"orders_pkey", false, true, "CREATE UNIQUE INDEX orders_pkey ON public.orders (id)"},
	})
	src.On("pg_get_constraintdef", [][]any{
		// name, contype, def, localcols, refcols, refschemaname, reftablename, deltype, uptype
		{"orders_pkey", "p", "PRIMARY KEY (id)", "id", "", "", "", "", ""},
	})
	dst.On("pg_constraint", [][]any{})

	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)
	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	q := dst.Queried()
	// No pool-built PK index (ADD CONSTRAINT builds it)...
	for _, sql := range q {
		if strings.Contains(sql, "CREATE UNIQUE INDEX orders_pkey") {
			t.Fatalf("the pool must not build the PK index, got %q in %v", sql, q)
		}
	}
	// ...the PK constraint is restored under its source name.
	if !hasQuery(q, `ADD CONSTRAINT "orders_pkey" PRIMARY KEY (id)`) {
		t.Fatalf("PK constraint not restored under its source name, got %v", q)
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
