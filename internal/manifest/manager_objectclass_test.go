package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// objectClassManager builds a Manager over two fake streaming conns whose
// dest reports the target table EXISTS when dstExists (matching a real
// conflicting table). The source and dest serve the same binary COPY payload
// so a verify (when performed) matches.
func objectClassManager(t *testing.T, cfg *config.Config, payload []byte, dstExists bool) (*Manager, *pg.FakeConn) {
	t.Helper()
	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	if dstExists {
		dst.On("pg_class", [][]any{{int64(1)}}) // target exists
	} else {
		dst.On("pg_class", [][]any{}) // target absent
	}
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	return NewManager(cfg, src, dst, j), dst
}

// TestMetadataOnly verifies F-11 --metadata-only: the structure (DDL) is
// created, NO data is copied, the unit completes, and the run succeeds even
// when the destination table already exists (idempotent, no conflict).
func TestMetadataOnly(t *testing.T) {
	cfg := config.Default()
	cfg.MetadataOnly = true

	// Fresh destination: DDL is issued, no COPY.
	m, dst := objectClassManager(t, &cfg, []byte("BATCH"), false)
	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
	if m.Rows != 0 {
		t.Errorf("rows = %d, want 0 (no data copy)", m.Rows)
	}
	if dst.WrittenCopy("FROM STDIN") != nil {
		t.Errorf("metadata-only must not COPY data, got %d bytes", len(dst.WrittenCopy("FROM STDIN")))
	}
	q := dst.Queried()
	if !hasQuery(q, "CREATE TABLE") {
		t.Errorf("expected CREATE TABLE (DDL), got %v", q)
	}
	// Unit completed at StateCompleted (not swapped).
	u := m.journal.Units()[0]
	if u.State != StateCompleted {
		t.Errorf("state = %s, want completed", u.State)
	}
}

// TestMetadataOnlyIdempotent verifies the spec F-11 acceptance: a repeated
// metadata-only run over an EXISTING destination structure is idempotent —
// no conflict, no DDL error, nothing copied.
func TestMetadataOnlyIdempotent(t *testing.T) {
	cfg := config.Default()
	cfg.MetadataOnly = true
	// The destination table already exists (a previous metadata-only run).
	m, dst := objectClassManager(t, &cfg, []byte("BATCH"), true)
	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("re-run over existing structure: %v (want idempotent, no conflict)", err)
	}
	if m.Rows != 0 || m.Failures != 0 {
		t.Errorf("re-run: rows=%d failures=%d, want 0/0", m.Rows, m.Failures)
	}
	if dst.WrittenCopy("FROM STDIN") != nil {
		t.Errorf("re-run must not COPY data")
	}
}

// TestDataOnly verifies F-11 --data-only: data is loaded INTO the existing
// destination structure (in-place, after a TRUNCATE), verified, with no
// staging table and no swap.
func TestDataOnly(t *testing.T) {
	cfg := config.Default()
	cfg.DataOnly = true
	// The destination table already exists (the structure is pre-created).
	m, dst := objectClassManager(t, &cfg, []byte("BATCH"), true)
	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
	if m.Rows != 1 {
		t.Errorf("rows = %d, want 1", m.Rows)
	}
	if m.Comps != 1 {
		t.Errorf("comps = %d, want 1 (data-only still verifies)", m.Comps)
	}
	q := dst.Queried()
	if !hasQuery(q, "TRUNCATE") {
		t.Errorf("expected a TRUNCATE (in-place replace), got %v", q)
	}
	if hasQuery(q, "RENAME TO") {
		t.Errorf("data-only is in-place: must not swap (no ALTER ... RENAME)")
	}
	for _, s := range q {
		if strings.Contains(s, "_gs_") {
			t.Errorf("data-only must not use a staging table, saw %q", s)
		}
	}
}

// TestDataOnlyMissingStructure verifies F-11 --data-only: a missing
// destination structure is a hard error (the operator must create it first
// with --metadata-only or a full copy), not a silent create.
func TestDataOnlyMissingStructure(t *testing.T) {
	cfg := config.Default()
	cfg.DataOnly = true
	// The destination table does NOT exist.
	m, dst := objectClassManager(t, &cfg, []byte("BATCH"), false)
	err := m.Run(context.Background(), oneUnitPlan())
	if err == nil {
		t.Fatal("run: want an error for a missing destination structure, got nil")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error = %v, want a 'does not exist' diagnostic", err)
	}
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1", m.Failures)
	}
	if dst.WrittenCopy("FROM STDIN") != nil {
		t.Errorf("must not COPY into a missing table")
	}
}

// TestDataOnlyExistingTableNoConflict verifies F-11 --data-only: the
// destination table ALREADY exists (the normal case), and the planner flags
// it as a conflict (its detection is "does the destination table exist").
// Under the strict default (stop) this must NOT stop the run — an existing
// structure is exactly what data-only loads into.
func TestDataOnlyExistingTableNoConflict(t *testing.T) {
	cfg := config.Default()
	cfg.DataOnly = true
	cfg.ConflictAction = config.Stop
	m, _ := objectClassManager(t, &cfg, []byte("BATCH"), true)
	p := &plan.Plan{Units: []plan.Table{
		{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100, HasConflict: true},
	}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v (an existing structure is not a conflict for --data-only)", err)
	}
	if m.Rows != 1 {
		t.Errorf("rows = %d, want 1 (data was loaded)", m.Rows)
	}
	if m.Failures != 0 {
		t.Errorf("failures = %d, want 0", m.Failures)
	}
}

// TestIncludeClassesDefault verifies the resolver: no flag = both classes
// (the strict default policy is kept).
func TestIncludeClassesDefault(t *testing.T) {
	c := config.Default()
	md, data := c.IncludeClasses()
	if !md || !data {
		t.Errorf("default: md=%v data=%v, want both true", md, data)
	}
}
