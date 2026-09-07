package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// conflictManager builds a Manager over two fake streaming conns whose dest
// reports the target table EXISTS (so the in-place --truncate/--append paths
// do not create it, matching a real conflicting table). The source and dest
// serve the same binary COPY payload so a verify (when performed) matches.
func conflictManager(t *testing.T, cfg *config.Config, payload []byte, dstExists bool) (*Manager, *pg.FakeConn) {
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

// TestManagerTruncate verifies the --truncate policy (FR-11): the data is
// copied in-place into the final table after a TRUNCATE, the content checksum
// is still verified, and NO staging swap occurs.
func TestManagerTruncate(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Truncate
	m, dst := conflictManager(t, &cfg, []byte("BATCH"), true)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
	if m.Comps != 1 {
		t.Errorf("comps = %d, want 1 (truncate still verifies)", m.Comps)
	}
	q := dst.Queried()
	if !hasQuery(q, "TRUNCATE") {
		t.Errorf("expected a TRUNCATE on the final table, got %v", q)
	}
	if hasQuery(q, "RENAME TO") {
		t.Errorf("in-place --truncate must not swap (no ALTER ... RENAME)")
	}
}

// TestManagerAppend verifies the --append policy (FR-11): the data is copied
// into the final table WITHOUT truncating it, and NO exact checksum verify is
// performed (the final holds existing + source rows).
func TestManagerAppend(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Append
	m, dst := conflictManager(t, &cfg, []byte("BATCH"), true)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
	if m.Rows == 0 {
		t.Errorf("rows = 0, want > 0 (data must have been appended)")
	}
	if m.Comps != 0 {
		t.Errorf("comps = %d, want 0 (--append must not verify an exact checksum)", m.Comps)
	}
	if hasQuery(dst.Queried(), "TRUNCATE") {
		t.Errorf("--append must not TRUNCATE the final table")
	}
	if hasQuery(dst.Queried(), "RENAME TO") {
		t.Errorf("in-place --append must not swap (no ALTER ... RENAME)")
	}
}

// TestManagerResolve locks the conflict-action resolution (FR-11): each action
// selects the right (inPlace, truncate, verify) combination.
func TestManagerResolve(t *testing.T) {
	cases := []struct {
		action        config.ConflictAction
		inPlace, trun, verify bool
	}{
		{config.SkipExisting, false, false, true}, // skipped upstream; default path
		{config.Overwrite, false, false, true},    // staging + swap
		{config.Truncate, true, true, true},
		{config.Append, true, false, false},
	}
	for _, c := range cases {
		cfg := config.Default()
		cfg.ConflictAction = c.action
		m := NewManager(&cfg, pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(nil, ""))
		ip, tr, v := m.resolve(&Unit{Schema: "public", Table: "orders"})
		if ip != c.inPlace || tr != c.trun || v != c.verify {
			t.Errorf("%s: got (inPlace=%v truncate=%v verify=%v), want (%v,%v,%v)",
				c.action, ip, tr, v, c.inPlace, c.trun, c.verify)
		}
	}
}

// hasQuery reports whether any recorded SQL statement contains substr.
func hasQuery(q []string, substr string) bool {
	for _, s := range q {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
