package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// conflictManager builds a Manager over two fake streaming conns whose dest
// reports the target table EXISTS (so the in-place --truncate path
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

// TestManagerResolve locks the --resolve-conflicts resolution (FR-11): each
// selects the right (inPlace, truncate, verify) combination.
func TestManagerResolve(t *testing.T) {
	cases := []struct {
		action                config.ConflictAction
		inPlace, trun, verify bool
	}{
		{config.Stop, false, false, true},      // stopped upstream; default path
		{config.Overwrite, false, false, true}, // staging + swap
		{config.Truncate, true, true, true},
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

// TestManagerOverwriteChecksumSkip verifies F-10 §8: under --overwrite a
// conflicting table whose data is ALREADY identical on the destination
// (same content digest) is skipped, not rewritten. (The mismatch case — a
// differing destination is re-copied via staging+swap — is covered by
// TestManagerOverwriteSwap.)
func TestManagerOverwriteChecksumSkip(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Overwrite
	// Identical payload on both sides -> digests match -> skip.
	m, dst := conflictManager(t, &cfg, []byte("SAME-BINARY-PAYLOAD"), true)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Skipped != 1 {
		t.Errorf("skipped = %d, want 1 (identical --overwrite target)", m.Skipped)
	}
	if m.Units != 0 {
		t.Errorf("units = %d, want 0 (a checksum-matching overwrite is skipped, not copied)", m.Units)
	}
	if m.Failures != 0 {
		t.Errorf("failures = %d, want 0", m.Failures)
	}
	q := dst.Queried()
	if hasQuery(q, "FROM STDIN") {
		t.Errorf("a checksum-matching --overwrite must not COPY into the destination, got %v", q)
	}
	if hasQuery(q, "RENAME TO") {
		t.Errorf("a skipped unit must not swap, got %v", q)
	}
}
