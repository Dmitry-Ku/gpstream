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
func conflictManager(t *testing.T, cfg *config.Config, payload []byte, dstExists bool) (*Manager, *pg.FakeConn, *Journal) {
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
	return NewManager(cfg, src, dst, j), dst, j
}

// TestManagerTruncate verifies the --truncate policy (FR-11): the data is
// copied in-place into the final table after a TRUNCATE, the content checksum
// is still verified, and NO staging swap occurs.
func TestManagerTruncate(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Truncate
	cfg.Verify = config.VerifyOn
	m, dst, _ := conflictManager(t, &cfg, []byte("BATCH"), true)

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
	// verify follows --verify (default off): the post-copy content check is
	// opt-in, independent of the conflict policy.
	cases := []struct {
		action              config.ConflictAction
		verify              config.VerifyMode
		inPlace, trun, expV bool
	}{
		{config.Stop, config.VerifyOff, false, false, false},      // default: no verify
		{config.Stop, config.VerifyOn, false, false, true},        // verify on
		{config.Overwrite, config.VerifyOff, false, false, false}, // staging + swap
		{config.Overwrite, config.VerifyOn, false, false, true},
		{config.Truncate, config.VerifyOff, true, true, false},
		{config.Truncate, config.VerifyOn, true, true, true},
	}
	for _, c := range cases {
		cfg := config.Default()
		cfg.ConflictAction = c.action
		cfg.Verify = c.verify
		m := NewManager(&cfg, pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(nil, ""))
		ip, tr, v := m.resolve(&Unit{Schema: "public", Table: "orders"})
		if ip != c.inPlace || tr != c.trun || v != c.expV {
			t.Errorf("%s/%s: got (inPlace=%v truncate=%v verify=%v), want (%v,%v,%v)",
				c.action, c.verify, ip, tr, v, c.inPlace, c.trun, c.expV)
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

// TestManagerOverwriteAlwaysRewrites locks the --overwrite semantics: an
// existing destination is ALWAYS re-copied (staging + swap), even when its
// data is byte-identical to the source. There is no pre-copy content
// comparison (it would read the whole table twice); the only content check is
// the post-copy verify (--verify on), which is off here by default.
func TestManagerOverwriteAlwaysRewrites(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Overwrite
	// Identical payload on both sides, destination exists.
	m, dst, j := conflictManager(t, &cfg, []byte("SAME-BINARY-PAYLOAD"), true)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1 (an identical --overwrite target is re-copied, not skipped)", m.Units)
	}
	if m.Skipped != 0 {
		t.Errorf("skipped = %d, want 0 (no pre-copy identical-data shortcut)", m.Skipped)
	}
	if m.Failures != 0 {
		t.Errorf("failures = %d, want 0", m.Failures)
	}
	q := dst.Queried()
	if !hasQuery(q, "FROM STDIN") {
		t.Errorf("an identical --overwrite target must be re-copied (COPY FROM STDIN), got %v", q)
	}
	if !hasQuery(q, "RENAME TO") {
		t.Errorf("the re-copied data must be swapped in, got %v", q)
	}
	units := j.Units()
	if len(units) != 1 {
		t.Fatalf("journal has %d units, want 1", len(units))
	}
	if units[0].State != StateSwapped {
		t.Errorf("unit state = %q, want %q (re-copied + swapped)", units[0].State, StateSwapped)
	}
}
