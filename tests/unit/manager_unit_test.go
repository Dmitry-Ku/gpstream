// Package unit holds black-box (external) unit tests for the gpstream data
// path. These sit in a separate directory from the internal/* white-box tests
// so they exercise only the EXPORTED API — exactly what an operator's
// integration harness would see. They run by default (no build tag) and need
// no live cluster (they use pg.NewFakeConn, the same substitute the internal
// tests use), so they are fast and deterministic.
package unit

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// newManager wires the real Manager over two fake streaming conns and a
// file-mirrored journal. The source serves a binary COPY payload; the dest
// records COPY ... FROM STDIN and re-reads its own payload for verify. Only
// the SQL layer is faked — framing, compression, verify and the journal are
// the real code.
func newManager(t *testing.T, cfg *config.Config, srcPayload, dstPayload []byte) (*manifest.Manager, *manifest.Journal) {
	t.Helper()
	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", srcPayload)

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", dstPayload)
	// Swap's existence guard: the target does not exist.
	dst.On("pg_class", [][]any{})

	j := manifest.NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	return manifest.NewManager(cfg, src, dst, j), j
}

// oneUnitPlan is a single-unit plan (no conflict).
func oneUnitPlan() *plan.Plan {
	return &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100},
		},
	}
}

// TestCopyHappyPath verifies the full unit pipeline end-to-end (FR-01/FR-02/
// FR-17): copy -> verify (checksum match) -> swap -> journal. The framed,
// compressed bytes must round-trip so the verify checksum matches.
func TestCopyHappyPath(t *testing.T) {
	cfg := config.Default()
	cfg.Verify = config.VerifyOn
	payload := []byte("BINARY-COPY-ROW-BATCH")
	m, j := newManager(t, &cfg, payload, payload)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
	if m.Rows == 0 {
		t.Errorf("rows = 0, want > 0 (data must have been copied)")
	}
	if m.Comps != 1 {
		t.Errorf("comps = %d, want 1 (verified checksum)", m.Comps)
	}
	// The unit must be journaled as swapped (terminal success) so a resume
	// would skip it (FR-06/07).
	u := j.Get(1)
	if u == nil || u.State != manifest.StateSwapped {
		t.Errorf("expected unit 1 journaled swapped (success), got %+v", u)
	}
}

// TestSkipNoCopy verifies the explicit skip policy (FR-10/FR-11): with
// --resolve-conflicts skip and a target that already exists, the unit is
// skipped — no copy, no failure, no overwrite of existing data.
// (The DEFAULT --resolve-conflicts stop is strict and STOPS the run — see
// TestManagerStrictConflictStops.)
func TestSkipNoCopy(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Skip // --resolve-conflicts skip
	payload := []byte("BINARY-COPY-ROW-BATCH")
	m, _ := newManager(t, &cfg, payload, payload)

	// The planner flagged the conflict, so the manager must skip, not copy.
	plan := &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100, HasConflict: true},
		},
	}
	if err := m.Run(context.Background(), plan); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Skipped != 1 {
		t.Errorf("skipped = %d, want 1", m.Skipped)
	}
	if m.Rows != 0 {
		t.Errorf("rows = %d, want 0 (a skipped unit must not copy)", m.Rows)
	}
	if m.Failures != 0 {
		t.Errorf("failures = %d, want 0", m.Failures)
	}
}
