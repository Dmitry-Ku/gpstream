package manifest

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// twoUnitPlan returns a plan with two units so a panic in one does not need to
// abort the other (FR-06).
func twoUnitPlan() *plan.Plan {
	return &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100},
			{DB: "db", Schema: "public", Name: "customers", DstSchema: "public", DstName: "customers", Bytes: 100},
		},
	}
}

// TestManagerPanicRecovery verifies (FR-06) that a panic while processing one
// unit is recovered: the batch does not crash, the panicked unit is recorded as
// a failure (a resume can retry it, FR-07), and the other unit still completes.
// The panic is injected on the FIRST ReadCopy (unit 1's copy) so unit 2's copy
// still runs — proving the worker recovers and continues.
func TestManagerPanicRecovery(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 1 // serial: the one worker must process both units
	payload := []byte("BINARY-COPY-ROW-BATCH")

	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)
	src.SetPanicCopyTimes(1) // panic the first copy (unit 1); unit 2's copy succeeds

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	dst.On("pg_class", [][]any{})

	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	// The run must not crash (a panic would propagate if recovery were missing).
	// A failure is returned instead.
	err := m.Run(context.Background(), twoUnitPlan())
	if err == nil {
		t.Fatalf("expected a run error from the recovered panic, got nil")
	}
	// The panicked unit was recorded as a failure; the other unit still ran.
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1 (the recovered panic)", m.Failures)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1 (the unit after the recovered panic must complete)", m.Units)
	}
	if m.Rows == 0 {
		t.Errorf("rows = 0, want > 0 (the surviving unit must copy data)")
	}
	// The failed unit must be journaled as failed so a resume can retry it.
	u := j.Get(1)
	if u == nil || u.State != StateFailed {
		t.Errorf("expected unit 1 journaled as failed, got %+v", u)
	}
}

// TestManagerOnUnitFailureAbort verifies (spec F-06) that
// --on-unit-failure=abort stops the run at the first unit failure: the failed
// unit is journaled, NO unit after it is started (a serial run must not copy
// the second table), and the run returns the failure. The DEFAULT (continue)
// is the opposite — TestManagerPanicRecovery above covers it.
func TestManagerOnUnitFailureAbort(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 1 // serial: abort must be observable between units
	cfg.OnUnitFailure = "abort"
	payload := []byte("BINARY-COPY-ROW-BATCH")

	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)
	src.SetPanicCopyTimes(1) // unit 1 fails; unit 2 must never start

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	dst.On("pg_class", [][]any{})

	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	err := m.Run(context.Background(), twoUnitPlan())
	if err == nil {
		t.Fatalf("expected the abort-mode run to return the failure, got nil")
	}
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1 (the first failed unit)", m.Failures)
	}
	if m.Units != 0 {
		t.Errorf("units = %d, want 0 (abort: no unit may start after the first failure)", m.Units)
	}
	if u := j.Get(2); u != nil && u.State != "" {
		t.Errorf("unit 2 must never be started in abort mode, got state %s", u.State)
	}
}

// TestManagerOnUnitFailureContinue verifies (spec F-06) the DEFAULT: with
// --on-unit-failure=continue (the default) a failed unit does NOT stop the
// run — the next unit still completes and both outcomes are reported.
func TestManagerOnUnitFailureContinue(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 1 // serial
	payload := []byte("BINARY-COPY-ROW-BATCH")

	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)
	src.SetPanicCopyTimes(1) // unit 1 fails

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	dst.On("pg_class", [][]any{})

	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), twoUnitPlan()); err == nil {
		t.Fatalf("expected the run to return the failed unit's error, got nil")
	}
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1 (the failed unit is recorded)", m.Failures)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1 (continue: the next unit still completes)", m.Units)
	}
}

// TestManagerPanicInCopyStream verifies (FR-06) that a panic inside a copy
// sub-goroutine (not the worker's own goroutine) is recovered as an error and
// does not crash the run — the data-path goroutines have their own recovery.
func TestManagerPanicInCopyStream(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 1
	payload := []byte("BINARY-COPY-ROW-BATCH")

	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)
	src.SetPanicCopy() // panic every copy (single unit)

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	dst.On("pg_class", [][]any{})

	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	// The run must not crash: the copy sub-goroutine's deferred recover + pipe
	// close make the panic an error (FR-06).
	err := m.Run(context.Background(), oneUnitPlan())
	if err == nil {
		t.Fatalf("expected a run error from the recovered copy panic, got nil")
	}
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1", m.Failures)
	}
}
