package manifest

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/plan"
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
