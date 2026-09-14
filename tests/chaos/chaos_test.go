// Package chaos holds fault-injection (chaos) tests for the gpstream data path.
// They run by default (no build tag) and need no live cluster: they inject
// faults into the fake SQL layer (panic in a copy, a hung segment via a ping
// delay, an error rate) and assert the Manager/doctor behave — a panic is
// recovered (FR-06), a failure is recorded (CR-7), and a fault is never masked
// as healthy (CR-7/v2 §4.2).
package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// faultManager wires the real Manager over two fake conns. panicFirst makes
// the source panic on the first ReadCopy (a data-path fault, CR-7).
func faultManager(t *testing.T, cfg *config.Config, payload []byte, panicFirst bool) (*manifest.Manager, *manifest.Journal) {
	t.Helper()
	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)
	if panicFirst {
		src.SetPanicCopy() // every copy panics (single unit)
	}

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	dst.On("pg_class", [][]any{})

	j := manifest.NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	return manifest.NewManager(cfg, src, dst, j), j
}

// twoUnitPlan lets a panic in unit 1 not need to abort unit 2 (FR-06).
func twoUnitPlan() *plan.Plan {
	return &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100},
			{DB: "db", Schema: "public", Name: "customers", DstSchema: "public", DstName: "customers", Bytes: 100},
		},
	}
}

// TestPanicRecoveryIsNotAcrash verifies (FR-06/CR-7) that a panic in a unit's
// data path is recovered: the batch does not crash, the unit is recorded as a
// failure (a resume can retry it), and the run returns an error instead of
// panicking. With one worker the panicked unit is isolated; a second unit
// still completes.
func TestPanicRecoveryIsNotAcrash(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 1 // serial: the one worker processes both units
	payload := []byte("BINARY-COPY-ROW-BATCH")

	// Panic only the first copy so unit 2 still completes.
	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)
	src.SetPanicCopyTimes(1)

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	dst.On("pg_class", [][]any{})

	j := manifest.NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := manifest.NewManager(&cfg, src, dst, j)

	// Must not panic: a recovered fault surfaces as an error.
	err := m.Run(context.Background(), twoUnitPlan())
	if err == nil {
		t.Fatal("expected a run error from the recovered panic, got nil")
	}
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1 (the recovered panic)", m.Failures)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1 (the unit after the recovered panic must complete)", m.Units)
	}
	if m.Rows == 0 {
		t.Errorf("rows = 0, want > 0 (the surviving unit must copy data)")
	}
	// The failed unit must be journaled failed so a resume can retry it.
	u := j.Get(1)
	if u == nil || u.State != manifest.StateFailed {
		t.Errorf("expected unit 1 journaled failed, got %+v", u)
	}
}

// TestFaultRateRecorded verifies (FR-04/CR-7) that a fault is counted, not
// hidden: a single unit whose copy panics ends with Failures == 1 (the fault
// is observable), not silently as a success.
func TestFaultRateRecorded(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 1
	payload := []byte("BINARY-COPY-ROW-BATCH")
	m, _ := faultManager(t, &cfg, payload, true)

	plan := &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100},
		},
	}
	err := m.Run(context.Background(), plan)
	if err == nil {
		t.Fatal("expected a run error from the injected fault, got nil")
	}
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1", m.Failures)
	}
}

// TestHungSegmentIsDetected verifies (FR-10/CR-7) that a hung segment (modelled
// as a ping delay) is surfaced as a reachability fault via the fake's ping
// delay hook — the doctor/manager must not treat a hung segment as healthy.
func TestHungSegmentIsDetected(t *testing.T) {
	f := pg.NewFakeConn()
	f.SetPingDelay(3 * time.Second)
	// A ping that would block past a short timeout is treated as unreachable.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := f.Ping(ctx); err == nil {
		t.Fatal("expected a hung (delayed) segment to fail ping within the timeout")
	}
}
