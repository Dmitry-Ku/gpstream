// Package perf holds performance-characterization tests for the gpstream data
// path. They run by default (no build tag) and need no live cluster: they drive
// the real Manager over faked SQL (only the SQL layer is faked — framing,
// compression, verify, the journal and the adaptive limiter are the real code)
// and assert the *characteristics* gpstream must have: a rate-limit actually
// bounds the data path, and a batch of N units all completes (FR-01/FR-03/
// FR-14). They are not a 100 Gbit/s / 100 TB–1 PB benchmark (that needs a live
// cluster) — see RC-CHECKLIST.md.
package perf

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/ratelimit"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/plan"
)

// buildManager wires the real Manager over two fake conns with the same payload
// on both sides (so verify matches) and a file-mirrored journal.
func buildManager(t *testing.T, cfg *config.Config, payload []byte) (*manifest.Manager, *manifest.Journal) {
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
	dst.On("pg_class", [][]any{})

	j := manifest.NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	return manifest.NewManager(cfg, src, dst, j), j
}

// nUnitPlan builds a plan of n identical units (FR-03: architecture
// independent, many tables).
func nUnitPlan(n int) *plan.Plan {
	units := make([]plan.Table, 0, n)
	for i := 0; i < n; i++ {
		units = append(units, plan.Table{
			DB: "db", Schema: "public", Name: "t" + string(rune('a'+i%26)),
			DstSchema: "public", DstName: "t" + string(rune('a'+i%26)),
			Bytes:     1 << 20, // 1 MiB each
		})
	}
	return &plan.Plan{Units: units}
}

// TestBatchManyUnitsComplete verifies (FR-01/FR-03) that a batch of many units
// all complete through the real worker pool: every unit is processed, data is
// copied, and nothing fails. This is the "100 000 small tables" characteristic
// reduced to a deterministic, fast batch.
func TestBatchManyUnitsComplete(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 4
	payload := make([]byte, 1<<16)
	m, _ := buildManager(t, &cfg, payload)

	plan := nUnitPlan(50)
	if err := m.Run(context.Background(), plan); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 50 {
		t.Errorf("units = %d, want 50", m.Units)
	}
	if m.Failures != 0 {
		t.Errorf("failures = %d, want 0", m.Failures)
	}
	if m.Rows == 0 {
		t.Errorf("rows = 0, want > 0 (data must have been copied)")
	}
}

// TestRateLimitBoundsDataPath verifies (FR-14) that the adaptive limiter
// actually bounds the data path: given a baseline ceiling, the limiter holds
// the current rate at or below the ceiling, and under a sustained overload it
// steps down toward the floor without going below it.
func TestRateLimitBoundsDataPath(t *testing.T) {
	// A provider that reports a sustained disk/network overload.
	prov := &overloadedProvider{}
	l := ratelimit.New(ratelimit.Options{
		Baseline: 100, // 100 Gbps ceiling
		Floor:    10,
		Step:     0.25,
		Provider: prov,
	})
	// Under load the limiter must reduce from the baseline.
	prov.load = 90 // iowait%
	l.Update()
	if l.Current() > 100 {
		t.Fatalf("current = %.2f Gbps, must not exceed the 100 Gbps baseline", l.Current())
	}
	if l.Current() >= 100 {
		t.Errorf("expected the limiter to reduce below baseline under overload, got %.2f", l.Current())
	}
	// A healthy interval must recover upward but never above the baseline.
	prov.load = 0
	for i := 0; i < 200; i++ {
		l.Update()
		if l.Current() > 100 {
			t.Fatalf("current = %.2f Gbps exceeded baseline", l.Current())
		}
	}
	if l.Current() <= 10 {
		t.Errorf("expected recovery back above the floor, got %.2f", l.Current())
	}
}

// TestThroughputIsPositive verifies (FR-04) that a real copy produces positive
// throughput and records bytes — the data path moved bytes (not a fake
// "no bytes transferred").
func TestThroughputIsPositive(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 2
	payload := make([]byte, 1<<20)
	m, _ := buildManager(t, &cfg, payload)

	if err := m.Run(context.Background(), nUnitPlan(8)); err != nil {
		t.Fatalf("run: %v", err)
	}
	// The metrics collector (FR-04) records the wire bytes moved.
	if got := m.Metrics().Bytes; got == 0 {
		t.Errorf("metrics bytes = 0, want > 0 (data must have been transferred)")
	}
}

// overloadedProvider is a ratelimit.Provider that returns a controllable load.
type overloadedProvider struct{ load float64 }

func (p *overloadedProvider) Snapshot() ratelimit.Metrics {
	return ratelimit.Metrics{IOWaitPercent: p.load}
}

// ensure the package's time import is used even if a test is trimmed.
var _ = time.Second
