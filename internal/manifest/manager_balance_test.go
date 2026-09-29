package manifest

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// newManagerGP builds a Manager whose DESTINATION fake reports a 2-segment
// Greenplum layout (F-15): the run's balance measurement (measureBalance)
// must see the layout and the per-segment fill of the copied table. The
// source stays a plain-PG fake (the balance is a destination metric).
func newManagerGP(t *testing.T, cfg *config.Config, payload []byte) (*Manager, string) {
	t.Helper()
	src, dst := newPair(t, payload, payload)
	// Greenplum destination: the catalog resolves (F-15 preflight), two up
	// primaries, and the copied table holds 80/20 rows over the segments —
	// max/avg = 80/50 = 1.6, above the default 1.5 gate (warning).
	dst.On("to_regclass", [][]any{{"gp_segment_configuration"}})
	dst.On("DISTINCT gp_segment_id", [][]any{{int64(0)}, {int64(1)}})
	dst.On("GROUP BY gp_segment_id", [][]any{{int64(0), int64(80)}, {int64(1), int64(20)}})
	dir := t.TempDir()
	j := NewJournal(pg.NewFakeConn(), filepath.Join(dir, "journal.jsonl"))
	return NewManager(cfg, src, dst, j), dir
}

// TestManagerRunEmitsSegmentBalance verifies the F-15 live metric: after a
// successful run on a Greenplum destination, the copied table's per-segment
// fill is measured (max/avg over gp_segment_id), recorded in events.jsonl as
// a segment_balance event (F-04), and surfaced in Stats.Balances for the
// final report — with the warning flag set (80/20 = max/avg 1.6 > 1.5).
func TestManagerRunEmitsSegmentBalance(t *testing.T) {
	cfg := config.Default()
	m, dir := newManagerGP(t, &cfg, []byte("data"))

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// Stats: the run measured exactly one table, with the expected fill.
	if len(m.Balances) != 1 {
		t.Fatalf("balances = %d, want 1: %+v", len(m.Balances), m.Balances)
	}
	b := m.Balances[0]
	if b.Table != "public.orders" || b.Segments != 2 || b.Max != 80 {
		t.Fatalf("balance = %+v, want public.orders 2 segs max=80", b)
	}
	if b.MaxAvg != 1.6 {
		t.Fatalf("max/avg = %v, want 1.6", b.MaxAvg)
	}
	if !b.Warning {
		t.Fatal("max/avg 1.6 must warn at the default 1.5 threshold")
	}
	// events.jsonl: the F-04 diagnostic event carries the metric.
	evs := eventsOf(t, dir)
	bal := findEvent(t, evs, "segment_balance")
	if len(bal) != 1 {
		t.Fatalf("got %d segment_balance events, want 1: %v", len(bal), evs)
	}
	if bal[0]["table"] != "public.orders" || bal[0]["warning"] != true || bal[0]["max_avg"] != 1.6 {
		t.Fatalf("event = %v, want table=public.orders warning=true max_avg=1.6", bal[0])
	}
}

// TestManagerRunNoBalanceOnPlainPG verifies the metric degrades silently on a
// non-Greenplum destination (no gp_segment_configuration): the run succeeds,
// no balance is recorded and no segment_balance event is emitted — F-15 is a
// Greenplum metric and must not be a failure on plain PostgreSQL.
func TestManagerRunNoBalanceOnPlainPG(t *testing.T) {
	cfg := config.Default()
	m, dir := newManagerAt(t, &cfg, []byte("data"), []byte("data"))

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(m.Balances) != 0 {
		t.Fatalf("balances = %+v, want none on a plain-PG destination", m.Balances)
	}
	evs := eventsOf(t, dir)
	if bal := findEvent(t, evs, "segment_balance"); len(bal) != 0 {
		t.Fatalf("got %d segment_balance events on plain PG, want 0: %v", len(bal), bal)
	}
}

// TestManagerBalanceHonoursThreshold verifies the operator's
// --segment-balance-threshold gates the warning: the same 80/20 fill warns
// at 1.5 but not at 2.0 (the metric is still recorded either way — the
// warning is the gate, not the measurement).
func TestManagerBalanceHonoursThreshold(t *testing.T) {
	cfg := config.Default()
	cfg.SegmentBalanceThreshold = 2.0
	m, _ := newManagerGP(t, &cfg, []byte("data"))

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(m.Balances) != 1 {
		t.Fatalf("balances = %d, want 1 (metric recorded regardless of gate)", len(m.Balances))
	}
	if b := m.Balances[0]; b.Warning {
		t.Fatalf("balance = %+v, want NO warning at threshold 2.0 (max/avg 1.6 < 2.0)", b)
	}
	if b := m.Balances[0]; b.Threshold != 2.0 {
		t.Fatalf("threshold = %v, want 2.0", b.Threshold)
	}
}
