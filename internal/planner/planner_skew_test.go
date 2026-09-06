package planner

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/pg"
)

// mkSkewFake returns a fake that answers the distribution-key probe, the
// per-segment size probe (gs-seg-sizes) and the pg_class listing, so the full
// detectRedistribution path (including the measured skew) can be exercised
// (FR-15/CR-5).
func mkSkewFake(hasKey bool, seg []int64) *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{{"s", "orders", "r", int64(1000), int64(0), int64(1), "srcdb"}})
	f.On("gs-redistrib-check", [][]any{{hasKey}})
	if len(seg) > 0 {
		rows := make([][]any, 0, len(seg))
		for _, s := range seg {
			rows = append(rows, []any{s})
		}
		f.On("gs-seg-sizes", rows)
	}
	return f
}

// TestSkewMeasureDifferentLayoutNoSize verifies that a distribution-key
// difference without per-segment size data is reported as a full skew (1) —
// the conservative answer (FR-15/CR-5).
func TestSkewMeasureDifferentLayoutNoSize(t *testing.T) {
	if got := measureSkew("true", "false", nil, nil); got != 1 {
		t.Errorf("skew = %v, want 1 (different layout, no size data)", got)
	}
}

// TestSkewMeasureSameLayout verifies that an identical layout (same key, same
// segment count) is measured as zero skew (no redistribution) — the metric
// CR-5 requires.
func TestSkewMeasureSameLayout(t *testing.T) {
	seg := []int64{10, 20, 30, 40}
	if got := measureSkew("k", "k", seg, seg); got != 0 {
		t.Errorf("skew = %v, want 0 (identical layout)", got)
	}
}

// TestSkewMeasureDifferentLayoutWithSize verifies the actual skew computation
// (CR-5): the measured skew is the fraction of data that must move, computed
// from the per-segment size coincidence of the two layouts.
func TestSkewMeasureDifferentLayoutWithSize(t *testing.T) {
	// source: evenly spread 10/10/10/10 (total 40)
	// dest: all on segment 0, 40/0/0/0 (total 40)
	// overlap = min(10,40)+min(10,0)+min(10,0)+min(10,0) = 10
	// skew = 1 - 10/40 = 0.75
	src := []int64{10, 10, 10, 10}
	dst := []int64{40, 0, 0, 0}
	if got := measureSkew("a", "b", src, dst); got != 0.75 {
		t.Errorf("skew = %v, want 0.75", got)
	}
}

// TestRedistributionSkewWithThreshold verifies the full path (FR-15/CR-5):
// with per-segment size data the plan reports the measured skew and applies the
// --skew-threshold to decide NeedsRedistrib.
func TestRedistributionSkewWithThreshold(t *testing.T) {
	cfg := config.Default()
	cfg.SkewThreshold = 0.05
	// src: even layout, key "true"; dst: different key "false" but sizes all on
	// one segment, so the measured skew is 0.75 (see the helper below).
	src := mkSkewFake(true, []int64{10, 10, 10, 10})
	dst := mkSkewFake(false, []int64{40, 0, 0, 0})
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 40}}

	r := detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	if r.Skew != 0.75 {
		t.Errorf("skew = %v, want 0.75", r.Skew)
	}
	if r.Threshold != 0.05 {
		t.Errorf("threshold = %v, want 0.05", r.Threshold)
	}
	if !r.NeedsRedistrib {
		t.Errorf("expected NeedsRedistrib (skew 0.75 > threshold 0.05)")
	}
}

// TestRedistributionSkewBelowThreshold verifies (CR-5) that a measured skew at
// or below the --skew-threshold is reported as NOT requiring redistribution.
func TestRedistributionSkewBelowThreshold(t *testing.T) {
	cfg := config.Default()
	// A high threshold (0.9) makes the 0.75 skew fall below it.
	cfg.SkewThreshold = 0.9
	src := mkSkewFake(true, []int64{10, 10, 10, 10})
	dst := mkSkewFake(false, []int64{40, 0, 0, 0})
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 40}}

	r := detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	if r.Skew != 0.75 {
		t.Errorf("skew = %v, want 0.75", r.Skew)
	}
	if r.NeedsRedistrib {
		t.Errorf("expected no redistribution (skew 0.75 <= threshold 0.9)")
	}
}

// TestRedistributionSkewBoundary verifies the strict inequality at the
// threshold: skew == threshold is NOT a redistribution.
func TestRedistributionSkewBoundary(t *testing.T) {
	cfg := config.Default()
	cfg.SkewThreshold = 0.75
	src := mkSkewFake(true, []int64{10, 10, 10, 10})
	dst := mkSkewFake(false, []int64{40, 0, 0, 0})
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 40}}

	r := detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	if r.Skew != 0.75 {
		t.Errorf("skew = %v, want 0.75", r.Skew)
	}
	if r.NeedsRedistrib {
		t.Errorf("skew == threshold must not be a redistribution (strict >)")
	}
}
