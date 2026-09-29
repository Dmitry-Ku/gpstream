package dest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestBalanceMaxAvgEven verifies a perfectly even fill: max/avg = 1.0, no
// warning at any sane threshold (spec F-15: the metric is max/avg over the
// destination segments).
func TestBalanceMaxAvgEven(t *testing.T) {
	b := BalanceMaxAvg("public.t", 1.5, []int64{5000, 5000})
	if b.Segments != 2 || b.Rows != 10000 || b.Max != 5000 {
		t.Fatalf("even: segs=%d rows=%d max=%d, want 2/10000/5000", b.Segments, b.Rows, b.Max)
	}
	if b.Avg != 5000 || b.MaxAvg != 1.0 {
		t.Fatalf("even: avg=%v max/avg=%v, want 5000/1.0", b.Avg, b.MaxAvg)
	}
	if b.Warning {
		t.Fatal("even fill must not warn")
	}
}

// TestBalanceMaxAvgSkew verifies a skewed fill: the busiest segment holds
// 4000 of 5000 rows, max/avg = 1.6 — above the default 1.5 gate (warning)
// but below a 2.0 gate, and below the negative "gate off" sentinel.
func TestBalanceMaxAvgSkew(t *testing.T) {
	b := BalanceMaxAvg("public.t", 1.5, []int64{4000, 1000})
	if b.Rows != 5000 || b.Max != 4000 || b.Avg != 2500 {
		t.Fatalf("skew: rows=%d max=%d avg=%v, want 5000/4000/2500", b.Rows, b.Max, b.Avg)
	}
	if b.MaxAvg != 1.6 {
		t.Fatalf("skew: max/avg = %v, want 1.6", b.MaxAvg)
	}
	if !b.Warning {
		t.Fatal("skew 1.6 must warn at threshold 1.5")
	}
	if !strings.Contains(b.WarningText(), "max/avg = 1.60") {
		t.Fatalf("warning text = %q, want max/avg = 1.60", b.WarningText())
	}
	// A higher gate does not warn.
	if b2 := BalanceMaxAvg("public.t", 2.0, []int64{4000, 1000}); b2.Warning {
		t.Fatal("skew 1.6 must not warn at threshold 2.0")
	}
	// The negative sentinel disables the gate (metric only).
	if b3 := BalanceMaxAvg("public.t", -1, []int64{4000, 1000}); b3.Warning {
		t.Fatal("negative threshold must disable the warning gate")
	}
}

// TestBalanceMaxAvgEmpty verifies an unmeasurable table (no rows / no
// segments) yields the zero Balance: Segments=0, no warning, empty text.
func TestBalanceMaxAvgEmpty(t *testing.T) {
	b := BalanceMaxAvg("public.t", 1.5, nil)
	if b.Segments != 0 || b.Warning || b.MaxAvg != 0 {
		t.Fatalf("empty: %+v, want zero balance", b)
	}
	if b.WarningText() != "" {
		t.Fatalf("empty must not render a warning, got %q", b.WarningText())
	}
}

// TestBalanceMaxAvgPartial verifies a partially-filled segment set: an empty
// segment counts toward the average (a hot-key table on a 3-segment
// destination can leave a segment empty — that is a real skew signal).
// [100, 100, 0]: total 200, avg 66.67, max 100, max/avg exactly 1.5.
func TestBalanceMaxAvgPartial(t *testing.T) {
	b := BalanceMaxAvg("public.t", 1.5, []int64{100, 100, 0})
	if b.Segments != 3 || b.Max != 100 || b.Rows != 200 || b.Avg != 200.0/3 {
		t.Fatalf("partial: segs=%d max=%d rows=%d avg=%v, want 3/100/200/66.67", b.Segments, b.Max, b.Rows, b.Avg)
	}
	if b.MaxAvg != 1.5 {
		t.Fatalf("partial: max/avg = %v, want 1.5", b.MaxAvg)
	}
	if b.Warning {
		t.Fatal("max/avg exactly at the threshold must not warn (strictly greater)")
	}
}

// gpFake wires a fake destination that reports two up primaries (content
// 0/1) and a per-segment row count for the copied table.
func gpFake(segRows, tableRows [][]any) *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("to_regclass", [][]any{{"gp_segment_configuration"}})
	f.On("DISTINCT gp_segment_id", segRows)
	f.On("GROUP BY gp_segment_id", tableRows)
	return f
}

// TestSegmentCounts verifies the per-segment counts come back in
// gp_segment_id order (a segment the table has no rows in is counted as 0,
// not dropped — the metric must see the whole layout).
func TestSegmentCounts(t *testing.T) {
	f := gpFake(
		[][]any{{int64(0)}, {int64(1)}},
		[][]any{{int64(0), int64(4000)}, {int64(1), int64(1000)}},
	)
	a := mkAgent(f)
	counts, err := a.SegmentCounts(context.Background(), "public", "t")
	if err != nil {
		t.Fatalf("segment counts: %v", err)
	}
	if len(counts) != 2 || counts[0] != 4000 || counts[1] != 1000 {
		t.Fatalf("counts = %v, want [4000 1000]", counts)
	}
	// The count query must target the QUOTED destination name (F-15 runs
	// over the swapped final table).
	last := f.Queried()[len(f.Queried())-1]
	if !strings.Contains(last, `"public"."t"`) || !strings.Contains(last, "GROUP BY gp_segment_id") {
		t.Fatalf("count query = %q, want grouped by gp_segment_id over the final table", last)
	}
}

// TestSegmentCountsMissingSegment verifies a segment with no rows is present
// as 0 (the layout is complete, the fill is not).
func TestSegmentCountsMissingSegment(t *testing.T) {
	f := gpFake(
		[][]any{{int64(0)}, {int64(1)}},
		[][]any{{int64(0), int64(5000)}}, // segment 1 holds no rows
	)
	a := mkAgent(f)
	counts, err := a.SegmentCounts(context.Background(), "public", "t")
	if err != nil {
		t.Fatalf("segment counts: %v", err)
	}
	if len(counts) != 2 || counts[0] != 5000 || counts[1] != 0 {
		t.Fatalf("counts = %v, want [5000 0]", counts)
	}
}

// TestBalanceTable verifies the agent-level metric: a 4000/1000 split over
// two segments is max/avg 1.6 and warns at the default gate.
func TestBalanceTable(t *testing.T) {
	f := gpFake(
		[][]any{{int64(0)}, {int64(1)}},
		[][]any{{int64(0), int64(4000)}, {int64(1), int64(1000)}},
	)
	a := mkAgent(f)
	b := a.BalanceTable(context.Background(), "public", "t", 1.5)
	if b.Segments != 2 || b.Max != 4000 || b.MaxAvg != 1.6 || !b.Warning {
		t.Fatalf("balance = %+v, want 2 segs max=4000 max/avg=1.6 warning", b)
	}
	if b.Table != "public.t" {
		t.Fatalf("table = %q, want public.t", b.Table)
	}
}

// TestBalanceTableNonGreenplum verifies a plain-PostgreSQL destination
// (no gp_segment_configuration) yields the zero Balance, not an error: the
// metric is diagnostic and must degrade silently (F-15 is Greenplum-only).
func TestBalanceTableNonGreenplum(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("to_regclass", [][]any{{nil}}) // not Greenplum
	a := mkAgent(f)
	b := a.BalanceTable(context.Background(), "public", "t", 1.5)
	if b.Segments != 0 || b.Warning {
		t.Fatalf("non-Greenplum balance = %+v, want zero", b)
	}
}

// TestRenderBalance verifies the final-report rendering: one line per
// measured table (max/avg + per-segment counts) and a WARNING line only for
// the tables that breach the threshold.
func TestRenderBalance(t *testing.T) {
	out := RenderBalance([]Balance{
		BalanceMaxAvg("public.ok", 1.5, []int64{5000, 5000}),
		BalanceMaxAvg("public.hot", 1.5, []int64{4000, 1000}),
	})
	if !strings.Contains(out, "public.ok") || !strings.Contains(out, "max/avg=1.00") {
		t.Fatalf("render missing the balanced table:\n%s", out)
	}
	if !strings.Contains(out, "public.hot") || !strings.Contains(out, "max/avg=1.60") {
		t.Fatalf("render missing the skewed table:\n%s", out)
	}
	if !strings.Contains(out, "WARNING") || !strings.Contains(out, "public.hot") {
		t.Fatalf("render missing the warning line:\n%s", out)
	}
	// The per-segment counts must appear in gp_segment_id order.
	if !strings.Contains(out, "(5000 5000)") || !strings.Contains(out, "(4000 1000)") {
		t.Fatalf("render missing the per-segment counts:\n%s", out)
	}
	// An all-unmeasurable set renders nothing.
	if got := RenderBalance([]Balance{{Table: "x", Threshold: 1.5}}); got != "" {
		t.Fatalf("unmeasurable set must render empty, got %q", got)
	}
}
