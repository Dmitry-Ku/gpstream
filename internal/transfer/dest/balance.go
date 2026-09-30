package dest

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Balance is the F-15 segment-fill balance of one table on the destination
// (spec F-15: 'control of destination segment fill imbalance (metric
// max/avg across segments), warning when the threshold is exceeded'). It is the
// LIVE per-segment metric the CR-5/F-15 acceptance requires — the planner's
// redistribution skew is a pre-copy prediction, this is the measured layout
// of the copied data.
type Balance struct {
	Table     string  `json:"table"`            // schema.table on the destination
	Segments  int     `json:"segments"`         // per-segment row counts observed (0 = non-Greenplum)
	Rows      int64   `json:"rows"`             // total rows counted
	Max       int64   `json:"max"`              // busiest segment
	Avg       float64 `json:"avg"`              // rows / segments
	MaxAvg    float64 `json:"max_avg"`          // Max / Avg (1.0 = perfectly even)
	Threshold float64 `json:"threshold"`        // the --segment-balance-threshold applied
	Warning   bool    `json:"warning"`          // MaxAvg > Threshold (spec: warning when the threshold is exceeded)
	Counts    []int64 `json:"counts,omitempty"` // per-segment rows in gp_segment_id order
}

// WarningText renders the spec-mandated warning line (empty when there is no
// warning).
func (b Balance) WarningText() string {
	if !b.Warning || b.Segments == 0 {
		return ""
	}
	return fmt.Sprintf("segment balance: %s max/avg = %.2f (> threshold %.2f, max=%d avg=%.1f over %d segments)",
		b.Table, b.MaxAvg, b.Threshold, b.Max, b.Avg, b.Segments)
}

// BalanceMaxAvg computes the max/avg segment-fill metric for a table from its
// per-segment row counts (F-15/CR-5). It returns the zero Balance when there
// are no segments (empty table or a cluster that cannot report a layout): the
// caller must then treat the table as non-measurable, not as balanced.
func BalanceMaxAvg(table string, threshold float64, counts []int64) Balance {
	b := Balance{Table: table, Threshold: threshold}
	if len(counts) == 0 {
		return b
	}
	b.Segments = len(counts)
	b.Counts = append([]int64(nil), counts...)
	var max int64
	for _, c := range counts {
		b.Rows += c
		if c > max {
			max = c
		}
	}
	b.Max = max
	b.Avg = float64(b.Rows) / float64(b.Segments)
	if b.Avg > 0 {
		b.MaxAvg = float64(b.Max) / b.Avg
	}
	// The threshold is the operator's --segment-balance-threshold; negative
	// disables the warning gate (like --skew-threshold), so MaxAvg >= 0
	// cannot trip it.
	b.Warning = threshold > 0 && b.MaxAvg > threshold
	return b
}

// SegmentCounts returns the per-segment row counts of a table in
// gp_segment_id order, or (nil, err) when the table has no rows to place or
// the cluster is not Greenplum. A missing table yields a zero count (no
// error): the unit may not have reached the destination yet.
func (a *Agent) SegmentCounts(ctx context.Context, schema, table string) ([]int64, error) {
	// Preflight: resolve the Greenplum catalog by name (safe on plain
	// PostgreSQL — to_regclass returns NULL, not an error), so the main
	// query is only issued where gp_segment_id exists.
	if pg.RowStr(ctx, a.q, "SELECT to_regclass('gp_segment_configuration')::text") == "" {
		return nil, fmt.Errorf("segment balance: not a Greenplum destination (no gp_segment_configuration)")
	}
	segIDs := []int64{}
	segRows, err := a.q.Query(ctx,
		`SELECT DISTINCT gp_segment_id FROM gp_segment_configuration
		 WHERE role = 'p' AND status = 'u' AND content >= 0 ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("segment balance: enumerate segments: %w", err)
	}
	for _, r := range segRows {
		if len(r) == 0 {
			continue
		}
		segIDs = append(segIDs, toInt64(r[0]))
	}
	if len(segIDs) == 0 {
		return nil, fmt.Errorf("segment balance: no up primary segments on the destination")
	}
	// One row per (segment, rows) for the copied table. The identifiers are
	// quoted client-side and bound as $1/$2 (a "%s.%s" format string would
	// only render in the test fake's fmt.Sprintf path; the live pgx driver
	// binds positional parameters and rejects args with no $-placeholders).
	rows, err := a.q.Query(ctx,
		`SELECT gp_segment_id, count(*) FROM $1::regclass GROUP BY gp_segment_id`,
		quoteIdent(schema)+"."+quoteIdent(table))
	if err != nil {
		return nil, fmt.Errorf("segment balance: %s.%s: %w", schema, table, err)
	}
	byID := make(map[int64]int64, len(rows))
	for _, r := range rows {
		if len(r) < 2 {
			continue
		}
		byID[toInt64(r[0])] = toInt64(r[1])
	}
	counts := make([]int64, 0, len(segIDs))
	for _, id := range segIDs {
		counts = append(counts, byID[id])
	}
	return counts, nil
}

// BalanceTable measures the F-15 segment-fill balance of one table on the
// destination. Non-fatal by contract: it returns a zero Balance (Segments=0)
// for a non-Greenplum destination or a measurement fault — the data is
// already in place, the metric is diagnostic (F-04), and a plain-PostgreSQL
// destination has no segments to balance.
func (a *Agent) BalanceTable(ctx context.Context, schema, table string, threshold float64) Balance {
	label := schema + "." + table
	counts, err := a.SegmentCounts(ctx, schema, table)
	if err != nil || len(counts) == 0 {
		return Balance{Table: label, Threshold: threshold}
	}
	return BalanceMaxAvg(label, threshold, counts)
}

// RenderBalance renders the F-15 balance lines for a final report: one line
// per measured table (max/avg and the per-segment counts) plus the warning
// lines for the tables that exceed the threshold (spec: 'warning when
// the threshold is exceeded'). It returns "" when no table was measurable.
func RenderBalance(balances []Balance) string {
	var lines []string
	for _, b := range balances {
		if b.Segments == 0 {
			continue
		}
		lines = append(lines, fmt.Sprintf("  balance %-40s max=%d avg=%.1f max/avg=%.2f (%s)",
			b.Table, b.Max, b.Avg, b.MaxAvg, joinCounts(b.Counts)))
		if b.Warning {
			lines = append(lines, "  WARNING "+b.WarningText())
		}
	}
	return strings.Join(lines, "\n")
}

// joinCounts renders the per-segment counts compactly (4918 5082 = segments
// 0/1 in gp_segment_id order).
func joinCounts(counts []int64) string {
	parts := make([]string, len(counts))
	for i, c := range counts {
		parts[i] = strconv.FormatInt(c, 10)
	}
	return strings.Join(parts, " ")
}

// toInt64 renders an any as an int64 (0 for nil). It mirrors the planner's
// catalog reader (planner.toInt64) so a pg_char/smallint/bigint cell all
// decode the same way.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case int16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	default:
		s := fmt.Sprint(n)
		var out int64
		_, _ = fmt.Sscanf(s, "%d", &out)
		return out
	}
}
