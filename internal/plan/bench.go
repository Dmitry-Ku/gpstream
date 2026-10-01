package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Bench resolves the read-only source selection for `gpstream bench` (F-14
// input-stream benchmark): the exact table set a `run`/`dry-run` would copy
// under the operator's CLI/env/toml selection (dbs, schemas, tables,
// excludes, rename, metadata/data-only modes), WITHOUT touching the
// destination. The destination is irrelevant to an input-stream benchmark, so
// no destination connection is opened (a --dest flag, if given, is ignored).
//
// The result is the planner's own selection (collectTables + selectUnits +
// applyRename), so the unit set, order (schema, name) and partitioned-parent
// leaf exclusion are byte-identical to a run's plan.
func Bench(ctx context.Context, cfg *config.Config, srcQ pg.Querier) ([]Table, error) {
	if srcQ == nil {
		return nil, fmt.Errorf("bench: no source connection")
	}
	if err := checkAvailability(ctx, srcQ, nil); err != nil {
		return nil, err
	}
	tabs, err := collectTables(ctx, cfg, srcQ)
	if err != nil {
		return nil, err
	}
	tabs = selectUnits(cfg, tabs)
	applyRename(cfg, tabs)
	if len(tabs) == 0 {
		return nil, fmt.Errorf("bench: selection empty (no tables matched --dbs/--schemas/--tables/--exclude)")
	}
	return tabs, nil
}

// BenchSummaryLine renders the one-line volume summary for a bench selection.
func BenchSummaryLine(tabs []Table) string {
	var bytes int64
	n := 0
	for _, t := range tabs {
		bytes += t.Bytes
		if t.Indexes > 0 {
			n++
		}
	}
	return fmt.Sprintf("%d table(s), estimated %s", len(tabs), humanBytesN(bytes))
}

func humanBytesN(b int64) string {
	if b < 0 {
		return "-"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// selectedNames returns the human-readable list of selected tables
// (schema.name, source names), capped for display.
func selectedNames(tabs []Table, capN int) string {
	var sb strings.Builder
	for i, t := range tabs {
		if i >= capN {
			fmt.Fprintf(&sb, " … +%d more", len(tabs)-capN)
			break
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(t.Schema)
		sb.WriteString(".")
		sb.WriteString(t.Name)
	}
	return sb.String()
}
