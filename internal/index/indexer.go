// Package index implements index rebuild (FR-17): after a unit's data is
// copied into the staging table, its indexes are rebuilt so the data is
// indexed before the staging table is renamed to its final name (FR-02/FR-17).
//
// The indexer is decoupled from the job manager (it only needs a streaming
// connection to the destination and the config) so the data path and the index
// path can be developed and tested independently. On vanilla PostgreSQL the
// rebuild is a REINDEX; on Greenplum the same REINDEX re-builds the indexes
// per segment.
package index

import (
	"context"
	"fmt"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Builder rebuilds indexes on tables after a copy (FR-17).
type Builder struct {
	cfg *config.Config
	// workers is the number of index-rebuild workers (FR-17 --index-workers);
	// reserved for parallelism, the core runs sequentially for correctness.
	workers int
}

// New creates an index builder from the config.
func New(cfg *config.Config) *Builder {
	w := cfg.IndexWorkers
	if w < 1 {
		w = 1
	}
	return &Builder{cfg: cfg, workers: w}
}

// Rebuild rebuilds every index on schema.table (FR-17). It is a no-op when
// --no-indexes is set. It issues REINDEX TABLE, which re-creates each index
// over the (already copied) data so the table is fully indexed before the
// atomic rename (FR-02).
func (b *Builder) Rebuild(ctx context.Context, q pg.StreamingQuerier, schema, table string) error {
	if b.cfg.NoIndexes {
		return nil
	}
	// REINDEX TABLE re-builds all indexes on the table (FR-17).
	_, err := q.Exec(ctx, fmt.Sprintf(`REINDEX TABLE %s`, quoteIdent(schema)+"."+quoteIdent(table)))
	if err != nil {
		return fmt.Errorf("reindex %s.%s: %w", schema, table, err)
	}
	return nil
}

// quoteIdent double-quotes a SQL identifier, escaping embedded quotes.
func quoteIdent(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '"')
	return string(out)
}
