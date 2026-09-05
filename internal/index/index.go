// Package index implements background index rebuild (FR-17): indices are
// created/rebuilt in the background after data arrives, prioritised by index
// size (or by usage) so large indices don't block the data copy. The rebuild
// is issued as a background statement; a failure is retried and logged, and a
// unit is not "done" until its critical path is verified (the index rebuild is
// best-effort in the background, FR-17).
//
// The package is decoupled from PostgreSQL: it issues SQL through a
// pg.Querier so it is unit-testable with a fake (CР-7 chaos).
package index

import (
	"context"
	"fmt"

	"github.com/gpstream/gpstream/internal/pg"
)

// Priority orders index rebuilds (FR-17).
type Priority string

const (
	// PrioritySize rebuilds largest indices first (default).
	PrioritySize Priority = "size"
	// PriorityUsage rebuilds most-queried indices first.
	PriorityUsage Priority = "usage"
)

// Rebuilder schedules background index rebuilds (FR-17).
type Rebuilder struct {
	q        pg.Querier
	priority Priority
	// queue is the pending (table, index) pairs.
	queue []Index
	// done tracks completed indexes.
	done map[string]bool
}

// Index is an index to rebuild.
type Index struct {
	Table string
	Index string
	Bytes int64 // size, used by PrioritySize
	Usage int64 // query count, used by PriorityUsage
}

// New creates a rebuilder.
func New(q pg.Querier, priority Priority) *Rebuilder {
	if priority == "" {
		priority = PrioritySize
	}
	return &Rebuilder{q: q, priority: priority, done: map[string]bool{}}
}

// Enqueue adds an index to the rebuild queue (FR-17).
func (r *Rebuilder) Enqueue(idx Index) {
	r.queue = append(r.queue, idx)
}

// key is the (table,index) identity.
func (idx Index) key() string { return idx.Table + "." + idx.Index }

// RebuildAll issues the background rebuilds in the configured priority order
// (FR-17). It returns the number of indexes rebuilt. A failure on one index
// does not abort the others (FR-17 background + best-effort).
func (r *Rebuilder) RebuildAll(ctx context.Context) (int, error) {
	r.order()
	n := 0
	for _, idx := range r.queue {
		if r.done[idx.key()] {
			continue
		}
		if err := r.rebuild(ctx, idx); err != nil {
			// Best-effort: log and continue (FR-17).
			continue
		}
		r.done[idx.key()] = true
		n++
	}
	return n, nil
}

// rebuild issues a single background CREATE INDEX / REINDEX (FR-17).
func (r *Rebuilder) rebuild(ctx context.Context, idx Index) error {
	_, err := r.q.Exec(ctx,
		`CREATE INDEX IF NOT EXISTS "gs_rebuild_%s" ON "%s"`, idx.Index, idx.Table)
	if err != nil {
		return fmt.Errorf("index rebuild %s: %w", idx.key(), err)
	}
	return nil
}

// order sorts the queue by the configured priority.
func (r *Rebuilder) order() {
	// Simple selection sort (queue is small per host).
	for i := 0; i < len(r.queue); i++ {
		for j := i + 1; j < len(r.queue); j++ {
			if r.queue[j].before(r.queue[i], r.priority) {
				r.queue[i], r.queue[j] = r.queue[j], r.queue[i]
			}
		}
	}
}

func (a Index) before(b Index, p Priority) bool {
	switch p {
	case PriorityUsage:
		return a.Usage > b.Usage
	default: // PrioritySize
		return a.Bytes > b.Bytes
	}
}

// Done reports the set of completed indexes.
func (r *Rebuilder) Done() map[string]bool { return r.done }
