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
	"sync"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/dest"
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

// Job is one enqueued index-build task (FR-17). Dst is invoked by a pool
// worker after the owning unit has been swapped to its final name; the
// definitions must be ordered secondary-first with PK/unique last (the caller
// is responsible for that order, F-17).
//
// Priority implements --index-priority:
//   - "size": the caller sets Priority to the (estimated) table size in
//     bytes; the pool drains the largest tables first so the longest builds
//     start earliest and the tail of the run is short.
//   - "fifo": Priority is ignored and jobs run in enqueue order.
type Job struct {
	UnitID    uint32
	Dst       *dest.Agent
	Schema    string // destination schema
	Table     string // final (post-swap) table name
	SrcSchema string // source schema (for the name rewrite)
	SrcTable  string // source table name
	Defs      []pg.IndexDef
	Priority  int64 // higher first when mode == "size"
}

// Pool is the background index pool (FR-17): a bounded, prioritized queue of
// index-build jobs consumed by N workers (--index-workers, default 2). Units
// enqueue their indexes immediately after the swap and keep streaming data;
// the pool builds the indexes in the background over the (read-only,
// no-load) final table. GP6 has no CREATE INDEX CONCURRENTLY, so "background"
// means: after the load, while the table is not in use (runbook:
// receiver-side window).
//
// createOneIndex in the destination layer is idempotent (existing index ->
// REINDEX by name), so a process killed in the index phase resumes cleanly
// via a re-enqueued job with no duplicates (F-17 acceptance).
type Pool struct {
	workers  int
	noIndex  bool
	bySize   bool
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []Job
	closed   bool
	started  bool
	firstErr error
	doneJobs int
	done     chan struct{}
	// onProgress, if set, is invoked (outside the pool lock) after every job
	// finishes with the current queue depth and finished-job count. The
	// manager wires this to monitor.Metrics (FR-04/FR-17).
	onProgress func(queueDepth, done int)
}

// NewPool creates a background index pool with the given worker count
// (--index-workers; 0 or negative defaults to 2 per F-17). noIndex is
// --no-indexes: Enqueue then drops every job (skip, not error). bySize
// selects --index-priority: "size" drains the largest tables first,
// "fifo" runs jobs in enqueue order.
func NewPool(workers int, noIndex, bySize bool) *Pool {
	if workers < 1 {
		workers = 2
	}
	p := &Pool{workers: workers, noIndex: noIndex, bySize: bySize}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Enqueue adds an index job to the pool. With --no-indexes the job is
// dropped (FR-17). It is a no-op after Close.
func (p *Pool) Enqueue(j Job) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.noIndex {
		return
	}
	// bySize: binary search into a descending-by-priority order; fifo:
	// append (FIFO).
	if p.bySize {
		i := 0
		for i < len(p.queue) && p.queue[i].Priority >= j.Priority {
			i++
		}
		p.queue = append(p.queue, Job{})
		copy(p.queue[i+1:], p.queue[i:])
		p.queue[i] = j
	} else {
		p.queue = append(p.queue, j)
	}
	p.cond.Signal()
}

// next pops the highest-priority job, blocking until the queue is non-empty
// or the pool is closed.
func (p *Pool) next() (Job, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.queue) == 0 && !p.closed {
		p.cond.Wait()
	}
	if len(p.queue) == 0 {
		return Job{}, false
	}
	j := p.queue[0]
	p.queue = p.queue[1:]
	return j, true
}

// Run starts the workers and blocks until every enqueued job has finished
// (or the pool is closed). It is idempotent. ctx is passed to each job so a
// cancelled run stops issuing new index work.
func (p *Pool) Run(ctx context.Context) {
	p.mu.Lock()
	if p.started {
		started := p.done
		p.mu.Unlock()
		<-started
		return
	}
	p.started = true
	p.done = make(chan struct{})
	p.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, ok := p.next()
				if !ok {
					return
				}

				err := j.Dst.CreateIndexes(ctx, j.Schema, j.Table, j.SrcSchema, j.SrcTable, j.Defs)
				p.mu.Lock()
				p.doneJobs++
				if err != nil && p.firstErr == nil {
					p.firstErr = fmt.Errorf("index pool: unit %d: %w", j.UnitID, err)
				}
				depth, done := len(p.queue), p.doneJobs
				cb := p.onProgress
				p.mu.Unlock()
				if cb != nil {
					cb(depth, done)
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(p.done)
	}()
	<-p.done
}

// SetOnProgress installs the progress callback (see the onProgress field).
// It must be called before Run; workers invoke it after each job.
func (p *Pool) SetOnProgress(f func(queueDepth, done int)) {
	p.mu.Lock()
	p.onProgress = f
	p.mu.Unlock()
}

// Len reports the number of pending jobs (queue depth, for status/metrics).
func (p *Pool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue)
}

// Done reports how many jobs have finished.
func (p *Pool) Done() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.doneJobs
}

// Err returns the first job error (if any).
func (p *Pool) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstErr
}

// Close prevents further Enqueue calls and wakes idle workers so Run can
// drain and return (used when a run aborts before the pool is drained).
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cond.Broadcast()
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
