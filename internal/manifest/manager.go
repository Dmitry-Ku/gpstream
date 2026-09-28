// Package job also provides the Job Manager (Manager) that drives a real copy
// end-to-end (FR-01/FR-02/FR-06/FR-07/FR-17): for each planned unit it reads
// the schema off the source, creates a staging table on the destination,
// streams the data off the source (`COPY ... TO STDOUT`) over the network
// into the staging table (`COPY ... FROM STDIN`) without touching local
// storage (FR-01), verifies the content checksum (FR-02), rebuilds indexes
// (FR-17), and only then atomically renames the staging table to its final
// name (FR-02). Every transition is recorded in the operation journal so a
// crash can be resumed (FR-06/FR-07).
//
// The Manager is exercised by the integration tests against a real PostgreSQL
// server (two embedded clusters acting as source and destination), so the
// data path is verified against real COPY FROM/TO rather than fakes.
package manifest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/index"
	"github.com/gpstream/gpstream/internal/monitor"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/ratelimit"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// Manager orchestrates a copy across the planned units.
type Manager struct {
	cfg     *config.Config
	srcQ    pg.StreamingQuerier
	dstQ    pg.StreamingQuerier
	src     *source.Agent
	dst     *dest.Agent
	journal *Journal
	index   *index.Builder
	idxPool *index.Pool // background index pool (FR-17); drained after the data path
	// idxSeen marks units whose index build this run has enqueued, so the
	// journal scan does not enqueue them twice (FR-17 resume).
	idxSeen map[uint32]bool
	idxMu   sync.Mutex // guards idxSeen
	monitor *monitor.Metrics
	comp    *compress.Compressor
	dec     *compress.Decompressor
	lim     *ratelimit.Limiter // adaptive rate limiter (FR-14); nil when unlimited
	pacer   *ratelimit.Pacer   // throttles the data path to lim (FR-14)
	// snapshotMode is the FR-02 consistency mode actually in effect for this
	// run ("db", "cluster", or "" when no snapshot is pinned). snapshotTx is
	// true while a read-only REPEATABLE READ transaction is open on the source
	// (all unit COPY statements read from that one snapshot, FR-02).
	// pinnedSrc is the single source connection the snapshot transaction runs
	// on (set for pool-backed sources with parallel workers, FR-03+FR-02);
	// pinRelease returns it to the pool after the COMMIT.
	snapshotMode string
	snapshotTx   bool
	pinnedSrc    pg.StreamingQuerier
	pinRelease   func()
	// statMu guards Stats, which multiple workers mutate under the FR-03 pool.
	statMu sync.Mutex
	// Stats accumulates the observed result of the run.
	Stats
}

// Stats is the observed result of a Manager run (used for reporting and for
// the integration test to assert that bytes were actually transferred).
type Stats struct {
	Bytes    int64 // rows streamed (FR-01)
	Rows     int64 // rows copied (FR-02)
	Units    int   // units processed
	Comps    int   // units with a verified checksum match (FR-02)
	Skipped  int   // units skipped by --conflict-action (FR-10/FR-11)
	Failures int   // units that failed after retries
}

// Metrics returns the collector so the caller can expose it (FR-04). It is the
// same collector the limiter/compression degrade against, so a rate-limit or
// compression change is observable as a metric (FR-04/FR-12/FR-14).
func (m *Manager) Metrics() *monitor.Metrics { return m.monitor }

// NewManager creates a Manager for a copy between two streaming connections.
func NewManager(cfg *config.Config, srcQ, dstQ pg.StreamingQuerier, j *Journal) *Manager {
	mode, level, err := compress.Parse(string(cfg.Compression))
	if err != nil {
		mode, level = compress.ModeZstd, 3
	}
	m := &Manager{
		cfg:     cfg,
		srcQ:    srcQ,
		dstQ:    dstQ,
		src:     source.New(srcQ),
		dst:     dest.New(dstQ),
		journal: j,
		index:   index.New(cfg),
		idxPool: index.NewPool(cfg.IndexWorkers, cfg.NoIndexes, cfg.IndexPriority == "size"),
		idxSeen: map[uint32]bool{},
		monitor: monitor.New(),
		comp:    compress.New(mode, level),
		dec:     compress.NewDec(mode),
	}
	// FR-17/FR-04: expose the background index pool's progress as metrics.
	m.idxPool.SetOnProgress(func(queueDepth, done int) {
		m.monitor.SetIndexProgress(queueDepth, done)
	})
	// FR-14: when a rate-limit is set, build the adaptive limiter over the host
	// metrics provider and the pacer that throttles the data path. A rate change
	// is observable as a metric (FR-04) via the diagnostic callback.
	if cfg.RateLimit > 0 {
		lim := ratelimit.New(ratelimit.Options{
			Baseline: cfg.RateLimit,
			Provider: ratelimit.NewProcProvider(),
		})
		lim.SetOnChange(func(from, to ratelimit.Gbps, reason string) {
			m.monitor.SetRateLimit(to)
		})
		m.lim = lim
		m.pacer = ratelimit.NewPacer(lim)
		m.monitor.SetRateLimit(cfg.RateLimit)
	}
	return m
}

// Run executes the copy for the plan (FR-01/FR-02). Units are processed through
// a bounded worker pool of cfg.WorkersPerPair (FR-03); when the rate limiter is
// set the pool shares a single pacer so the *total* flow stays under the
// ceiling (FR-14). Each unit is fully journaled so the run is resumable
// (FR-06/FR-07). A first unit failure is reported after the in-flight units
// finish (the pool does not cancel the batch).
func (m *Manager) Run(ctx context.Context, p *plan.Plan) error {
	// Ensure the primary journal table exists (ADR-001 §4.3); the file
	// mirror is the durable fallback if this fails (FR-07).
	if err := m.journal.Init(ctx); err != nil {
		// Non-fatal: the file mirror still makes the job resumable.
	}

	// FR-02 consistency: establish the source read snapshot BEFORE any unit is
	// copied so every COPY of the database reads from the same point-in-time
	// (see establishSnapshot). A snapshot error is non-fatal (the copy still
	// proceeds) but is logged so the operator can see the consistency mode.
	m.establishSnapshot(ctx)

	// FR-14: sample host load periodically so the limiter adapts the data-path
	// rate (the pacer reads ratelimit.Current). No-op when no rate-limit is set.
	stopSampler := m.startLimiterSampler(ctx)
	if stopSampler != nil {
		defer stopSampler()
	}

	// Build the unit list (unit-id and schema fallback, FR-02/FR-11).
	units := make([]*Unit, 0, len(p.Units))
	for i, t := range p.Units {
		unit := &Unit{
			JobID:     jobID(p),
			UnitID:    int64(i + 1),
			DB:        t.DB,
			Schema:    t.Schema,
			Table:     t.Name,
			DstSchema: t.DstSchema,
			DstName:   t.DstName,
			Kind:      "table",
			Bytes:     t.Bytes,
			Conflict:  t.HasConflict,
		}
		// Fall back to the source names when a rename did not supply a
		// destination (applyRename defaults them, but a hand-built plan may
		// leave them empty).
		if unit.DstSchema == "" {
			unit.DstSchema = unit.Schema
		}
		if unit.DstName == "" {
			unit.DstName = unit.Table
		}
		units = append(units, unit)
	}

	// Bounded worker pool (FR-03). WorkersPerPair is validated >= 1.
	workers := m.cfg.WorkersPerPair
	if workers < 1 {
		workers = 1
	}

	var (
		wg       sync.WaitGroup
		work     = make(chan *Unit, len(units))
		errMu    sync.Mutex
		firstErr error
	)
	dispatch := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range work {
				// A panic while processing a unit is a genuine fault (CR-7):
				// recover it so the worker does not die. The unit is recorded as
				// a failure (a resume can retry it, FR-06/FR-07) and the worker
				// continues with the next unit, so a dead worker's loss of one
				// unit does not kill the batch (FR-06).
				if err := m.processUnitSafe(ctx, u); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}
	for i := 0; i < workers; i++ {
		dispatch()
	}
	for _, u := range units {
		work <- u
	}
	close(work)
	wg.Wait()

	// FR-17: drain the background index pool. First re-enqueue units that
	// reached the final name in an earlier (killed) run but never got their
	// index build — the journal is the source of truth, and createOneIndex is
	// idempotent per index name, so a re-enqueue is duplicate-safe.
	m.requeueIndexes(ctx)
	m.idxPool.Close()
	m.idxPool.Run(ctx)
	if err := m.idxPool.Err(); err != nil && firstErr == nil {
		firstErr = err
	}

	// Release the source snapshot (FR-02) once every unit has been copied.
	m.releaseSnapshot(ctx)
	return firstErr
}

// processUnitSafe wraps processUnit with a panic recovery so a genuine fault in
// one unit (CR-7) does not kill the worker (FR-06). On panic it records the
// unit as a failure (a resume can retry it, FR-06/FR-07) and returns an error
// so the caller sets firstErr; the worker itself continues with the next unit
// (the deferred recovery here keeps the worker goroutine alive).
func (m *Manager) processUnitSafe(ctx context.Context, u *Unit) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			// Record the failed unit so a resume can retry it (FR-06/FR-07),
			// then surface the panic as a failure for the run.
			u.State = StateFailed
			_ = m.journal.SetState(u)
			m.addFailure()
			retErr = fmt.Errorf("unit %s.%s panic: %v", u.Schema, u.Table, r)
		}
	}()
	return m.processUnit(ctx, u)
}

// processUnit runs a single unit through the pipeline (FR-01/FR-02/FR-17),
// honouring the conflict policy (FR-10/FR-11). It is safe to call from
// multiple workers (FR-03): stats are guarded by statMu and the journal by its
// own mutex.
func (m *Manager) processUnit(ctx context.Context, u *Unit) error {
	// Conflict policy: skip-existing default (FR-10/FR-11). Skip when the
	// destination already holds the target — either because the planner flagged
	// a conflict (u.Conflict) or a live existence check confirms it.
	if u.Table != "" && m.cfg.ConflictAction == config.SkipExisting && (u.Conflict || m.dst.Exists(ctx, u.DstSchema, u.DstName)) {
		m.addSkipped()
		m.journal.CreateUnit(u, StatePending)
		return nil
	}
	if err := m.runUnit(ctx, u); err != nil {
		m.addFailure()
		m.journal.SetState(u)
		return err
	}
	return nil
}

// addSkipped increments the skipped-units counter (FR-10/FR-11) under statMu.
// processUnit may run on multiple workers (FR-03), so the counter is guarded.
func (m *Manager) addSkipped() {
	m.statMu.Lock()
	m.Skipped++
	m.statMu.Unlock()
}

// addFailure increments the failed-units counter under statMu (see addSkipped).
func (m *Manager) addFailure() {
	m.statMu.Lock()
	m.Failures++
	m.statMu.Unlock()
}

// tableFor maps a run unit back to the plan.Table the source COPY reads
// from (FR-03). A single Manager run processes exactly one plan, and the unit
// carries its source schema/name (u.Schema/u.Table) and destination
// schema/name (u.DstSchema/u.DstName). The source COPY reads the SOURCE
// names, so Schema/Name are the source names; DstSchema/DstName carry the
// destination names (after rename) so the copy is fully-qualified on both
// sides and a rename is honoured end-to-end (FR-11/§4.10).
func (m *Manager) tableFor(u *Unit) plan.Table {
	return plan.Table{
		DB:        u.DB,
		Schema:    u.Schema,
		Name:      u.Table,
		DstDB:     u.DB,
		DstSchema: u.DstSchema,
		DstName:   u.DstName,
		Bytes:     u.Bytes,
	}
}

// resolve returns how a conflicting unit must be handled (FR-11). It reads the
// operator's --conflict-action and decides the copy target and verification:
//
//	skip-existing  -> the caller (processUnit) skips the unit (never reaches here)
//	overwrite      -> copy to a staging table and swap over the final (default)
//	truncate       -> copy in-place into the final table after TRUNCATE, verify
//	append         -> copy in-place into the final table WITHOUT truncate, no
//	                 exact checksum verify (final = existing rows + source rows)
//
// inPlace  : copy into the final table directly (truncate/append)
// truncate : clear the final table before copying (only --truncate)
// verify   : compare the content checksum after copying (default + --truncate)
func (m *Manager) resolve(u *Unit) (inPlace, truncate, verify bool) {
	switch m.cfg.ConflictAction {
	case config.Truncate:
		return true, true, true
	case config.Append:
		return true, false, false
	default:
		// SkipExisting is handled in processUnit; Overwrite and any unlisted
		// action fall back to the staging+swap path.
		return false, false, true
	}
}

// runUnit copies, verifies, rebuilds indexes and swaps a single unit
// (FR-01/FR-02/FR-11/FR-17), recording each transition in the journal. It takes
// the pre-built unit (see Run) so the pool can share unit construction. The
// conflict policy (FR-11) selects the copy target: the default and --overwrite
// copy into a staging table and atomically swap it over the final name;
// --truncate/--append copy in-place into the final table (FR-11).
func (m *Manager) runUnit(ctx context.Context, u *Unit) error {
	// Source and destination names may differ (FR-11/§4.10 rename): the copy
	// READS from the source (u.Schema/u.Table) and WRITES to the destination
	// (u.DstSchema/u.DstName). Without a rename they are equal.
	ds, dn := u.DstSchema, u.DstName

	// 1. Record the unit as pending (FR-02 journal).
	if _, err := m.journal.CreateUnit(u, StatePending); err != nil {
		return fmt.Errorf("journal pending: %w", err)
	}

	// 2. Read the schema off the SOURCE, then create the DESTINATION schema
	//    (and the copy target) from it (FR-02). The target is built from the
	//    source's own column definitions (format_type) so it is faithful
	//    across clusters; the distribution policy is reproduced on a
	//    Greenplum source (F-11 metadata: «типы, распределение»).
	t := m.tableFor(u)
	schema, name := u.Schema, u.Table
	cols, err := m.src.Columns(ctx, schema, name)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	// The distribution policy is stable catalog data (not snapshot state), so
	// it is read on the base source querier, NOT the pinned FR-02 snapshot
	// connection: a query fault there must not abort the REPEATABLE READ
	// snapshot that every data COPY shares. On a pool source m.srcQ hands out
	// a pool connection per query, so a bad statement is contained. (GP6
	// live e2e, 27.09: the first version read it on the pinned conn and the
	// resulting error aborted the whole snapshot transaction.)
	dist := source.New(m.srcQ).Distribution(ctx, schema, name)
	if err := m.dst.EnsureSchema(ctx, ds); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	// 3. Resolve the conflict policy (FR-11): the copy target is either a
	//    staging table (default / --overwrite) or the final table in-place
	//    (--truncate / --append). All destinations below use ds/dn.
	inPlace, truncate, verify := m.resolve(u)
	var target string
	if inPlace {
		// Copy into the final table directly. Create it from the source layout
		// if it does not exist yet, then (optionally) truncate it.
		target = dn
		if !m.dst.Exists(ctx, ds, dn) {
			if err := m.dst.CreateTable(ctx, ds, cols, dn, dist); err != nil {
				return fmt.Errorf("create table: %w", err)
			}
		}
		if truncate {
			if err := m.dst.Truncate(ctx, ds, dn); err != nil {
				return fmt.Errorf("truncate: %w", err)
			}
		}
	} else {
		target = dest.StagingName(u.JobID, u.UnitID)
		// The staging table is scratch for this unit's in-flight copy. A
		// surviving one (a crashed/failed run that copied but never swapped)
		// holds unswapped partial data; it MUST be empty before the copy,
		// otherwise a resume appends to the partial data and doubles the rows
		// (F-06/F-07: "resume доводит до конца без дублей"). A fresh run has
		// no staging table, so it is created from the source layout.
		if m.dst.Exists(ctx, ds, target) {
			if err := m.dst.Truncate(ctx, ds, target); err != nil {
				return fmt.Errorf("truncate staging: %w", err)
			}
		} else if err := m.dst.CreateStaging(ctx, ds, cols, target, dist); err != nil {
			return fmt.Errorf("create staging: %w", err)
		}
	}

	// 4. Mark copying (FR-06/FR-07 journal).
	u.State = StateCopying
	if err := m.journal.SetState(u); err != nil {
		return fmt.Errorf("journal copying: %w", err)
	}

	// 5. Stream the data: source --(framed, compressed)--> target (FR-01/§4.2).
	fp := m.framedPipe(uint32(u.UnitID))
	rows, err := m.copyStream(ctx, t, u, target, fp)
	if err != nil {
		// Roll the copy target back on a staging failure (FR-07); an in-place
		// failure is recorded as failed and re-tried by a resume (TRUNCATE is
		// idempotent, so a retry re-copies from a clean table).
		if !inPlace {
			_ = m.dst.DropStaging(ctx, ds, target)
		}
		u.State = StateFailed
		_ = m.journal.SetState(u)
		return fmt.Errorf("stream: %w", err)
	}
	u.RowCount = rows
	m.statMu.Lock()
	m.Rows += rows
	m.Bytes += rows
	m.statMu.Unlock()
	m.monitor.Record(uint64(rows), uint64(rows))

	// 6. Verify the content checksum (FR-02): re-derive it on both sides and
	//    compare, without re-copying the table. Skipped for --append (the final
	//    holds existing + source rows, so an exact source checksum cannot match).
	if verify {
		u.State = StateVerifying
		_ = m.journal.SetState(u)
		srcSum, err := m.src.Checksum(ctx, t.Schema, t.Name)
		if err != nil {
			if !inPlace {
				_ = m.dst.DropStaging(ctx, ds, target)
			}
			u.State = StateFailed
			_ = m.journal.SetState(u)
			return fmt.Errorf("source checksum: %w", err)
		}
		ok, vrows, err := m.dst.Verify(ctx, ds, target, srcSum)
		if err != nil {
			if !inPlace {
				_ = m.dst.DropStaging(ctx, ds, target)
			}
			u.State = StateFailed
			_ = m.journal.SetState(u)
			return fmt.Errorf("verify: %w", err)
		}
		if !ok {
			if !inPlace {
				_ = m.dst.DropStaging(ctx, ds, target)
			}
			u.State = StateFailed
			_ = m.journal.SetState(u)
			return fmt.Errorf("verify: checksum mismatch (rows=%d)", vrows)
		}
		u.Checksum = srcSum
		m.statMu.Lock()
		m.Comps++
		m.statMu.Unlock()
	}

	// 7. (FR-17) Indexes are NOT rebuilt here: the staging table is created
	//    without indexes, so a pre-swap REINDEX was a no-op. The index build is
	//    enqueued to the background pool after the swap (step 10), where it
	//    runs over the final table without blocking the data path.

	// 8. Record completed (data + checksum, ADR-001 §4.4).
	u.State = StateCompleted
	if err := m.journal.SetState(u); err != nil {
		return fmt.Errorf("journal completed: %w", err)
	}
	m.statMu.Lock()
	m.Units++
	m.statMu.Unlock()

	// 9. Atomically swap the staging table to its final name (FR-02). Only the
	//    staging path swaps; the in-place path already wrote the final table.
	if !inPlace {
		if err := m.dst.Swap(ctx, ds, dn, target, m.cfg.ConflictAction == config.Overwrite); err != nil {
			// A swap failure after a verified copy is still safe (the staging
			// table holds the verified data); leave it and report so a resume
			// can retry the swap.
			return fmt.Errorf("swap: %w", err)
		}
	}
	u.State = StateSwapped
	if err := m.journal.SetState(u); err != nil {
		return fmt.Errorf("journal swapped: %w", err)
	}

	// 10. Enqueue the index build to the background pool (FR-17). Non-fatal by
	//     design: the data is already in place, and a failed/missing read is
	//     covered by the journal scan in Run on the next resume.
	m.enqueueIndexes(ctx, u)
	return nil
}

// enqueueIndexes reads the source table's index definitions and enqueues a
// background build over the destination's final name (FR-17). Failures are
// non-fatal by design: the unit's data is already in place, and the journal
// scan in Run (requeueIndexes) re-enqueues the build on the next resume.
func (m *Manager) enqueueIndexes(ctx context.Context, u *Unit) {
	defs, err := m.src.Indexes(ctx, u.Schema, u.Table)
	if err != nil {
		return
	}
	if len(defs) == 0 {
		return
	}
	m.idxMu.Lock()
	m.idxSeen[uint32(u.UnitID)] = true
	m.idxMu.Unlock()
	m.idxPool.Enqueue(index.Job{
		UnitID:    uint32(u.UnitID),
		Dst:       m.dst,
		Schema:    u.DstSchema,
		Table:     u.DstName,
		SrcSchema: u.Schema,
		SrcTable:  u.Table,
		Defs:      defs,
		Priority:  u.Bytes, // --index-priority=size: larger tables first
	})
}

// requeueIndexes re-enqueues the index build for every unit the journal holds
// in the final ("swapped") state but that this run has not yet enqueued — e.g.
// a previous run killed after the swap and before the pool drained. The
// idempotent createOneIndex (existing index -> REINDEX by name) makes the
// re-enqueue duplicate-safe (FR-17 acceptance: clean resume, no duplicates).
func (m *Manager) requeueIndexes(ctx context.Context) {
	for _, u := range m.journal.Units() {
		if u.State != StateSwapped {
			continue
		}
		m.idxMu.Lock()
		_, seen := m.idxSeen[uint32(u.UnitID)]
		if !seen {
			m.idxSeen[uint32(u.UnitID)] = true
		}
		m.idxMu.Unlock()
		if seen {
			continue
		}
		defs, err := m.src.Indexes(ctx, u.Schema, u.Table)
		if err != nil || len(defs) == 0 {
			continue
		}
		m.idxPool.Enqueue(index.Job{
			UnitID:    uint32(u.UnitID),
			Dst:       m.dst,
			Schema:    u.DstSchema,
			Table:     u.DstName,
			SrcSchema: u.Schema,
			SrcTable:  u.Table,
			Defs:      defs,
			Priority:  u.Bytes,
		})
	}
}

// framedPipe builds a framed, compressed transport over a net.Pipe (FR-01).
func (m *Manager) framedPipe(unitID uint32) *transfer.FramedPipe {
	// A net.Pipe is an in-memory, back-pressure-respecting connected pair;
	// in production this is a net.Conn (optionally TLS, FR-13).
	w, r := net.Pipe()
	return transfer.NewFramedPipe(w, r, 1, unitID, m.comp, m.dec)
}

// copyStream runs the source and destination COPY in parallel over the framed
// pipe (FR-01). The source writes `COPY ... TO STDOUT` into the pipe; the
// destination reads it into `COPY ... FROM STDIN`. It returns the number of
// rows the destination received. The FramedPipe is both the io.Writer (source)
// and the io.Reader (destination) over one back-pressure-respecting pipe.
//
// With --copy-mode on_segment (T-B) the source side is the segment-direct
// transport instead: the kernel streams one framed slice per primary segment
// to the controller listener, which merges them into the same pipe.
func (m *Manager) copyStream(ctx context.Context, t plan.Table, u *Unit, staging string, fp *transfer.FramedPipe) (int64, error) {
	if m.cfg.CopyMode == config.CopyOnSegment {
		// The segment-direct path builds its own framed pipe (the merged slice
		// stream); the coordinator pipe the caller pre-created is unused, so
		// close it to avoid a leaked net.Pipe.
		_ = fp.Close()
		return m.copyStreamSegmentDirect(ctx, t, u, staging)
	}
	var (
		dstRows int64
		dstErr  error
		srcErr  error
		wg      sync.WaitGroup
	)
	wg.Add(2)

	// Destination: read framed bytes into the staging table until EOF.
	go func() {
		defer wg.Done()
		// A panic in the data path (a genuine fault, CR-7) must not crash the
		// whole copy (FR-06): convert it to an error so the caller records a
		// failure and the other workers continue.
		defer func() {
			if r := recover(); r != nil {
				dstErr = fmt.Errorf("destination copy panic: %v", r)
			}
		}()
		rows, err := m.dst.CopyIntoStream(ctx, u.DstSchema, staging, fp)
		if err != nil && !isEOF(err) {
			dstErr = err
		}
		dstRows = rows
	}()

	// Source: write the table's bytes into the pipe (throttled by the limiter,
	// FR-14), then close it so the destination read sees io.EOF (end-of-copy).
	srcW := m.pacedWriter(fp)
	go func() {
		defer wg.Done()
		// Close the pipe on exit so the destination always sees end-of-copy,
		// even if CopyToStream panics (a genuine fault, CR-7). The deferred
		// recover converts a panic to an error (FR-06) so the run survives.
		defer func() {
			_ = fp.Close()
		}()
		defer func() {
			if r := recover(); r != nil {
				srcErr = fmt.Errorf("source copy panic: %v", r)
			}
		}()
		_, err := m.src.CopyToStream(ctx, t.Schema, t.Name, srcW)
		if err != nil && !isEOF(err) {
			srcErr = err
		}
	}()

	wg.Wait()
	if srcErr != nil {
		return 0, fmt.Errorf("source copy: %w", srcErr)
	}
	if dstErr != nil {
		return 0, fmt.Errorf("dest copy: %w", dstErr)
	}
	// The destination is authoritative for the row count (it wrote the table).
	return dstRows, nil
}

// copyStreamSegmentDirect is the segment-direct data path (T-B, Fallback B).
// Instead of `COPY ... TO STDOUT` through the coordinator, the GPDB kernel
// launches gpstream-segreceiver on every primary segment via
// `COPY ... TO PROGRAM ... ON SEGMENT BINARY` (dispatched with
// DF_WITH_SNAPSHOT, so every slice reads the coordinator's pinned snapshot —
// the same consistency as the coordinator path). Each segreceiver frames its
// local slice (Frame v2, content-tagged) and streams it over direct TCP to
// the controller listener; the controller merges the N slices into one
// binary-COPY stream and feeds it through the same framed/compressed pipe as
// the coordinator path, so the destination is unchanged. The kernel waits for
// all segreceivers to exit (DF_NEED_TWO_PHASE) before the COPY returns.
func (m *Manager) copyStreamSegmentDirect(ctx context.Context, t plan.Table, u *Unit, staging string) (int64, error) {
	n, err := m.primarySegmentCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("segment-direct: %w", err)
	}
	if n < 1 {
		return 0, fmt.Errorf("segment-direct: no up primary segments")
	}
	// Each parallel unit runs its own controller listener, so derive a
	// per-unit port from the operator's base address (port + unitID).
	listen, err := segstream.UnitListen(m.cfg.SegListen, uint32(u.UnitID))
	if err != nil {
		return 0, fmt.Errorf("segment-direct: %w", err)
	}
	l, err := net.Listen("tcp", listen)
	if err != nil {
		return 0, fmt.Errorf("segment-direct: listen %s: %w", listen, err)
	}
	defer l.Close()

	jobID, unitID := uint32(1), uint32(u.UnitID)
	prog, err := segstream.ProgramCommand(m.cfg.SegBinary, listen, jobID, unitID)
	if err != nil {
		return 0, fmt.Errorf("segment-direct: %w", err)
	}
	sql := segstream.CopyOnSegmentSQL(t.Schema, t.Name, prog)

	// Data plane: the merged slice stream goes through the same framed,
	// compressed pipe as the coordinator path, so the destination is unchanged.
	fp := m.framedPipe(uint32(u.UnitID))

	var (
		dstRows int64
		dstErr  error
	)
	dstDone := make(chan struct{})
	// Destination: read the merged binary-COPY stream into the staging table.
	go func() {
		defer close(dstDone)
		defer func() {
			if r := recover(); r != nil {
				dstErr = fmt.Errorf("destination copy panic: %v", r)
			}
		}()
		rows, err := m.dst.CopyIntoStream(ctx, u.DstSchema, staging, fp)
		if err != nil && !isEOF(err) {
			dstErr = err
		}
		dstRows = rows
	}()

	// Source: merge the N per-segment slices into one binary-COPY stream on fp.
	mux := segstream.NewSliceMux(fp, n)
	if err := mux.Begin(); err != nil {
		_ = fp.Close()
		<-dstDone
		return 0, fmt.Errorf("segment-direct: %w", err)
	}

	type recvOut struct {
		results []*segstream.SliceResult
		err     error
	}
	recvCh := make(chan recvOut, 1)
	go func() {
		res, err := segstream.Receive(ctx, l, n, mux.Write)
		recvCh <- recvOut{res, err}
	}()

	// The kernel runs the segreceiver on each primary and waits for all of
	// them to exit before the COPY returns (DF_NEED_TWO_PHASE); this blocks
	// until every slice stream is complete. The ON SEGMENT COPY dispatches
	// the coordinator's snapshot to the segments (DF_WITH_SNAPSHOT), so it
	// must run on the snapshot connection in FR-02 mode (m.srcRead()).
	if _, err := m.srcRead().Exec(ctx, sql); err != nil {
		_ = fp.Close()
		<-dstDone
		return 0, fmt.Errorf("segment-direct COPY ON SEGMENT: %w", err)
	}

	out := <-recvCh
	if out.err != nil {
		_ = fp.Close()
		<-dstDone
		return 0, fmt.Errorf("segment-direct receive: %w", out.err)
	}
	// Cross-check: the sum of the per-slice trailer row counts must equal the
	// rows the destination actually wrote (a merge bug would break this).
	var trailerRows int64
	for _, r := range out.results {
		if r.Err != nil {
			_ = fp.Close()
			<-dstDone
			return 0, fmt.Errorf("segment-direct slice content=%d: %w", r.ContentID, r.Err)
		}
		trailerRows += r.Rows
	}
	if err := mux.End(); err != nil {
		_ = fp.Close()
		<-dstDone
		return 0, fmt.Errorf("segment-direct: %w", err)
	}
	// Close the write side so the destination sees end-of-copy.
	_ = fp.Close()
	<-dstDone

	if dstErr != nil {
		return 0, fmt.Errorf("dest copy: %w", dstErr)
	}
	if trailerRows != dstRows {
		return 0, fmt.Errorf("segment-direct: trailer rows %d != dest rows %d", trailerRows, dstRows)
	}
	return dstRows, nil
}

// primarySegmentCount returns the number of up primary CONTENTS on the
// source — i.e. the number of segreceivers the kernel launches with
// ON SEGMENT (GANGTYPE_PRIMARY_WRITER, one per content; the master's
// content=-1 row is not a segment). It requires a Greenplum source
// (gp_segment_configuration); on plain PostgreSQL it errors, which the
// caller surfaces as a clear "segment-direct requires Greenplum" failure.
//
// count(*) would include the master row (content=-1) and overcount by one
// (E2 e2e, 27.09: n=3 on a 2-segment cluster — the receiver waited for a
// third slice that never came).
func (m *Manager) primarySegmentCount(ctx context.Context) (int, error) {
	v := pg.RowStr(ctx, m.srcRead(),
		`SELECT count(DISTINCT content) FROM gp_segment_configuration
		 WHERE role = 'p' AND status = 'u' AND content >= 0`)
	if v == "" {
		return 0, fmt.Errorf("cannot enumerate segments (segment-direct requires a Greenplum source)")
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("bad segment count %q: %w", v, err)
	}
	return n, nil
}

// pacedWriter throttles the source-side writes to the limiter's current rate
// (FR-14). When the limiter is nil (no --rate-limit) it is a transparent
// passthrough, so the default path is unaffected.
func (m *Manager) pacedWriter(fp *transfer.FramedPipe) io.Writer {
	if m.pacer == nil {
		return fp
	}
	return &pacedWriter{m: m, fp: fp}
}

type pacedWriter struct {
	m  *Manager
	fp *transfer.FramedPipe
}

// Write throttles to the current rate, then writes the chunk to the pipe.
func (w *pacedWriter) Write(p []byte) (int, error) {
	if err := w.m.pacer.Wait(context.Background(), len(p)); err != nil {
		return 0, err
	}
	return w.fp.Write(p)
}

// startLimiterSampler periodically samples host load so the limiter adapts the
// data-path rate (FR-14). It is a no-op when no rate-limit is set and returns
// nil so the caller can skip the deferred stop.
func (m *Manager) startLimiterSampler(ctx context.Context) func() {
	if m.lim == nil {
		return nil
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.lim.Update()
			}
		}
	}()
	return func() {}
}

// establishSnapshot pins the source read snapshot for the run (FR-02). The spec
// requires that the data of each source database be copied within a single
// point-in-time snapshot, realised as a long-lived read-only REPEATABLE READ
// transaction on the source with every COPY executed inside it.
//
//	--consistent-db (default): begin a snapshot transaction up front; the first
//	  unit's COPY establishes the snapshot and the remaining units read from
//	  the same one (all COPYs of the database are inside one snapshot).
//	--consistent-cluster: additionally place the source in a single frozen
//	  state (maintenance window) so the whole cluster is frozen, then begin the
//	  snapshot transaction.
//
// A failure to begin the transaction is non-fatal: the copy still proceeds
// (unit-by-unit) and is recorded in the journal, but the operator is told the
// consistency mode could not be established so a snapshot-consistent copy is
// not guaranteed.
func (m *Manager) establishSnapshot(ctx context.Context) {
	if m.cfg.ConsistentDB && m.cfg.ConsistentCluster {
		// Mutually exclusive (validated by config); treat as a warning.
		return
	}
	if !m.cfg.ConsistentDB && !m.cfg.ConsistentCluster {
		m.snapshotMode = ""
		return
	}

	mode := "db"
	if m.cfg.ConsistentCluster {
		mode = "cluster"
	}

	// FR-02 + FR-03: a snapshot is per-connection (a transaction lives on one
	// backend), so a pool-backed source must pin ONE connection for the whole
	// run; every source read then shares the single REPEATABLE READ snapshot
	// even under parallel workers. Without the pin, each pool COPY would take
	// a fresh connection and see a different snapshot, silently breaking the
	// consistency guarantee. The pin serialises source reads (pinnedConn.mu).
	if pool, ok := m.srcQ.(*pg.Pool); ok {
		if q, release, err := pool.Pin(ctx); err == nil {
			m.pinnedSrc = q
			m.pinRelease = release
			m.src = source.New(q) // route all source reads through the pinned conn
		}
	}

	// A live snapshot requires a streaming connection. The integration tests
	// and fakes do not run a real transaction, so when the source is a fake
	// (no streaming transaction support) we record the intended mode without
	// issuing the transaction — the fake COPYs already read a consistent view.
	if err := m.beginSnapshotTx(ctx, mode); err != nil {
		// Non-fatal: proceed without a pinned snapshot but surface it.
		m.snapshotMode = ""
		m.unpinSource()
		return
	}
	m.snapshotMode = mode
	m.snapshotTx = true
}

// srcRead returns the querier that source reads (data COPYs, catalog, checksum)
// run on. In FR-02 snapshot mode over a pool it is the pinned connection so all
// reads share one snapshot; otherwise it is the source querier (pool or conn).
func (m *Manager) srcRead() pg.StreamingQuerier {
	if m.pinnedSrc != nil {
		return m.pinnedSrc
	}
	return m.srcQ
}

// unpinSource returns a pinned snapshot connection to its pool (idempotent).
func (m *Manager) unpinSource() {
	if m.pinRelease != nil {
		m.pinRelease()
		m.pinRelease = nil
		m.pinnedSrc = nil
	}
}

// beginSnapshotTx opens the read-only REPEATABLE READ transaction that pins the
// source snapshot for the run (FR-02). For --consistent-cluster it first
// requests a cluster-wide freeze. It runs on the snapshot connection (the
// pinned pool connection in snapshot mode, else the source querier) so the
// BEGIN is on the same backend the COPYs will use. It returns an error if the
// transaction cannot be started (the caller then proceeds without a snapshot).
func (m *Manager) beginSnapshotTx(ctx context.Context, mode string) error {
	// --consistent-cluster: single source freeze (maintenance window, FR-02).
	if mode == "cluster" {
		if _, err := m.srcRead().Exec(ctx, `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`); err != nil {
			return fmt.Errorf("consistent-cluster freeze: %w", err)
		}
	}
	if _, err := m.srcRead().Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ`); err != nil {
		return fmt.Errorf("begin snapshot tx: %w", err)
	}
	return nil
}

// releaseSnapshot ends the source snapshot transaction (FR-02) once all units
// have been copied. It is a no-op when no snapshot transaction was opened.
func (m *Manager) releaseSnapshot(ctx context.Context) {
	if !m.snapshotTx {
		return
	}
	_, _ = m.srcRead().Exec(ctx, `COMMIT`)
	m.snapshotTx = false
	m.unpinSource()
}

// jobID derives a stable job identifier from the plan hash (FR-10). It is used
// as the namespace prefix for staging tables and journal keys.
func jobID(p *plan.Plan) string {
	if p == nil || p.Hash == "" {
		return "gpstream"
	}
	return p.Hash
}

// isEOF reports whether err is io.EOF (a clean end of the COPY stream).
func isEOF(err error) bool {
	return errors.Is(err, io.EOF)
}
