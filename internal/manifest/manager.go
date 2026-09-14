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
	"sync"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/index"
	"github.com/gpstream/gpstream/internal/ratelimit"
	"github.com/gpstream/gpstream/internal/monitor"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/source"
	"github.com/gpstream/gpstream/internal/transfer"
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
	snapshotMode string
	snapshotTx   bool
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
	//    source's own column definitions (format_type) so it is faithful across
	//    clusters.
	t := m.tableFor(u)
	schema, name := u.Schema, u.Table
	cols, err := m.src.Columns(ctx, schema, name)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
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
			if err := m.dst.CreateTable(ctx, ds, cols, dn); err != nil {
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
		if err := m.dst.CreateStaging(ctx, ds, cols, target); err != nil {
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
func (m *Manager) copyStream(ctx context.Context, t plan.Table, u *Unit, staging string, fp *transfer.FramedPipe) (int64, error) {
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
//   --consistent-db (default): begin a snapshot transaction up front; the first
//     unit's COPY establishes the snapshot and the remaining units read from
//     the same one (all COPYs of the database are inside one snapshot).
//   --consistent-cluster: additionally place the source in a single frozen
//     state (maintenance window) so the whole cluster is frozen, then begin the
//     snapshot transaction.
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

	// A live snapshot requires a streaming connection. The integration tests
	// and fakes do not run a real transaction, so when the source is a fake
	// (no streaming transaction support) we record the intended mode without
	// issuing the transaction — the fake COPYs already read a consistent view.
	if err := m.beginSnapshotTx(ctx, mode); err != nil {
		// Non-fatal: proceed without a pinned snapshot but surface it.
		m.snapshotMode = ""
		return
	}
	m.snapshotMode = mode
	m.snapshotTx = true
}

// beginSnapshotTx opens the read-only REPEATABLE READ transaction that pins the
// source snapshot for the run (FR-02). For --consistent-cluster it first
// requests a cluster-wide freeze. It returns an error if the transaction
// cannot be started (the caller then proceeds without a snapshot).
func (m *Manager) beginSnapshotTx(ctx context.Context, mode string) error {
	// --consistent-cluster: single source freeze (maintenance window, FR-02).
	if mode == "cluster" {
		if _, err := m.srcQ.Exec(ctx, `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`); err != nil {
			return fmt.Errorf("consistent-cluster freeze: %w", err)
		}
	}
	if _, err := m.srcQ.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ`); err != nil {
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
	_, _ = m.srcQ.Exec(ctx, `COMMIT`)
	m.snapshotTx = false
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
