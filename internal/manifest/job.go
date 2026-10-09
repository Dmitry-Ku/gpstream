// Package job implements the gpstream Job Manager (FR-02/FR-06/FR-07): the
// operation journal, checkpoints, staging + atomic swap, cleanup, and the
// auto-resume / crash-recovery state machine.
//
// The journal has two writers (ADR-001): a primary record set in the
// destination service DB and a local file mirror, so a job survives the
// destination DB becoming unreachable (FR-07). A unit is "done" only once
// data + checksum (+ optionally swap) are recorded (ADR-001 §4.4).
package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// State is the lifecycle state of a unit or job (FR-06/FR-07).
type State string

const (
	// StatePending is a unit not yet started.
	StatePending State = "pending"
	// StateCopying is a unit whose data is in flight.
	StateCopying State = "copying"
	// StateVerifying is a unit awaiting verification.
	StateVerifying State = "verifying"
	// StateCompleted is a verified unit (data + checksum).
	StateCompleted State = "completed"
	// StateSwapped is a completed unit atomically renamed to its final name.
	StateSwapped State = "swapped"
	// StateFailed is a unit that exhausted its retries.
	StateFailed State = "failed"
	// StateSkipped is a unit the run deliberately did not copy (a
	// --resolve-conflicts skip, or an --overwrite target whose data is
	// already identical on the destination). It is terminal: `status`
	// counts it as done and `resume` does not re-queue it.
	StateSkipped State = "skipped"
)

// Unit is one journalable unit: a database/schema/table/partition/index.
type Unit struct {
	JobID  string `json:"job_id"`
	UnitID int64  `json:"unit_id"`
	DB     string `json:"db"`
	Schema string `json:"schema"` // SOURCE schema (read from the source cluster)
	Table  string `json:"table"`  // SOURCE table  (read from the source cluster)
	// DstSchema/DstName are the destination names after rename (FR-11/§4.10);
	// they equal Schema/Table when no rename applies. The copy writes to these.
	DstSchema string `json:"dst_schema,omitempty"`
	DstName   string `json:"dst_name,omitempty"`
	Part      string `json:"part,omitempty"`
	Kind      string `json:"kind"` // table|partition|index|view|...
	State     State  `json:"state"`
	Checksum  string `json:"checksum,omitempty"`
	RowCount  int64  `json:"row_count"`
	Bytes     int64  `json:"bytes"`
	Conflict  bool   `json:"conflict,omitempty"` // planner-flagged conflict (FR-10/FR-11)
	// Mesh marks a unit that rode the MESH (segment-to-segment) data path.
	// For a mesh unit the DESTINATION agent is the writer of record for the
	// gpstream_units DB row (architecture 08.10): it copies, verifies and
	// swaps the unit, so it records the journal itself. The controller then
	// writes ONLY the file mirror for it (the durable resume source) and
	// skips the DB journal (which it no longer owns for mesh units).
	Mesh bool `json:"mesh,omitempty"`
	// DestJournal (2B-6, architecture 08.10) marks a unit whose gpstream_units
	// DB row is owned by the DESTINATION agent: the COORDINATOR two-agent data
	// path (the coordinator analogue of the mesh MiniLaunch path) — the
	// destination copies, verifies and swaps the unit, so it records the
	// journal itself (copying at the serve start, swapped/failed at the serve
	// end). The controller writes ONLY the file mirror for it (the durable
	// resume source) and skips the DB journal for the post-stream states
	// (completed/swapped); the pre-stream states (pending/copying) the
	// controller writes are superseded in the DB by the destination's own
	// serve-time write (the destination is the writer of record for the row),
	// exactly as the mesh path does. Set after the stream in runUnit;
	// preserved in the file mirror so a resume knows the dest agent owns the
	// row.
	DestJournal bool `json:"dest_journal,omitempty"`
	Attempts    int  `json:"attempts"`
	// Epoch is the snapshot generation (T5, DESIGN §2.2) under which the
	// unit's data was verified: the run's epoch is stamped when the unit
	// reaches StateCompleted. A resume re-reads a table whose completed
	// units span more than one epoch (relaxed) or were verified in an
	// older epoch than the fresh one (strict).
	Epoch int64 `json:"epoch,omitempty"`
	// IndexDone marks the unit's background index build (FR-17) as finished
	// in a previous run: a T5 resume that keeps a swapped unit must NOT
	// re-enqueue its (already built) indexes, or an idempotent re-run would
	// REINDEX every table on the destination. Set after the index pool
	// drains cleanly; a unit killed between the swap and the index build
	// stays IndexDone=false and is re-enqueued on the next resume (the
	// idempotent createOneIndex makes that duplicate-safe).
	IndexDone bool `json:"index_done,omitempty"`
	// Partition is the source table's partition spec (Fix 1); "" for a plain
	// table. It is reproduced in the destination DDL so the destination
	// re-creates the same leaf layout. Carried on the unit (not just the plan
	// table) so a resume re-reads the same spec.
	Partition pg.PartitionSpec `json:"partition,omitempty"`
	// PartitionGuard is the planner's Fix-1 guard diagnostic ("" = safe). A
	// non-empty value means the unit must fail early at run time: a
	// string-keyed parent without a DEFAULT partition on a collation-
	// different destination would abort the whole COPY mid-stream.
	PartitionGuard string `json:"partition_guard,omitempty"`
	// RouteMode (P8, M4/T4b): how this table's data lands on the destination
	// (direct_1to1 vs router_full vs merge), copied from the plan's table at
	// unit build. The two-agent on_segment data path uses it to pick the
	// direct_1to1 fast-path (per-segment utility COPYs) or the routed
	// segment-to-segment path over the merge path.
	RouteMode route.Mode `json:"route_mode,omitempty"`
	// SrcReplicated (design 04.10, question 5): the source table is
	// DISTRIBUTED REPLICATED — an ON SEGMENT read would stream S identical
	// full copies (S× rows on the destination), so the unit takes the
	// coordinator data path (read through the source coordinator, write
	// through the destination coordinator) instead of on_segment.
	SrcReplicated bool `json:"src_replicated,omitempty"`
	// RouteReason (design 04.10): when RouteMode==merge, why (hash-mismatch
	// / non-hash distribution) — the manager logs it as a fallback event.
	RouteReason string    `json:"route_reason,omitempty"`
	Updated     time.Time `json:"updated"`
}

// JournalSink (2D-A, architecture 08.10, full zero-DB) is how the journal
// reaches the destination service DB WITHOUT the controller holding a direct
// connection: the Manager wires it to the two-agent runner, which delegates
// each write to the destination agent (the writer of record — the journal
// lives in the destination service DB, ADR-001 §4.3). When the sink is nil
// (tests / the in-process path that keeps a local pool), the journal uses its
// pg.Querier directly (the legacy path). The file mirror is ALWAYS written
// locally regardless of sink — it is the durable resume source (FR-07).
type JournalSink interface {
	// Init creates the gpstream_units table (idempotent).
	Init(ctx context.Context) error
	// SaveMeta upserts the gpstream_jobs row (the job's snapshot metadata).
	SaveMeta(ctx context.Context, jobID string, m *JobMeta) error
	// SetState upserts one gpstream_units row (the unit's current state).
	SetState(ctx context.Context, u *Unit) error
}

// Journal is the operation log (ADR-001 §4.3). It writes to a pg.Querier
// (the destination service DB) and a local file mirror so a job survives the
// destination DB going down.
type Journal struct {
	mu     sync.Mutex
	q      pg.Querier // primary (dest DB); may be nil in tests
	mirror string     // local file mirror path
	units  map[int64]*Unit
	// sink (2D-A, full zero-DB), when set, routes the DB writes to the
	// destination agent (the controller holds no direct dest connection).
	// nil = the legacy direct-pool path (q).
	sink JournalSink
	// note, when set, receives a human-readable line for every primary-journal
	// write error (the Manager wires it to the F-04/F-14 event log, 07.10:
	// a silent `_, _ =` let a GP6/PG9.4 journal write die unnoticed).
	note func(msg string)
}

// NewJournal creates a journal backed by a Querier and a file mirror.
func NewJournal(q pg.Querier, mirrorPath string) *Journal {
	return &Journal{
		q:      q,
		mirror: mirrorPath,
		units:  map[int64]*Unit{},
	}
}

// SetSink (2D-A, architecture 08.10, full zero-DB) routes the journal's
// destination-DB writes through the destination agent instead of a direct
// controller connection (the journal lives in the destination service DB,
// ADR-001 §4.3). The Manager wires it to the two-agent runner at Run start
// (after the agents are up). A nil sink leaves the legacy direct-pool path
// (q) in place (tests / in-proc runners that keep a local pool).
func (j *Journal) SetSink(sink JournalSink) {
	j.sink = sink
}

// Note sets the sink for primary-journal error lines (F-04/F-14 event log).
func (j *Journal) Note(f func(string)) {
	if f != nil {
		j.note = f
	}
}

func (j *Journal) noteLine(msg string) {
	if j.note != nil {
		j.note(msg)
	}
}

// ConflictsPath is the F-10 §8 conflicts log: alongside the journal mirror,
// named conflicts.jsonl. Empty when no mirror path is set.
func (j *Journal) ConflictsPath() string {
	if j.mirror == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(j.mirror), "conflicts.jsonl")
}

// RunStatsPath is the F-04 live-run stats file: alongside the journal mirror,
// named runstats.json. The manager refreshes it (throughput, ETA, per-table
// rows/bytes, states) so `gpstream status` can show a live table for a running
// operation without connecting to the controller's /metrics endpoint. Empty
// when no mirror path is set.
func (j *Journal) RunStatsPath() string {
	if j.mirror == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(j.mirror), "runstats.json")
}

// EventsPath is the F-04/F-14 diagnostic event stream: alongside the
// journal mirror, named events.jsonl. Empty when no mirror path is set
// (the Manager then keeps a no-op event log).
func (j *Journal) EventsPath() string {
	if j.mirror == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(j.mirror), "events.jsonl")
}

// AuditPath is the F-13 audit log of actions: alongside the journal mirror,
// named audit.log (0600). It records the operator's actions on the
// operation (run, resume, cleanup) — separate from events.jsonl, which is
// the in-run diagnostic stream (F-04/F-14). Empty when no mirror path is
// set (the caller then keeps a no-op log).
func (j *Journal) AuditPath() string {
	if j.mirror == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(j.mirror), "audit.log")
}

// AuditLog is the F-13 action audit trail: one JSON line per operator
// action ({ts, action, fields...}), appended to audit.log. Like EventLog
// it is best-effort (a write failure never fails the action) and a nil
// path is a no-op, so commands that have no state dir stay side-effect free.
type AuditLog struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// NewAuditLog wraps the audit file path (empty = no-op). The file is
// created on first write, so a command that logs nothing leaves no file.
func NewAuditLog(path string) *AuditLog {
	return &AuditLog{path: path}
}

// Log appends one action record. fields must be alternating key, value
// pairs; an odd tail is dropped. It never fails.
func (l *AuditLog) Log(action string, fields ...any) {
	if l.path == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := map[string]any{"ts": time.Now().UTC().Format(time.RFC3339Nano), "action": action}
	for i := 0; i+1 < len(fields); i += 2 {
		if k, ok := fields[i].(string); ok {
			rec[k] = fields[i+1]
		}
	}
	f, err := l.openLocked()
	if err != nil {
		return
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return
	}
	line = append(line, '\n')
	if _, err := f.Write(line); err != nil {
		if l.f == f {
			l.f = nil
		}
	}
}

// openLocked lazily opens the audit file in append mode. Callers hold l.mu.
func (l *AuditLog) openLocked() (*os.File, error) {
	if l.f != nil {
		return l.f, nil
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	l.f = f
	return f, nil
}

// Close flushes and closes the audit file. Safe to call on a no-op log
// and more than once.
func (l *AuditLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
}

// WriteConflicts persists the plan's conflict diagnosis to conflicts.jsonl
// (F-10 §8: "all conflicts are logged (conflicts.jsonl + manifest)") so an
// operator can audit what would collide — and with which action — without
// parsing stdout. A nil/empty conflict set still writes the file (an empty
// array): the file's presence proves the preflight ran.
func (j *Journal) WriteConflicts(cs []plan.Conflict) error {
	if p := j.ConflictsPath(); p != "" {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		f, err := os.Create(p)
		if err != nil {
			return err
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, c := range cs {
			if err := enc.Encode(c); err != nil {
				return err
			}
		}
		return nil
	}
	// No mirror path: the in-memory plan still carries the conflicts (they
	// are journaled via Unit.Conflict), so this is a no-op, not an error.
	return nil
}

// Init creates the primary journal table on the destination service DB
// (ADR-001 §4.3). It is idempotent (CREATE TABLE IF NOT EXISTS). The reserved
// words schema/table are quoted so the DDL parses. The file mirror is the
// durable fallback (FR-07) when this cannot be created.
func (j *Journal) Init(ctx context.Context) error {
	// 2D-A (full zero-DB): the gpstream_units table is created by the
	// destination agent (the sink) — the controller holds no direct
	// destination connection. The legacy path (no sink) uses the pool.
	if j.sink != nil {
		return j.sink.Init(ctx)
	}
	if j.q == nil {
		return nil
	}
	_, err := j.q.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS gpstream_units (
		    job_id      text NOT NULL,
		    unit_id     bigint NOT NULL,
		    "db"        text,
		    "schema"    text,
		    "table"     text,
		    "part"      text,
		    kind        text,
		    state       text,
		    checksum    text,
		    row_count   bigint,
		    byte_count  bigint,
		    attempts    int,
		    epoch       bigint,
		    index_done  boolean,
		    updated     timestamptz,
		    PRIMARY KEY (job_id, unit_id)
		)`)
	if err != nil {
		return err
	}
	// FR-17: operations that started before the index_done column existed
	// (CREATE TABLE IF NOT EXISTS never alters an existing table) keep a
	// journal without it; the index-upsert below would then fail on every
	// state write (the error is surfaced to events.jsonl, but the column is
	// never recorded). Add it to the pre-existing table. Guarded by the
	// exception handler (PG 9.4 has no ADD COLUMN IF NOT EXISTS; GP6 = 9.4).
	// A failure here is non-fatal: the file mirror remains the resume source
	// and the upsert's write-error path surfaces a persistent failure.
	_, _ = j.q.Exec(ctx,
		`DO $$ BEGIN
			ALTER TABLE gpstream_units ADD COLUMN index_done boolean;
		EXCEPTION WHEN duplicate_column THEN
			PERFORM 1;
			END $$`)
	return nil
}

// SetState records a unit's state, persisting to the primary journal and the
// file mirror (FR-02). A completed unit must carry a checksum (ADR-001 §4.4).
// The reserved words schema/table are quoted so the primary-journal INSERT
// parses on a real cluster (ADR-001 §4.3).
func (j *Journal) SetState(u *Unit) error {
	u.Updated = time.Now()
	// Deep-copy the unit under the lock so the (slow) file mirror serialises a
	// frozen snapshot, never the live pointer that runUnit may still be
	// mutating (e.g. u.Checksum/u.RowCount). Each unit is owned by a single
	// worker, so cloning only the unit being set avoids reading another
	// worker's live fields — the map is never re-scanned here, which keeps
	// SetState O(1) (no O(n^2) full-mirror re-serialisation, FR-03/FR-07).
	c := *u
	j.mu.Lock()
	j.units[u.UnitID] = &c
	j.mu.Unlock()
	// Persist to primary journal (best-effort; the file mirror is the durable
	// fallback when the dest DB is unreachable, FR-07). GP6 = PostgreSQL 9.4
	// has no ON CONFLICT, so the upsert is SELECT-by-PK + UPDATE/INSERT: the
	// old upsert failed on GP6 with a syntax error that was swallowed by
	// `_, _ =`, leaving gpstream_units empty (07.10 e2e). A write error still
	// does not sink the job, but it is no longer silent: j.note carries it to
	// the F-04/F-14 event log.
	// 2D-A (full zero-DB): when the sink (the destination agent) is wired, the
	// controller's DB writes for the units it OWNS (pending/skipped +
	// coordinator non-terminal states) go to the agent instead of a direct
	// pool. The !Mesh && !DestJournal guard is PRESERVED: a dest-agent-owned
	// unit's terminal row is the agent's (2B-6/1D) — the controller must not
	// race it with its own (async) write.
	if j.sink != nil && !c.Mesh && !c.DestJournal {
		if err := j.sink.SetState(context.Background(), &c); err != nil {
			j.noteLine("journal: gpstream_units write failed (mirror remains the resume source): " + err.Error())
		}
		return j.mirrorAppend(&c)
	}
	if j.q != nil && !c.Mesh && !c.DestJournal {
		// A dest-agent-owned unit's DB row (the mesh path, or the coordinator
		// two-agent path since 2B-6) is the destination agent's (the writer of
		// record); the controller records only the file mirror for it.
		// UPDATE args: (job_id, unit_id, state, checksum, row_count,
		// byte_count, attempts, epoch, index_done, updated).
		updArgs := []any{u.JobID, u.UnitID, string(u.State), u.Checksum, u.RowCount, u.Bytes, u.Attempts, u.Epoch, u.IndexDone, u.Updated}
		// INSERT args: 15 columns (job_id, unit_id, db, schema, table, part,
		// kind, state, checksum, row_count, byte_count, attempts, epoch,
		// index_done, updated).
		insArgs := []any{u.JobID, u.UnitID, u.DB, u.Schema, u.Table, u.Part, u.Kind, string(u.State), u.Checksum, u.RowCount, u.Bytes, u.Attempts, u.Epoch, u.IndexDone, u.Updated}
		if err := pgUpsert(context.Background(), j.q,
			`SELECT 1 FROM gpstream_units WHERE job_id = $1 AND unit_id = $2`,
			[]any{u.JobID, u.UnitID},
			updArgs,
			insArgs,
			`UPDATE gpstream_units SET state = $3, checksum = $4, row_count = $5, byte_count = $6, attempts = $7, epoch = $8, index_done = $9, updated = $10 WHERE job_id = $1 AND unit_id = $2`,
			`INSERT INTO gpstream_units (job_id, unit_id, "db", "schema", "table", "part", kind, state, checksum, row_count, byte_count, attempts, epoch, index_done, updated)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`); err != nil {
			j.noteLine("journal: gpstream_units write failed (mirror remains the resume source): " + err.Error())
		}
	}
	// File mirror (durable): append the single changed record. A MESH unit's
	// DB journal row is written by the DESTINATION agent (the writer of
	// record, architecture 08.10) — the controller records only the mirror
	// (the durable resume source) and does not touch the destination DB
	// journal for it.
	return j.mirrorAppend(&c)
}

// pgUpsert inserts the row or, if a row with the same primary key already
// exists, updates it. GP6 (PostgreSQL 9.4) predates ON CONFLICT, so this is a
// SELECT-by-PK followed by UPDATE or INSERT. The SELECT→write gap is benign
// here: each unit is owned by a single worker (its SetState calls are
// sequential), and other workers use different (job_id, unit_id) keys, so two
// same-key writers can only ever be the same unit's own sequential calls.
func pgUpsert(ctx context.Context, q pg.Querier, selSQL string, selArgs, updSQLArgs, insSQLArgs []any, updSQL, insSQL string) error {
	if v, err := pg.Row(ctx, q, selSQL, selArgs...); err != nil {
		return err
	} else if v != nil {
		if _, err := q.Exec(ctx, updSQL, updSQLArgs...); err != nil {
			return err
		}
		return nil
	}
	_, err := q.Exec(ctx, insSQL, insSQLArgs...)
	return err
}

// mirrorAppend appends one unit record (a deep copy taken by the caller under
// the lock) to the local file mirror. It performs no map access, so it may run
// concurrently with other workers' SetState without racing (FR-03/FR-07).
func (j *Journal) mirrorAppend(u *Unit) error {
	if j.mirror == "" {
		return nil
	}
	f, err := os.OpenFile(j.mirror, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(u); err != nil {
		return err
	}
	return nil
}

// Get returns a unit by id (from the in-memory cache; the authoritative
// record is the primary journal / file mirror).
func (j *Journal) Get(id int64) *Unit {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.units[id]
}

// SwapState records an atomic rename of a staging unit to its final name
// (FR-02). It is only permitted from a completed (verified) unit.
func (j *Journal) SwapState(id int64) error {
	u := j.Get(id)
	if u == nil {
		return fmt.Errorf("job: unit %d not found", id)
	}
	if u.State != StateCompleted {
		return fmt.Errorf("job: unit %d not in completed state (cannot swap)", id)
	}
	u.State = StateSwapped
	return j.SetState(u)
}

// Cleanup removes all units of a job from the journal (FR-02,
// `gpstream cleanup`). Destructive; only invoked with --yes.
func (j *Journal) Cleanup(jobID string) error {
	j.mu.Lock()
	for id, u := range j.units {
		if u.JobID == jobID {
			delete(j.units, id)
		}
	}
	j.mu.Unlock()
	if j.q != nil {
		_, _ = j.q.Exec(context.Background(),
			`DELETE FROM gpstream_units WHERE job_id = $1`, jobID)
	}
	// Truncate the file mirror for this manifest.
	if j.mirror != "" {
		_ = os.Remove(j.mirror)
	}
	return nil
}

// Units returns a snapshot of all units (for resume/crash-recovery,
// FR-06/FR-07).
func (j *Journal) Units() []*Unit {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]*Unit, 0, len(j.units))
	for _, u := range j.units {
		// Return a copy so callers can mutate the state without the lock.
		c := *u
		out = append(out, &c)
	}
	return out
}

// LoadFromMirror re-reads the file mirror and repopulates the in-memory cache
// (FR-07 durable resume). The mirror is the durable fallback for the primary
// journal, but without this the appended records are never read back, so a
// restart would resume an empty job — the "durable" fallback would not be
// durable. The mirror is append-only and may hold several records per unit
// (one per state change); the last record for each UnitID is the most recent
// state, so we keep the last one seen. A missing or empty mirror is not an
// error (there was nothing to recover).
func (j *Journal) LoadFromMirror() error {
	if j.mirror == "" {
		return nil
	}
	f, err := os.Open(j.mirror)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no mirror yet: nothing to recover
		}
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for dec.More() {
		var u Unit
		if err := dec.Decode(&u); err != nil {
			// A truncated final line (the write was interrupted) is tolerated:
			// the earlier, complete records already restored the job's state.
			return nil
		}
		if u.UnitID == 0 {
			continue // no identity: cannot place in the cache
		}
		j.mu.Lock()
		j.units[u.UnitID] = &u
		j.mu.Unlock()
	}
	return nil
}

// MarkUnit sets a unit's state and persists it (FR-06/FR-07). If the unit is
// not known it is created as a pending unit.
func (j *Journal) MarkUnit(u *Unit, newState State) error {
	u.State = newState
	return j.SetState(u)
}

// CreateUnit adds a new unit in the given state and persists it.
func (j *Journal) CreateUnit(u *Unit, state State) (int64, error) {
	u.State = state
	if u.UnitID == 0 {
		j.mu.Lock()
		u.UnitID = int64(len(j.units) + 1)
		j.mu.Unlock()
	}
	if err := j.SetState(u); err != nil {
		return 0, err
	}
	return u.UnitID, nil
}
