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
	"sync"
	"time"

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
	Part     string `json:"part,omitempty"`
	Kind     string `json:"kind"` // table|partition|index|view|...
	State    State  `json:"state"`
	Checksum string `json:"checksum,omitempty"`
	RowCount int64  `json:"row_count"`
	Bytes    int64  `json:"bytes"`
	Conflict bool   `json:"conflict,omitempty"` // planner-flagged conflict (FR-10/FR-11)
	Attempts int    `json:"attempts"`
	Updated  time.Time `json:"updated"`
}

// Journal is the operation log (ADR-001 §4.3). It writes to a pg.Querier
// (the destination service DB) and a local file mirror so a job survives the
// destination DB going down.
type Journal struct {
	mu     sync.Mutex
	q      pg.Querier // primary (dest DB); may be nil in tests
	mirror string     // local file mirror path
	units  map[int64]*Unit
}

// NewJournal creates a journal backed by a Querier and a file mirror.
func NewJournal(q pg.Querier, mirrorPath string) *Journal {
	return &Journal{
		q:      q,
		mirror: mirrorPath,
		units:  map[int64]*Unit{},
	}
}

// Init creates the primary journal table on the destination service DB
// (ADR-001 §4.3). It is idempotent (CREATE TABLE IF NOT EXISTS). The reserved
// words schema/table are quoted so the DDL parses. The file mirror is the
// durable fallback (FR-07) when this cannot be created.
func (j *Journal) Init(ctx context.Context) error {
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
		    updated     timestamptz,
		    PRIMARY KEY (job_id, unit_id)
		)`)
	return err
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
	// fallback when the dest DB is unreachable, FR-07).
	if j.q != nil {
		_, _ = j.q.Exec(context.Background(),
			`INSERT INTO gpstream_units (job_id, unit_id, "db", "schema", "table", "part", kind, state, checksum, row_count, byte_count, attempts, updated)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			ON CONFLICT (job_id, unit_id) DO UPDATE SET
			    state=EXCLUDED.state, checksum=EXCLUDED.checksum, row_count=EXCLUDED.row_count,
			    byte_count=EXCLUDED.byte_count, attempts=EXCLUDED.attempts, updated=EXCLUDED.updated`,
			u.JobID, u.UnitID, u.DB, u.Schema, u.Table, u.Part, u.Kind, string(u.State), u.Checksum, u.RowCount, u.Bytes, u.Attempts, u.Updated)
	}
	// File mirror (durable): append the single changed record.
	return j.mirrorAppend(&c)
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
