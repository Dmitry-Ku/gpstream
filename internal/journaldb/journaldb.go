// Package journaldb is the shared gpstream_units (FR-02 journal) schema and
// upsert helpers, usable by BOTH the controller (the file-mirror + legacy
// path) and the destination agent (architecture 08.10: the destination agent
// writes the journal of the units it copies, verifies and swaps — the MESH
// mini path since 1D, the COORDINATOR two-agent path since 2B-6).
//
// It imports only transfer/pg (no manifest, no agent) so neither side
// creates an import cycle. The DDL + upsert are extracted verbatim from
// manifest.Journal (Init / SetState): GP6 = PostgreSQL 9.4 has no ON
// CONFLICT, so the upsert is a SELECT-by-PK followed by UPDATE or INSERT.
package journaldb

import (
	"context"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Unit is the journal row the destination agent writes for a unit it owns
// (it copies, verifies and swaps the unit, so it is the writer of record).
// It mirrors the manifest.Unit fields the gpstream_units schema stores (the
// controller's Unit carries more: the plan/layout fields are not journaled).
type Unit struct {
	JobID       string
	UnitID      int64
	DB          string // source DB (the journal keys by source names)
	Schema      string // source schema
	Table       string // source table
	Part        string // partition leaf ("" for a plain table)
	Kind        string // "table"
	State       string // "pending"|"copying"|"swapped"|"failed"
	Checksum    string // "<rows>:<hex>" content digest (the destination's, on the data path)
	RowCount    int64
	Bytes       int64
	Attempts    int
	Epoch       int64
	OperationID string // 2E-2: scopes the journal row to the operation that wrote it (see the 2E-2 query + cleanup drop). Empty = legacy row, never matches a scoped query.
	// IndexDone (FR-17) marks the unit's background index build as finished.
	// The destination agent writes it true on the row once the controller's
	// index pool drains cleanly (the journal action) — the column the 10K
	// stress journal never had. Defaults false: the serve-time journal writes
	// do not carry it.
	IndexDone bool
}

// Init ensures the gpstream_units table exists (idempotent: CREATE TABLE IF
// NOT EXISTS + the FR-17 index_done backfill, PG 9.4-safe). Called by the
// destination agent (it owns the gpstream_units rows it writes) and by the
// controller's legacy single-process path.
func Init(ctx context.Context, q pg.Querier) error {
	_, err := q.Exec(ctx,
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
		    operation_id text,
		    index_done  boolean,
		    updated     timestamptz,
		    PRIMARY KEY (job_id, unit_id)
		)`)
	if err != nil {
		return err
	}
	// FR-17: a pre-existing table (created before the column existed) keeps a
	// journal without index_done; CREATE TABLE IF NOT EXISTS never alters.
	// PG 9.4 has no ADD COLUMN IF NOT EXISTS, so the backfill is a guarded DO
	// block. Non-fatal (a failure surfaces on the next state write).
	_, _ = q.Exec(ctx,
		`DO $$ BEGIN
			ALTER TABLE gpstream_units ADD COLUMN index_done boolean;
		EXCEPTION WHEN duplicate_column THEN
			PERFORM 1;
			END $$`)
	// 2E-2: the operation_id column scopes rows to the operation that wrote
	// them. A pre-existing table keeps a journal without it; CREATE TABLE IF
	// NOT EXISTS never alters, so the backfill is a guarded DO block (PG 9.4
	// has no ADD COLUMN IF NOT EXISTS). Non-fatal: a failure only degrades
	// the 2E-2 query to the file mirror (graceful).
	_, _ = q.Exec(ctx,
		`DO $$ BEGIN
			ALTER TABLE gpstream_units ADD COLUMN operation_id text;
		EXCEPTION WHEN duplicate_column THEN
			PERFORM 1;
			END $$`)
	return nil
}

// SetState upserts one unit row (the SELECT-by-PK + UPDATE/INSERT pattern;
// PG 9.4 has no ON CONFLICT). The updated timestamp is set here so the
// caller does not manage clocks.
func SetState(ctx context.Context, q pg.Querier, u *Unit) error {
	now := time.Now()
	// UPDATE args: (job_id, unit_id, state, checksum, row_count, byte_count,
	// attempts, epoch, operation_id, index_done, updated).
	updArgs := []any{u.JobID, u.UnitID, u.State, u.Checksum, u.RowCount, u.Bytes, u.Attempts, u.Epoch, u.OperationID, u.IndexDone, now}
	// INSERT args: 16 columns.
	insArgs := []any{u.JobID, u.UnitID, u.DB, u.Schema, u.Table, u.Part, u.Kind, u.State, u.Checksum, u.RowCount, u.Bytes, u.Attempts, u.Epoch, u.OperationID, u.IndexDone, now}
	return pgUpsert(ctx, q,
		`SELECT 1 FROM gpstream_units WHERE job_id = $1 AND unit_id = $2`,
		[]any{u.JobID, u.UnitID},
		updArgs,
		insArgs,
		`UPDATE gpstream_units SET state = $3, checksum = $4, row_count = $5, byte_count = $6, attempts = $7, epoch = $8, operation_id = $9, index_done = $10, updated = $11 WHERE job_id = $1 AND unit_id = $2`,
		`INSERT INTO gpstream_units (job_id, unit_id, "db", "schema", "table", "part", kind, state, checksum, row_count, byte_count, attempts, epoch, operation_id, index_done, updated)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`)
}

// SetIndexDone (FR-17) marks the unit's background index build as finished on the
// gpstream_units row (the controller's index pool drained cleanly). The destination
// agent is the writer of record for the row (architecture 08.10), so the
// controller delegates this single-column update to it. It is a targeted
// UPDATE (no full upsert): the serve-time states (swapped + checksum + row
// count) are left untouched. A no-op if the row does not exist yet.
func SetIndexDone(ctx context.Context, q pg.Querier, jobID string, unitID int64, operationID string) error {
	// 2E-2: scope the index_done update on the operation too (the row's
	// operation_id). An empty operationID (a legacy row) degrades to the
	// job_id-only scope.
	if operationID == "" {
		_, err := q.Exec(ctx,
			`UPDATE gpstream_units SET index_done = true WHERE job_id = $1 AND unit_id = $2`,
			jobID, unitID)
		return err
	}
	_, err := q.Exec(ctx,
		`UPDATE gpstream_units SET index_done = true WHERE job_id = $1 AND unit_id = $2 AND operation_id = $3`,
		jobID, unitID, operationID)
	return err
}

// JobMeta is the gpstream_jobs row (the job's snapshot metadata: which
// generation of the source snapshot the job copied under + the transport).
// It mirrors the manifest.JobMeta fields the gpstream_jobs schema stores.
type JobMeta struct {
	JobID     string
	SnapEpoch int64
	SnapID    string
	CopyMode  string
}

// SaveMeta upserts the gpstream_jobs row (the SELECT-by-PK + UPDATE/INSERT
// pattern; PG 9.4 has no ON CONFLICT). The destination agent owns the
// journal (architecture 08.10, full zero-DB), so it writes the job metadata
// on its LOCAL coordinator connection instead of the controller. Idempotent
// (the epoch is written once per run, before any unit is processed).
func SaveMeta(ctx context.Context, q pg.Querier, m *JobMeta) error {
	if _, err := q.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS gpstream_jobs (
		    job_id      text PRIMARY KEY,
		    snap_epoch  bigint NOT NULL,
		    snap_id     text,
		    copy_mode   text
		)`); err != nil {
		return err
	}
	return pgUpsert(ctx, q,
		`SELECT 1 FROM gpstream_jobs WHERE job_id = $1`,
		[]any{m.JobID},
		[]any{m.JobID, m.SnapEpoch, m.SnapID, m.CopyMode},
		[]any{m.JobID, m.SnapEpoch, m.SnapID, m.CopyMode},
		`UPDATE gpstream_jobs SET snap_epoch = $2, snap_id = $3, copy_mode = $4 WHERE job_id = $1`,
		`INSERT INTO gpstream_jobs (job_id, snap_epoch, snap_id, copy_mode)
		 VALUES ($1, $2, $3, $4)`)
}

// pgUpsert inserts the row or, if a row with the same primary key exists,
// updates it (PG 9.4 has no ON CONFLICT). The SELECT->write gap is benign:
// each unit is owned by a single writer (the destination agent serves one
// unit at a time on the sequential control loop, and same-key writers can
// only ever be the same units own sequential calls).
func pgUpsert(ctx context.Context, q pg.Querier, selSQL string, selArgs, updArgs, insArgs []any, updSQL, insSQL string) error {
	if v, err := pg.Row(ctx, q, selSQL, selArgs...); err != nil {
		return err
	} else if v != nil {
		_, err := q.Exec(ctx, updSQL, updArgs...)
		return err
	}
	_, err := q.Exec(ctx, insSQL, insArgs...)
	return err
}
