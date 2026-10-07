// T5 (DESIGN §2.2): resume-epoch semantics.
//
// A snapshot cannot survive the death of the session that opened it (there is
// no persistent cross-session snapshot in PostgreSQL), so every resume of a
// job starts a FRESH source snapshot. The snapshot generation of a run is its
// EPOCH: the first run of an operation is epoch 1; every run that resumes an
// existing job (journal with records, or published metadata) increments it
// (E+1, DESIGN §2.2).
//
// The epoch of a run is stamped on:
//   - the job metadata record (gpstream_jobs.snap_epoch), and
//   - every unit that reaches a verified state (Unit.Epoch, gpstream_units.epoch),
//
// so a later resume can tell which snapshot each unit's verified data came
// from. The re-read decision at resume time follows the DESIGN §2.2 rule
// (a table is kept only if it was verified under one snapshot epoch):
//
//   - table completed in ONE epoch       -> keep (verify aggregate -> swap)
//   - table completed in DIFFERENT epochs -> re-read the whole table
//   - table has a non-completed unit      -> re-read the whole table
//   - already swapped                     -> keep (point in time = swap moment)
//
// The invariant preserved: table integrity — every unit of a table is
// verified under one snapshot. Cross-table consistency after a crash is
// broken in any case, so re-reading only the affected tables (not the whole
// database) is the rule (a 1 PB re-read per failure is unacceptable).
// Within a single run without a failure there is one snapshot for
// everything.
package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// JobMeta is the per-operation metadata record (DESIGN §4, gpstream_jobs). It
// is the durable home of the snapshot epoch so a resume knows which
// generation it continues. The primary record lives in the destination
// service DB; the file mirror (jobmeta.json next to journal.jsonl) is the
// durable fallback when the destination DB is unreachable (FR-07).
type JobMeta struct {
	JobID     string `json:"job_id"`
	SnapEpoch int64  `json:"snap_epoch"`
	SnapID    string `json:"snap_id,omitempty"`
	CopyMode  string `json:"copy_mode,omitempty"`
}

// metaFileName is the file-mirror name of JobMeta (next to journal.jsonl).
const metaFileName = "jobmeta.json"

// journalBaseDir returns the directory holding the journal mirror.
func (j *Journal) journalBaseDir() string {
	for i := len(j.mirror) - 1; i >= 0; i-- {
		if j.mirror[i] == '/' || j.mirror[i] == '\\' {
			return j.mirror[:i]
		}
	}
	return ""
}

// metaPath is the file-mirror path of the job metadata, derived from the
// journal mirror path (<dir>/journal.jsonl -> <dir>/jobmeta.json).
func metaPath(mirror string) string {
	for i := len(mirror) - 1; i >= 0; i-- {
		if mirror[i] == '/' || mirror[i] == '\\' {
			return mirror[:i+1] + metaFileName
		}
	}
	return metaFileName
}

// LoadMeta reads the job metadata for jobID from the primary journal
// (gpstream_jobs) and from the file mirror, and returns the one with the
// HIGHER epoch. The file mirror is written before the primary in SaveMeta,
// so preferring the higher value never understates the epoch. The file
// mirror is scoped to the operation's state directory, so it is honoured
// regardless of jobID. It returns a nil JobMeta when no metadata has been
// recorded yet (a fresh operation).
func (j *Journal) LoadMeta(jobID string) (*JobMeta, error) {
	var primary *JobMeta
	if j.q != nil {
		if v := pg.RowStr(context.Background(), j.q,
			`SELECT snap_epoch FROM gpstream_jobs WHERE job_id = $1`, jobID); v != "" {
			var e int64
			if _, err := fmt.Sscanf(v, "%d", &e); err == nil {
				primary = &JobMeta{JobID: jobID, SnapEpoch: e}
			}
		}
	}
	mirrorMeta, err := j.loadMetaFile()
	if err != nil {
		return nil, err
	}
	if mirrorMeta == nil {
		return primary, nil
	}
	if primary == nil || primary.SnapEpoch < mirrorMeta.SnapEpoch {
		return mirrorMeta, nil
	}
	return primary, nil
}

// SaveMeta persists the job metadata to the file mirror first (durable,
// FR-07) and then to the primary journal (best-effort). The epoch is written
// exactly once per run (by the Manager, before any unit is processed), so a
// crash at any point records the epoch the run's snapshot actually was.
func (j *Journal) SaveMeta(jobID string, m *JobMeta) error {
	if j.mirror != "" {
		if dir := j.journalBaseDir(); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if err := os.WriteFile(metaPath(j.mirror), data, 0o600); err != nil {
			return err
		}
	}
	if j.q != nil {
		if _, err := j.q.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS gpstream_jobs (
			    job_id      text PRIMARY KEY,
			    snap_epoch  bigint NOT NULL,
			    snap_id     text,
			    copy_mode   text
			)`); err != nil {
			j.noteLine("journal: gpstream_jobs create failed (mirror remains the resume source): " + err.Error())
		}
		// GP6 = PostgreSQL 9.4 has no ON CONFLICT: upsert via SELECT +
		// UPDATE/INSERT (07.10 e2e: the old upsert silently failed, leaving
		// gpstream_jobs empty on the destination).
		if err := pgUpsert(context.Background(), j.q,
			`SELECT 1 FROM gpstream_jobs WHERE job_id = $1`,
			[]any{jobID},
			[]any{jobID, m.SnapEpoch, m.SnapID, m.CopyMode},
			[]any{jobID, m.SnapEpoch, m.SnapID, m.CopyMode},
			`UPDATE gpstream_jobs SET snap_epoch = $2, snap_id = $3, copy_mode = $4 WHERE job_id = $1`,
			`INSERT INTO gpstream_jobs (job_id, snap_epoch, snap_id, copy_mode)
			 VALUES ($1, $2, $3, $4)`); err != nil {
			j.noteLine("journal: gpstream_jobs write failed (mirror remains the resume source): " + err.Error())
		}
	}
	return nil
}

// loadMetaFile reads the file mirror of the job metadata. A missing file is
// "no metadata yet"; a torn final line (crash mid-write) is tolerated — the
// journal mirror already carries the per-unit epochs, and the worst case of
// ignoring a torn meta is one extra re-read (conservative, never data loss).
func (j *Journal) loadMetaFile() (*JobMeta, error) {
	if j.mirror == "" {
		return nil, nil
	}
	data, err := os.ReadFile(metaPath(j.mirror))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var m JobMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, nil
	}
	if m.SnapEpoch < 0 {
		return nil, nil
	}
	return &m, nil
}

// establishEpoch determines this run's snapshot epoch and persists it
// (DESIGN §2.2): a fresh operation starts at epoch 1; a resume (journal
// records or published metadata already exist) continues at lastEpoch+1. It
// returns the epoch and whether this run is a resume. The metadata is written
// before any unit is processed so the epoch the run's snapshot actually was
// is recorded no matter where the run dies.
func (m *Manager) establishEpoch() (int64, bool, error) {
	var last int64
	if meta, err := m.journal.LoadMeta(m.jobID); err != nil {
		return 0, false, fmt.Errorf("load job meta: %w", err)
	} else if meta != nil {
		last = meta.SnapEpoch
	}
	epoch := last + 1
	isResume := last > 0 || journalHasRecords(m.journal)
	cm := string(m.cfg.CopyMode)
	if cm == "" {
		cm = "coordinator"
	}
	if err := m.journal.SaveMeta(m.jobID, &JobMeta{
		JobID:     m.jobID,
		SnapEpoch: epoch,
		CopyMode:  cm,
	}); err != nil {
		return 0, false, fmt.Errorf("save job meta: %w", err)
	}
	return epoch, isResume, nil
}

// journalHasRecords reports whether the in-memory journal holds any unit —
// the marker that this run resumes an existing job rather than starting a
// fresh one.
func journalHasRecords(j *Journal) bool {
	return len(j.Units()) > 0
}

// reReadTables computes the DESIGN §2.2 re-read decision at resume time. It
// groups the journal's units by logical table (db|schema|table) and keeps a
// table iff every unit of the group is terminal (completed or swapped) and
// the group's completed units all carry the SAME epoch (one snapshot owns
// the table). Swapped units do not participate in the epoch set (point in
// time = swap moment). Otherwise the whole table is re-read under the fresh
// snapshot. Groups without records (never copied) are simply copied — neither
// a re-read nor a keep. The returned set holds the table keys that must be
// re-copied under the fresh snapshot; for the staging path runUnit starts from
// a clean staging table (a surviving one is truncated, a56be8b), so a re-read
// replaces rather than appends.
func reReadTables(units []*Unit) map[string]bool {
	reRead := map[string]bool{}
	type groupState struct {
		epochs  map[int64]bool // epochs of the completed (non-swapped) units
		allTerm bool           // every unit of the group is completed/swapped
	}
	groups := map[string]*groupState{}
	for _, u := range units {
		key := tableKey(u)
		g, ok := groups[key]
		if !ok {
			g = &groupState{epochs: map[int64]bool{}, allTerm: true}
			groups[key] = g
		}
		switch u.State {
		case StateSwapped:
			// Terminal: kept (point in time = swap moment).
		case StateCompleted:
			g.epochs[u.Epoch] = true
		default:
			// pending/copying/verifying/failed: unverified — re-read.
			g.allTerm = false
		}
	}
	for key, g := range groups {
		if !g.allTerm {
			reRead[key] = true
			continue
		}
		if len(g.epochs) > 1 {
			reRead[key] = true
		}
	}
	return reRead
}

// tableKey is the logical-table group key of a unit.
func tableKey(u *Unit) string {
	return u.DB + "|" + u.Schema + "|" + u.Table
}
