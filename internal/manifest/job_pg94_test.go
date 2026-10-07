package manifest

import (
	"errors"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// errFakeRejected models a destination DB rejecting a statement (the GP6/PG9.4
// syntax error the old ON CONFLICT upsert produced).
var errFakeRejected = errors.New("relation does not accept ON CONFLICT (PG 9.4)")

// The primary journal (gpstream_units / gpstream_jobs) runs on the destination
// coordinator, which on GP6 is PostgreSQL 9.4. PG 9.4 has no
// `ON CONFLICT ... DO UPDATE` (added in 9.5), so the journal upsert must be a
// SELECT-by-PK followed by UPDATE or INSERT. The 07.10 e2e exposed the old
// upsert: it failed with a syntax error on GP6 that `_, _ =` swallowed, leaving
// both journal tables empty. These tests are the regression guard: no statement
// the journal issues may contain ON CONFLICT, the right branch is taken, and a
// write error is surfaced (not silently dropped).

// TestJournalSetStateNoOnConflictInsert covers the INSERT branch (row absent):
// the primary journal must issue a plain INSERT, never ON CONFLICT.
func TestJournalSetStateNoOnConflictInsert(t *testing.T) {
	f := pg.NewFakeConn()
	// No row yet for this (job_id, unit_id) -> the upsert must INSERT.
	f.On("SELECT 1 FROM gpstream_units", [][]any{})
	j := NewJournal(f, t.TempDir()+"/journal.jsonl")

	u := &Unit{JobID: "j1", UnitID: 7, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StatePending}
	if err := j.SetState(u); err != nil {
		t.Fatalf("SetState: %v", err)
	}

	log := strings.Join(f.Queried(), "\n")
	if strings.Contains(log, "ON CONFLICT") {
		t.Fatalf("journal issued ON CONFLICT (PG 9.5+; GP6=9.4 rejects it):\n%s", log)
	}
	if !strings.Contains(log, "INSERT INTO gpstream_units") {
		t.Fatalf("expected a plain INSERT when the row is absent, got:\n%s", log)
	}
	if strings.Contains(log, "UPDATE gpstream_units") {
		t.Fatalf("expected no UPDATE when the row is absent, got:\n%s", log)
	}
}

// TestJournalSetStateNoOnConflictUpdate covers the UPDATE branch (row present):
// the upsert must UPDATE the existing row, not re-INSERT.
func TestJournalSetStateNoOnConflictUpdate(t *testing.T) {
	f := pg.NewFakeConn()
	// A row already exists for this (job_id, unit_id) -> the upsert must UPDATE.
	f.On("SELECT 1 FROM gpstream_units", [][]any{{1}})
	j := NewJournal(f, t.TempDir()+"/journal.jsonl")

	u := &Unit{JobID: "j1", UnitID: 7, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted, Checksum: "ck"}
	if err := j.SetState(u); err != nil {
		t.Fatalf("SetState: %v", err)
	}

	log := strings.Join(f.Queried(), "\n")
	if strings.Contains(log, "ON CONFLICT") {
		t.Fatalf("journal issued ON CONFLICT (PG 9.5+; GP6=9.4 rejects it):\n%s", log)
	}
	if !strings.Contains(log, "UPDATE gpstream_units") {
		t.Fatalf("expected an UPDATE when the row is present, got:\n%s", log)
	}
	if strings.Contains(log, "INSERT INTO gpstream_units") {
		t.Fatalf("expected no INSERT when the row is present, got:\n%s", log)
	}
}

// TestJournalSetStateWriteErrorSurfaced guards the no-silent-failure contract:
// a primary-journal write failure must reach the Note sink (the Manager wires it
// to events.jsonl), not be swallowed by `_, _ =`.
func TestJournalSetStateWriteErrorSurfaced(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("SELECT 1 FROM gpstream_units", [][]any{{1}}) // row present -> UPDATE path
	f.SetExecError(errFakeRejected)
	j := NewJournal(f, t.TempDir()+"/journal.jsonl")

	var notes []string
	j.Note(func(msg string) { notes = append(notes, msg) })

	u := &Unit{JobID: "j1", UnitID: 7, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted}
	// The job must still complete (the file mirror is the resume source), but
	// the pg write failure must be reported, not swallowed.
	if err := j.SetState(u); err != nil {
		t.Fatalf("SetState should not fail on a pg journal error (mirror is durable): %v", err)
	}

	if len(notes) == 0 {
		t.Fatalf("expected a primary-journal write error to be surfaced via Note, got none")
	}
	joined := strings.Join(notes, "\n")
	if !strings.Contains(joined, "gpstream_units write failed") {
		t.Fatalf("expected the surfaced note to name the gpstream_units write failure, got:\n%s", joined)
	}
}
