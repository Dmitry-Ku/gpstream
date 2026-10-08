package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// FR-17 journal gap (10K stress finding: index_done=0 after a 54-minute run).
// The index_done flag is carried by the Unit struct (persisted in the file
// mirror), but the PRIMARY journal (gpstream_units, the destination DB table)
// had no index_done column: the upsert wrote 14 columns. On a journal table
// created before the flag existed the index_done value was never recorded in
// the DB table. These tests pin the schema and the upsert to 15 columns and
// the guarded migration for pre-existing tables (PG 9.4: no ADD COLUMN IF NOT
// EXISTS, so the guard is a DO block with a duplicate_column handler).

// TestJournalInitCreatesIndexDoneColumn: a fresh journal table carries the
// index_done column, and the migration statement is always issued (it is a
// no-op via the exception handler when the column already exists).
func TestJournalInitCreatesIndexDoneColumn(t *testing.T) {
	f := pg.NewFakeConn()
	j := NewJournal(f, t.TempDir()+"/journal.jsonl")
	if err := j.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	log := strings.Join(f.Queried(), "\n")
	if !strings.Contains(log, "CREATE TABLE IF NOT EXISTS gpstream_units") {
		t.Fatalf("expected the journal CREATE TABLE, got:\n%s", log)
	}
	if !strings.Contains(log, "index_done") {
		t.Fatalf("journal CREATE TABLE is missing the index_done column:\n%s", log)
	}
	if !strings.Contains(log, "EXCEPTION WHEN duplicate_column") {
		t.Fatalf("journal migration must be guarded (PG 9.4 has no ADD COLUMN IF NOT EXISTS):\n%s", log)
	}
}

// TestJournalSetStateWritesIndexDone: the upsert carries index_done in both
// branches (row absent -> INSERT 15 cols; row present -> UPDATE index_done).
func TestJournalSetStateWritesIndexDone(t *testing.T) {
	// INSERT branch: no row yet.
	f := pg.NewFakeConn()
	f.On("SELECT 1 FROM gpstream_units", [][]any{})
	j := NewJournal(f, t.TempDir()+"/journal.jsonl")
	u := &Unit{JobID: "j1", UnitID: 7, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateSwapped, IndexDone: true}
	if err := j.SetState(u); err != nil {
		t.Fatalf("SetState (insert): %v", err)
	}
	log := strings.Join(f.Queried(), "\n")
	if !strings.Contains(log, "INSERT INTO gpstream_units") {
		t.Fatalf("expected an INSERT when the row is absent, got:\n%s", log)
	}
	if !strings.Contains(log, "index_done") {
		t.Fatalf("journal INSERT must carry index_done (FR-17), got:\n%s", log)
	}

	// UPDATE branch: a row already exists.
	f2 := pg.NewFakeConn()
	f2.On("SELECT 1 FROM gpstream_units", [][]any{{1}})
	j2 := NewJournal(f2, t.TempDir()+"/journal.jsonl")
	u2 := &Unit{JobID: "j1", UnitID: 7, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateSwapped, IndexDone: true}
	if err := j2.SetState(u2); err != nil {
		t.Fatalf("SetState (update): %v", err)
	}
	log2 := strings.Join(f2.Queried(), "\n")
	if !strings.Contains(log2, "UPDATE gpstream_units") {
		t.Fatalf("expected an UPDATE when the row is present, got:\n%s", log2)
	}
	if !strings.Contains(log2, "index_done = $9") {
		t.Fatalf("journal UPDATE must set index_done (FR-17), got:\n%s", log2)
	}
}
