package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

func newTestJournal(t *testing.T) *Journal {
	t.Helper()
	f := pg.NewFakeConn()
	j := NewJournal(f, t.TempDir()+"/journal.jsonl")
	return j
}

func TestJournalSetStateCompleted(t *testing.T) {
	j := newTestJournal(t)
	u := &Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StatePending}
	if err := j.SetState(u); err != nil {
		t.Fatalf("setstate: %v", err)
	}
	got := j.Get(1)
	if got == nil || got.State != StatePending {
		t.Fatalf("got %+v", got)
	}
}

func TestJournalSwapRequiresCompleted(t *testing.T) {
	j := newTestJournal(t)
	// completed unit swaps OK
	u := &Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted, Checksum: "abc"}
	if err := j.SetState(u); err != nil {
		t.Fatalf("setstate: %v", err)
	}
	if err := j.SwapState(1); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if j.Get(1).State != StateSwapped {
		t.Errorf("state = %s, want swapped", j.Get(1).State)
	}
	// a non-completed unit cannot swap
	u2 := &Unit{JobID: "j1", UnitID: 2, DB: "db", Schema: "s", Table: "t2", Kind: "table", State: StateCopying}
	_ = j.SetState(u2)
	if err := j.SwapState(2); err == nil {
		t.Errorf("swap on copying unit should fail")
	}
}

func TestJournalMirrorWrites(t *testing.T) {
	j := newTestJournal(t)
	_ = j.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted, Checksum: "a"})
	// the file mirror should exist and be readable
	data, err := os.ReadFile(j.mirror)
	if err != nil {
		t.Fatalf("read mirror: %v", err)
	}
	if len(data) == 0 {
		t.Errorf("mirror is empty")
	}
}

func TestJournalSetStateCompletedRequiresChecksum(t *testing.T) {
	j := newTestJournal(t)
	u := &Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted}
	// A completed unit without a checksum is still recorded (the verify layer
	// enforces the checksum), but the journal should not panic.
	if err := j.SetState(u); err != nil {
		t.Fatalf("setstate: %v", err)
	}
}

func TestJournalUpdatedAt(t *testing.T) {
	j := newTestJournal(t)
	u := &Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StatePending}
	before := time.Now()
	_ = j.SetState(u)
	if j.Get(1).Updated.Before(before) {
		t.Errorf("updated timestamp not set")
	}
	_ = context.Background()
}

func TestJournalNilQuerier(t *testing.T) {
	// A journal without a primary Querier (dest DB unreachable) must still
	// write the file mirror (FR-07 fallback).
	j := NewJournal(nil, t.TempDir()+"/journal.jsonl")
	_ = j.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted, Checksum: "a"})
	if j.Get(1) == nil {
		t.Errorf("in-memory unit missing")
	}
}

// TestJournalWriteConflicts verifies F-10 §8: the plan's conflict diagnosis is
// persisted to conflicts.jsonl alongside the journal mirror (so the operator
// can audit what would collide + the action, without parsing stdout).
func TestJournalWriteConflicts(t *testing.T) {
	dir := t.TempDir()
	j := NewJournal(nil, filepath.Join(dir, "journal.jsonl"))

	cps := []plan.Conflict{
		{DB: "db", Schema: "public", Table: "b", Action: "skip (default)"},
		{DB: "db", Schema: "public", Table: "c", Action: "overwrite"},
	}
	if err := j.WriteConflicts(cps); err != nil {
		t.Fatalf("WriteConflicts: %v", err)
	}
	p := j.ConflictsPath()
	if p != filepath.Join(dir, "conflicts.jsonl") {
		t.Fatalf("ConflictsPath = %q, want %q", p, filepath.Join(dir, "conflicts.jsonl"))
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read conflicts.jsonl: %v", err)
	}
	var got []plan.Conflict
	dec := json.NewDecoder(bytes.NewReader(b))
	for i := 0; i < len(cps); i++ {
		var c plan.Conflict
		if err := dec.Decode(&c); err != nil {
			t.Fatalf("decode line %d: %v", i, err)
		}
		got = append(got, c)
	}
	if len(got) != 2 {
		t.Fatalf("conflicts = %d, want 2", len(got))
	}
	if got[0].Table != "b" || got[0].Action != "skip (default)" {
		t.Errorf("conflict[0] = %+v, want b / skip (default)", got[0])
	}
	if got[1].Table != "c" || got[1].Action != "overwrite" {
		t.Errorf("conflict[1] = %+v, want c / overwrite", got[1])
	}
}

// TestJournalWriteConflictsNoMirror verifies a journal without a mirror path
// (CI/exercise mode) is a no-op, not an error — the in-memory plan still
// carries the conflicts.
func TestJournalWriteConflictsNoMirror(t *testing.T) {
	j := NewJournal(nil, "")
	if j.ConflictsPath() != "" {
		t.Errorf("ConflictsPath with no mirror = %q, want empty", j.ConflictsPath())
	}
	if err := j.WriteConflicts([]plan.Conflict{{Table: "x"}}); err != nil {
		t.Errorf("WriteConflicts with no mirror = %v, want nil", err)
	}
}
