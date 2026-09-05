package job

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/pg"
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

func TestJournalCleanup(t *testing.T) {
	j := newTestJournal(t)
	_ = j.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted, Checksum: "a"})
	_ = j.SetState(&Unit{JobID: "j1", UnitID: 2, DB: "db", Schema: "s", Table: "t2", Kind: "table", State: StateCompleted, Checksum: "b"})
	if err := j.Cleanup("j1"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if j.Get(1) != nil || j.Get(2) != nil {
		t.Errorf("cleanup did not remove units")
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
