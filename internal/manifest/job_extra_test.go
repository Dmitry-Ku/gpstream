package manifest

import (
	"os"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestCreateUnitAutoIDs checks CreateUnit assigns ids sequentially and persists
// them (covers the UnitID==0 branch and the SetState-success path).
func TestCreateUnitAutoIDs(t *testing.T) {
	j := newTestJournal(t)
	id1, err := j.CreateUnit(&Unit{JobID: "j1", DB: "db", Schema: "s", Table: "t1", Kind: "table"}, StatePending)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id2, err := j.CreateUnit(&Unit{JobID: "j1", DB: "db", Schema: "s", Table: "t2", Kind: "table"}, StatePending)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id1 != 1 || id2 != 2 {
		t.Errorf("ids = %d,%d want 1,2", id1, id2)
	}
	if j.Get(id1) == nil || j.Get(id2) == nil {
		t.Errorf("created units not in cache")
	}
}

// TestCreateUnitKeepsExplicitID checks a pre-set UnitID is honoured.
func TestCreateUnitKeepsExplicitID(t *testing.T) {
	j := newTestJournal(t)
	id, err := j.CreateUnit(&Unit{JobID: "j1", UnitID: 99, DB: "db", Schema: "s", Table: "t", Kind: "table"}, StatePending)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id != 99 {
		t.Errorf("id = %d, want 99", id)
	}
}

// TestMarkUnit sets state on a new/unknown unit (covers MarkUnit).
func TestMarkUnit(t *testing.T) {
	j := newTestJournal(t)
	if err := j.MarkUnit(&Unit{JobID: "j1", UnitID: 5, DB: "db", Schema: "s", Table: "t", Kind: "table"}, StateCopying); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if j.Get(5).State != StateCopying {
		t.Errorf("state = %s, want copying", j.Get(5).State)
	}
}

// TestGetMissing returns nil for an unknown unit (covers Get nil path).
func TestGetMissing(t *testing.T) {
	j := newTestJournal(t)
	if j.Get(42) != nil {
		t.Errorf("expected nil for unknown unit")
	}
}

// TestUnitsSnapshot returns independent copies (mutating a returned unit must
// not affect the journal).
func TestUnitsSnapshot(t *testing.T) {
	j := newTestJournal(t)
	_ = j.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted, Checksum: "a"})
	units := j.Units()
	if len(units) != 1 {
		t.Fatalf("got %d units, want 1", len(units))
	}
	units[0].State = StateFailed
	if j.Get(1).State != StateCompleted {
		t.Errorf("snapshot mutation leaked into journal")
	}
}

// TestSwapMissingUnit checks SwapState on an unknown id errors (covers the
// not-found branch).
func TestSwapMissingUnit(t *testing.T) {
	j := newTestJournal(t)
	if err := j.SwapState(777); err == nil {
		t.Errorf("swap on missing unit should fail")
	}
}

// TestCleanupRemovesMirrorFile verifies Cleanup truncates the file mirror
// (covers the os.Remove branch).
func TestCleanupRemovesMirrorFile(t *testing.T) {
	j := newTestJournal(t)
	_ = j.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StateCompleted, Checksum: "a"})
	if _, err := os.Stat(j.mirror); err != nil {
		t.Fatalf("mirror should exist: %v", err)
	}
	if err := j.Cleanup("j1"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Stat(j.mirror); !os.IsNotExist(err) {
		t.Errorf("mirror should be removed, stat err=%v", err)
	}
}

// TestMirrorWriteNoMirror checks a journal with no mirror path skips file
// writes without error (covers the mirror=="" branch).
func TestMirrorWriteNoMirror(t *testing.T) {
	j := NewJournal(pg.NewFakeConn(), "")
	if err := j.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table", State: StatePending}); err != nil {
		t.Fatalf("setstate: %v", err)
	}
	if j.Get(1) == nil {
		t.Errorf("unit not recorded in memory")
	}
}
