package manifest

import (
	"strconv"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/dest"
)

// stagingName mirrors the production naming scheme for job "job1".
func stagingName(unitID int64) string {
	return dest.StagingName("job1", unitID)
}

// cleanupUnit builds a journal unit with the given state; renamed units get a
// destination name that differs from the source.
func cleanupUnit(id int64, state State, renamed, conflict bool) *Unit {
	u := &Unit{
		JobID: "job1", UnitID: id, DB: "db", Schema: "s",
		Table: "t" + strconv.FormatInt(id, 10), Kind: "table", State: state,
	}
	if renamed {
		u.DstSchema = "s2"
		u.DstName = "r" + strconv.FormatInt(id, 10)
	}
	u.Conflict = conflict
	return u
}

func newCleanupJournal(t *testing.T, units ...*Unit) *Journal {
	t.Helper()
	j := NewJournal(nil, t.TempDir()+"/journal.jsonl")
	for _, u := range units {
		if err := j.SetState(u); err != nil {
			t.Fatalf("SetState(%d): %v", u.UnitID, err)
		}
	}
	return j
}

func findTarget(ts []DropTarget, kind, name string) *DropTarget {
	for i := range ts {
		if ts[i].Kind == kind && ts[i].Name == name {
			return &ts[i]
		}
	}
	return nil
}

func TestCleanupDestObjectsPendingHasNoObjects(t *testing.T) {
	// A unit that never started created nothing on the destination.
	j := newCleanupJournal(t, cleanupUnit(1, StatePending, false, false))
	if ts := CleanupDestObjects(j); len(ts) != 0 {
		t.Fatalf("pending unit: want no drop targets, got %v", ts)
	}
}

func TestCleanupDestObjectsStagingAfterKill(t *testing.T) {
	// kill -9 mid-copy: state copying, staging table exists on the dest.
	j := newCleanupJournal(t, cleanupUnit(1, StateCopying, false, false))
	ts := CleanupDestObjects(j)
	if s := findTarget(ts, "staging", stagingName(1)); s == nil {
		t.Fatalf("copying unit: staging target missing: %v", ts)
	}
	// same-name no-conflict: the planner saw the dest table absent, so
	// gpstream created it -> it is owned and droppable.
	if f := findTarget(ts, "table", "t1"); f == nil {
		t.Fatalf("created final table target missing: %v", ts)
	}
}

func TestCleanupDestObjectsConflictNeverDropped(t *testing.T) {
	// A conflicted same-name target pre-existed (operator data): NEVER
	// reported for removal, even though the unit copied into it.
	j := newCleanupJournal(t, cleanupUnit(1, StateSwapped, false, true))
	ts := CleanupDestObjects(j)
	if f := findTarget(ts, "table", "t1"); f != nil {
		t.Fatalf("conflicted pre-existing table must not be a drop target: %v", ts)
	}
	// The staging of the (already swapped) unit is still reported: the
	// DROP IF EXISTS is a no-op when the swap already consumed it.
	if s := findTarget(ts, "staging", stagingName(1)); s == nil {
		t.Fatalf("swapped unit: staging (no-op) target missing: %v", ts)
	}
}

func TestCleanupDestObjectsRenamedIn(t *testing.T) {
	j := newCleanupJournal(t, cleanupUnit(1, StateSwapped, true, false))
	ts := CleanupDestObjects(j)
	if f := findTarget(ts, "table", "r1"); f == nil {
		t.Fatalf("renamed-in final table missing: %v", ts)
	}
	if s := findTarget(ts, "schema", "s2"); s == nil {
		t.Fatalf("renamed-in schema missing: %v", ts)
	}
	// The source schema must never be a drop target.
	if s := findTarget(ts, "schema", "s"); s != nil {
		t.Fatalf("source schema must not be dropped: %v", ts)
	}
}

func TestCleanupDestObjectsPublicSchemaNeverDropped(t *testing.T) {
	// A rename INTO public: the schema is pre-existing, never dropped.
	u := &Unit{
		JobID: "job1", UnitID: 1, DB: "db", Schema: "s",
		Table: "t1", Kind: "table", State: StateSwapped,
		DstSchema: "public", DstName: "t1r",
	}
	j := newCleanupJournal(t, u)
	ts := CleanupDestObjects(j)
	if s := findTarget(ts, "schema", "public"); s != nil {
		t.Fatalf("public must never be a schema drop target: %v", ts)
	}
	if f := findTarget(ts, "table", "t1r"); f == nil {
		t.Fatalf("renamed-in table into public must still be droppable: %v", ts)
	}
}

func TestCleanupDestObjectsDedupAndOrder(t *testing.T) {
	// Two units sharing a renamed-in schema: the schema is reported ONCE,
	// and it sorts after all tables/staging (dropped last).
	j := newCleanupJournal(t,
		cleanupUnit(1, StateSwapped, true, false),
		cleanupUnit(2, StateSwapped, true, false),
	)
	ts := CleanupDestObjects(j)
	schemas := 0
	lastNonSchema := -1
	for i, x := range ts {
		if x.Kind == "schema" {
			schemas++
			if i <= lastNonSchema {
				t.Fatalf("schema must sort after tables/staging: %v", ts)
			}
		} else {
			lastNonSchema = i
		}
	}
	if schemas != 1 {
		t.Fatalf("want exactly 1 schema target, got %d: %v", schemas, ts)
	}
}

func TestCleanupDestObjectsAllTerminalStates(t *testing.T) {
	// Every terminal-ish state that reached the copy phase reports staging;
	// failed units are included (the kill -9 / unit-failure case).
	for _, st := range []State{StateCopying, StateVerifying, StateCompleted, StateSwapped, StateFailed} {
		j := newCleanupJournal(t, cleanupUnit(7, st, false, false))
		if s := findTarget(CleanupDestObjects(j), "staging", stagingName(7)); s == nil {
			t.Fatalf("state %s: staging target missing", st)
		}
	}
}
