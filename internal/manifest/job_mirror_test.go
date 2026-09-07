package manifest

import (
	"os"
	"testing"
)

// TestLoadFromMirrorRestoresState is the FR-07 crash-recovery guarantee: the
// file mirror (the durable fallback for the primary journal) must be read back
// on restart so an interrupted job resumes its real state, not an empty one.
// It simulates a crash by persisting units to a journal, then building a NEW
// journal over the SAME mirror path (as a restart would) and loading it.
func TestLoadFromMirrorRestoresState(t *testing.T) {
	dir := t.TempDir()
	mirror := dir + "/journal.jsonl"

	// 1st process: record units and their progression into the file mirror.
	j1 := NewJournal(nil, mirror)
	if err := j1.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t1", Kind: "table", State: StatePending}); err != nil {
		t.Fatalf("setstate 1: %v", err)
	}
	if err := j1.SetState(&Unit{JobID: "j1", UnitID: 2, DB: "db", Schema: "s", Table: "t2", Kind: "table", State: StatePending}); err != nil {
		t.Fatalf("setstate 2: %v", err)
	}
	// Unit 1 advances to completed, then to swapped — the mirror keeps the
	// latest record for each UnitID.
	if err := j1.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t1", Kind: "table", State: StateCompleted, Checksum: "ck1"}); err != nil {
		t.Fatalf("setstate 1 completed: %v", err)
	}
	if err := j1.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t1", Kind: "table", State: StateSwapped, Checksum: "ck1"}); err != nil {
		t.Fatalf("setstate 1 swapped: %v", err)
	}
	// The mirror file persists on disk (we deliberately do NOT call Cleanup,
	// which would delete it). This is what survives a crash.
	if _, err := os.Stat(mirror); err != nil {
		t.Fatalf("mirror file should exist after SetState: %v", err)
	}

	// Crash: a brand-new process sees an empty in-memory cache.
	j2 := NewJournal(nil, mirror)
	if len(j2.Units()) != 0 {
		t.Fatalf("fresh journal should start empty, got %d", len(j2.Units()))
	}

	// Restart: load the durable mirror.
	if err := j2.LoadFromMirror(); err != nil {
		t.Fatalf("load mirror: %v", err)
	}
	// 2 units recovered.
	units := j2.Units()
	if len(units) != 2 {
		t.Fatalf("loaded %d units, want 2", len(units))
	}
	// Unit 1 must be at its latest state (swapped) and carry its checksum;
	// unit 2 stays pending — proving the last-record-wins semantics.
	u1 := j2.Get(1)
	if u1 == nil || u1.State != StateSwapped || u1.Checksum != "ck1" {
		t.Fatalf("unit 1 = %+v, want swapped/ck1", u1)
	}
	u2 := j2.Get(2)
	if u2 == nil || u2.State != StatePending {
		t.Fatalf("unit 2 = %+v, want pending", u2)
	}
}

// TestLoadFromMirrorMissing is a no-op when the mirror has not been created
// yet (nothing to recover) and not an error.
func TestLoadFromMirrorMissing(t *testing.T) {
	j := NewJournal(nil, t.TempDir()+"/does-not-exist.jsonl")
	if err := j.LoadFromMirror(); err != nil {
		t.Fatalf("load missing mirror: %v", err)
	}
	if len(j.Units()) != 0 {
		t.Fatalf("expected no units, got %d", len(j.Units()))
	}
}

// TestLoadFromMirrorNoPath is a no-op when no mirror path is configured.
func TestLoadFromMirrorNoPath(t *testing.T) {
	j := NewJournal(nil, "")
	if err := j.LoadFromMirror(); err != nil {
		t.Fatalf("load with no path: %v", err)
	}
}

// TestLoadFromMirrorThenUnitsAreResumable proves the recovered units are the
// ones resume would act on: a fresh journal (empty in-memory) that loads the
// mirror yields the pending units resume re-queues. This is the exact read
// path cmdResume goes through (FR-07 durable resume).
func TestLoadFromMirrorThenUnitsAreResumable(t *testing.T) {
	dir := t.TempDir()
	mirror := dir + "/journal.jsonl"

	// 1st process: one completed, one still pending.
	j1 := NewJournal(nil, mirror)
	_ = j1.SetState(&Unit{JobID: "j1", UnitID: 1, DB: "db", Schema: "s", Table: "t1", Kind: "table", State: StateSwapped})
	_ = j1.SetState(&Unit{JobID: "j1", UnitID: 2, DB: "db", Schema: "s", Table: "t2", Kind: "table", State: StatePending})

	// Restart: a fresh journal, empty in-memory, loaded from the mirror.
	j2 := NewJournal(nil, mirror)
	if len(j2.Units()) != 0 {
		t.Fatalf("fresh journal should start empty")
	}
	if err := j2.LoadFromMirror(); err != nil {
		t.Fatalf("load: %v", err)
	}
	// resume re-queues every non-terminal unit: exactly unit 2 here.
	requeued := 0
	for _, u := range j2.Units() {
		if u.State != StateSwapped && u.State != StateFailed {
			requeued++
		}
	}
	if requeued != 1 {
		t.Fatalf("re-queued %d non-terminal units, want 1", requeued)
	}
}
