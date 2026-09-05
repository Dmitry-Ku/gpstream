package resume

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gpstream/gpstream/internal/job"
	"github.com/gpstream/gpstream/internal/pg"
)

func newMgr(t *testing.T, maxResume int) *Manager {
	t.Helper()
	f := pg.NewFakeConn()
	j := job.NewJournal(f, filepath.Join(t.TempDir(), "journal.json"))
	return NewManager(j, maxResume)
}

func seedUnits(t *testing.T, j *job.Journal) {
	t.Helper()
	// Unit 1: completed (terminal).
	if _, err := j.CreateUnit(&job.Unit{
		JobID: "j", UnitID: 1, DB: "db", Schema: "s", Table: "t", Kind: "table",
		Bytes: 1000,
	}, job.StateCompleted); err != nil {
		t.Fatalf("create 1: %v", err)
	}
	// Unit 2: left in-flight (non-terminal).
	if _, err := j.CreateUnit(&job.Unit{
		JobID: "j", UnitID: 2, DB: "db", Schema: "s", Table: "t2", Kind: "table",
		Bytes: 500,
	}, job.StateCopying); err != nil {
		t.Fatalf("create 2: %v", err)
	}
}

func TestResumeRestartsInflight(t *testing.T) {
	m := newMgr(t, 5)
	seedUnits(t, m.journal)
	n, err := m.Resume(context.Background())
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n != 1 {
		t.Errorf("resumed %d, want 1 (only the non-terminal unit)", n)
	}
	if m.Attempts() != 1 {
		t.Errorf("attempts = %d, want 1", m.Attempts())
	}
}

func TestResumeRespectsMax(t *testing.T) {
	m := newMgr(t, 2)
	seedUnits(t, m.journal)
	for i := 0; i < 2; i++ {
		if _, err := m.Resume(context.Background()); err != nil {
			t.Fatalf("resume %d: %v", i, err)
		}
	}
	// Third resume must be refused.
	if _, err := m.Resume(context.Background()); err == nil {
		t.Error("expected max-resume refusal")
	}
}

func TestCrashRollback(t *testing.T) {
	m := newMgr(t, 5)
	seedUnits(t, m.journal)
	n, err := m.CrashRecovery(context.Background())
	if err != nil {
		t.Fatalf("crash recovery: %v", err)
	}
	if n != 1 {
		t.Errorf("rolled back %d, want 1", n)
	}
}

func TestWorkerDeathDoesNotAbort(t *testing.T) {
	m := newMgr(t, 5)
	if m.IsWorkerDead(3) {
		t.Fatal("worker 3 should be alive initially")
	}
	m.MarkWorkerDead(3)
	if !m.IsWorkerDead(3) {
		t.Fatal("worker 3 should be marked dead")
	}
	// The manager still functions; resume is unaffected by a dead worker
	// (FR-06: a worker death does not abort the whole job).
	seedUnits(t, m.journal)
	if _, err := m.Resume(context.Background()); err != nil {
		t.Fatalf("resume after worker death: %v", err)
	}
}

func TestIsTerminal(t *testing.T) {
	cases := []struct {
		s    job.State
		want bool
	}{
		{job.StateCompleted, true},
		{job.StateSwapped, true},
		{job.StateFailed, true},
		{job.StatePending, false},
		{job.StateCopying, false},
		{job.StateVerifying, false},
	}
	for _, c := range cases {
		if got := isTerminal(c.s); got != c.want {
			t.Errorf("isTerminal(%q) = %v, want %v", c.s, got, c.want)
		}
	}
}
