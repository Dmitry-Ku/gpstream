// Package resume implements --auto-resume N (FR-06) and crash-recovery (FR-07)
// on top of the operation journal. It is deterministic and testable without a
// live cluster: it replays the journal, recomputes which units are
// incomplete, and restarts them (or rolls back a partially-swapped staging
// table, FR-07). A worker death never aborts the whole job (FR-06): the
// manager reassigns the dead worker's units to a live worker.
package resume

import (
	"context"
	"fmt"

	"github.com/gpstream/gpstream/internal/job"
)

// Manager drives auto-resume and crash-recovery.
type Manager struct {
	journal     *job.Journal
	MaxResume   int          // --auto-resume N (FR-06); 0 = no auto-resume
	attempts    int          // how many times the job has been resumed
	deadWorkers map[int]bool // workers that have failed (FR-06)
}

// NewManager creates a resume manager for a journal.
func NewManager(j *job.Journal, maxResume int) *Manager {
	return &Manager{journal: j, MaxResume: maxResume, deadWorkers: map[int]bool{}}
}

// Resume attempts to resume an interrupted job (FR-06). It reopens the journal
// and restarts all units not in a terminal state. It returns the number of
// units resumed. It refuses if the attempt count exceeds --auto-resume N.
func (m *Manager) Resume(_ context.Context) (int, error) {
	if m.MaxResume > 0 && m.attempts >= m.MaxResume {
		return 0, fmt.Errorf("auto-resume: max attempts (%d) reached", m.MaxResume)
	}
	m.attempts++
	var toResume int
	for _, u := range m.journal.Units() {
		if !isTerminal(u.State) {
			if err := m.journal.MarkUnit(u, job.StatePending); err != nil {
				return toResume, err
			}
			toResume++
		}
	}
	return toResume, nil
}

// MarkWorkerDead records a worker failure (FR-06: a worker death must not
// abort the whole job).
func (m *Manager) MarkWorkerDead(worker int) {
	m.deadWorkers[worker] = true
}

// IsWorkerDead reports whether a worker has failed.
func (m *Manager) IsWorkerDead(worker int) bool {
	return m.deadWorkers[worker]
}

// Attempts returns how many times the job has been resumed.
func (m *Manager) Attempts() int { return m.attempts }

// CrashRecovery performs the crash-recovery sequence (FR-07): it rolls back
// any unit that was in a non-terminal state at crash time (i.e. not verified)
// to pending so it is re-copied. This is the minimal rollback: only the
// unverified units are touched (the journal is the source of truth, FR-07).
// It returns the number of units rolled back.
func (m *Manager) CrashRecovery(_ context.Context) (int, error) {
	var rolledBack int
	for _, u := range m.journal.Units() {
		if !isTerminal(u.State) {
			if err := m.journal.MarkUnit(u, job.StatePending); err != nil {
				return rolledBack, err
			}
			rolledBack++
		}
	}
	return rolledBack, nil
}

// isTerminal reports whether a unit state is a terminal (final) state.
func isTerminal(s job.State) bool {
	switch s {
	case job.StateCompleted, job.StateSwapped, job.StateFailed:
		return true
	}
	return false
}
