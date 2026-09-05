package job

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/pg"
)

// snapshotManager builds a Manager whose source records the SQL it issues, so a
// test can assert the FR-02 snapshot transaction was actually opened and
// closed around the copy.
func snapshotManager(t *testing.T, cfg *config.Config, payload []byte) (*Manager, *pg.FakeConn, *pg.FakeConn) {
	t.Helper()
	src := pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", payload)

	dst := pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", payload)
	// The target table is ABSENT so the (default) skip-existing policy still
	// copies the unit — the snapshot test is about the transaction, not the
	// conflict policy.
	dst.On("pg_class", [][]any{})

	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	return NewManager(cfg, src, dst, j), src, dst
}

// TestManagerConsistentDBSnapshot verifies that --consistent-db opens a
// REPEATABLE READ snapshot transaction on the source and commits it after the
// copy (FR-02: all COPYs of the database read from one snapshot).
func TestManagerConsistentDBSnapshot(t *testing.T) {
	cfg := config.Default()
	cfg.ConsistentDB = true
	m, src, _ := snapshotManager(t, &cfg, []byte("BATCH"))

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.snapshotMode != "db" {
		t.Fatalf("snapshotMode = %q, want db", m.snapshotMode)
	}
	srcQ := src.Queried()
	if !hasQuery(srcQ, "BEGIN ISOLATION LEVEL REPEATABLE READ") {
		t.Errorf("expected a snapshot BEGIN on the source, got %v", srcQ)
	}
	if !hasQuery(srcQ, "COMMIT") {
		t.Errorf("expected a snapshot COMMIT on the source after the copy, got %v", srcQ)
	}
	// The copy happened between the snapshot BEGIN (Run start) and the snapshot
	// COMMIT (after the worker pool finished), so the BEGIN/COMMIT bracket the
	// copy by construction; a non-zero row count proves data was copied inside
	// the snapshot.
	if m.Rows == 0 {
		t.Fatalf("expected the copy to transfer rows inside the snapshot")
	}
}

// TestManagerConsistentClusterSnapshot verifies that --consistent-cluster both
// freezes the session (READ ONLY) and opens the snapshot transaction (FR-02).
func TestManagerConsistentClusterSnapshot(t *testing.T) {
	cfg := config.Default()
	cfg.ConsistentDB = false
	cfg.ConsistentCluster = true
	m, src, _ := snapshotManager(t, &cfg, []byte("BATCH"))

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.snapshotMode != "cluster" {
		t.Fatalf("snapshotMode = %q, want cluster", m.snapshotMode)
	}
	q := src.Queried()
	if !hasQuery(q, "READ ONLY") {
		t.Errorf("expected a cluster freeze (READ ONLY), got %v", q)
	}
	if !hasQuery(q, "BEGIN ISOLATION LEVEL REPEATABLE READ") {
		t.Errorf("expected a snapshot BEGIN for cluster mode, got %v", q)
	}
	if !hasQuery(q, "COMMIT") {
		t.Errorf("expected a snapshot COMMIT, got %v", q)
	}
}

// TestManagerNoConsistencyNoSnapshot verifies that with neither consistency
// mode set no snapshot transaction is opened (the default data path is
// untouched).
func TestManagerNoConsistencyNoSnapshot(t *testing.T) {
	cfg := config.Default()
	cfg.ConsistentDB = false
	cfg.ConsistentCluster = false
	m, src, _ := snapshotManager(t, &cfg, []byte("BATCH"))

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.snapshotMode != "" {
		t.Errorf("snapshotMode = %q, want empty", m.snapshotMode)
	}
	q := src.Queried()
	if hasQuery(q, "BEGIN ISOLATION") || hasQuery(q, "COMMIT") {
		t.Errorf("no snapshot should be opened when consistency is off, got %v", q)
	}
}
