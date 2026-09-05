package indexer

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/pg"
)

// TestRebuildIssuesReindex issues REINDEX TABLE (FR-17).
func TestRebuildIssuesReindex(t *testing.T) {
	cfg := config.Default()
	cfg.IndexWorkers = 2
	f := pg.NewFakeConn()
	b := New(&cfg)
	if err := b.Rebuild(context.Background(), f, "public", "orders"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	q := f.Queried()
	if len(q) == 0 || !contains(q[len(q)-1], "REINDEX TABLE") {
		t.Fatalf("expected REINDEX TABLE, got %v", q)
	}
}

// TestRebuildNoIndexes is a no-op when --no-indexes is set (FR-17).
func TestRebuildNoIndexes(t *testing.T) {
	cfg := config.Default()
	cfg.NoIndexes = true
	f := pg.NewFakeConn()
	b := New(&cfg)
	if err := b.Rebuild(context.Background(), f, "public", "orders"); err != nil {
		t.Fatalf("no-indexes rebuild should be a no-op: %v", err)
	}
	if len(f.Queried()) != 0 {
		t.Fatalf("no SQL should be issued with NoIndexes, got %v", f.Queried())
	}
}

// TestWorkersClampsToOne ensures the worker count is at least 1.
func TestWorkersClampsToOne(t *testing.T) {
	cfg := config.Default()
	cfg.IndexWorkers = 0 // below 1 -> clamped
	b := New(&cfg)
	if b.workers != 1 {
		t.Fatalf("workers = %d, want 1", b.workers)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
