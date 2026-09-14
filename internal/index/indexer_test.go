package index

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// unitsInOrder returns the unit ids in the order their CREATE INDEX
// statements were issued against f (recovered from the index name, which is
// "u<N>").
func unitsInOrder(f *pg.FakeConn) []int {
	var out []int
	for _, q := range f.Queried() {
		if !strings.Contains(q, "CREATE INDEX") && !strings.Contains(q, "CREATE UNIQUE INDEX") {
			continue
		}
		i := strings.Index(q, "u")
		if i < 0 {
			continue
		}
		for _, c := range q[i+1:] {
			if c < '0' || c > '9' {
				break
			}
			out = append(out, int(c-'0'))
			break
		}
	}
	return out
}

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

// jobFor builds an index Job on the shared fake f; the index name "u<N>"
// lets tests recover the execution order from f.Queried().
func jobFor(f *pg.FakeConn, unit int) Job {
	f.On("pg_class", [][]any{})
	a := dest.New(f)
	u := "u" + string(rune('0'+unit))
	return Job{
		UnitID:    uint32(unit),
		Dst:       a,
		Schema:    "public",
		Table:     "t",
		SrcSchema: "public",
		SrcTable:  "t",
		Defs:      []pg.IndexDef{{Name: u, Def: "CREATE INDEX " + u + " ON public.t (id)"}},
	}
}

// TestPoolSizePriority drains the highest-priority (largest) jobs first with
// a single worker, so the observed execution order is the queue order
// (--index-priority=size, F-17).
func TestPoolSizePriority(t *testing.T) {
	f := pg.NewFakeConn()
	p := NewPool(1, false, true)
	j1 := jobFor(f, 1)
	j2 := jobFor(f, 2)
	j3 := jobFor(f, 3)
	j1.Priority = 10
	j2.Priority = 30
	j3.Priority = 20
	p.Enqueue(j1)
	p.Enqueue(j2)
	p.Enqueue(j3)
	p.Close()
	p.Run(context.Background())
	if p.Done() != 3 || p.Err() != nil {
		t.Fatalf("pool done=%d err=%v, want 3/nil", p.Done(), p.Err())
	}
	if got := unitsInOrder(f); len(got) != 3 || got[0] != 2 || got[1] != 3 || got[2] != 1 {
		t.Fatalf("size priority order = %v, want [2 3 1]", got)
	}
}

// TestPoolFIFO runs jobs in enqueue order with --index-priority=manual.
func TestPoolFIFO(t *testing.T) {
	f := pg.NewFakeConn()
	p := NewPool(1, false, false)
	p.Enqueue(jobFor(f, 1))
	p.Enqueue(jobFor(f, 2))
	p.Enqueue(jobFor(f, 3))
	p.Close()
	p.Run(context.Background())
	if p.Done() != 3 || p.Err() != nil {
		t.Fatalf("pool done=%d err=%v, want 3/nil", p.Done(), p.Err())
	}
	if got := unitsInOrder(f); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("manual order = %v, want [1 2 3]", got)
	}
}

// TestPoolNoIndexes drops every job (--no-indexes, FR-17): nothing is
// enqueued and no SQL is issued.
func TestPoolNoIndexes(t *testing.T) {
	f := pg.NewFakeConn()
	p := NewPool(1, true, false)
	p.Enqueue(jobFor(f, 1))
	if p.Len() != 0 {
		t.Fatalf("queue depth = %d with NoIndexes, want 0", p.Len())
	}
	p.Close()
	p.Run(context.Background())
	if p.Done() != 0 {
		t.Fatalf("done = %d with NoIndexes, want 0", p.Done())
	}
	if len(f.Queried()) != 0 {
		t.Fatalf("no SQL should be issued with NoIndexes, got %v", f.Queried())
	}
}

// TestPoolIdempotent enqueues the same unit twice (a resume re-run). The
// second CreateIndexes call must REINDEX the already-created index rather
// than re-issue CREATE INDEX (F-17 resume-safety).
func TestPoolIdempotent(t *testing.T) {
	f := pg.NewFakeConn()
	p := NewPool(1, false, false)
	j := jobFor(f, 1)
	// jobFor resets the pg_class stub to empty; mark the index as already
	// present so CreateIndexes takes the REINDEX branch (no duplicate
	// CREATE INDEX on a resume re-run, F-17).
	f.On("pg_class", [][]any{{1}})
	p.Enqueue(j)
	p.Enqueue(j) // same unit again
	p.Close()
	p.Run(context.Background())
	if p.Done() != 2 || p.Err() != nil {
		t.Fatalf("pool done=%d err=%v, want 2/nil", p.Done(), p.Err())
	}
	q := f.Queried()
	sawCreate, sawReindex := false, false
	for _, sql := range q {
		if strings.Contains(sql, "CREATE INDEX") || strings.Contains(sql, "CREATE UNIQUE INDEX") {
			sawCreate = true
		}
		if strings.Contains(sql, "REINDEX INDEX") {
			sawReindex = true
		}
	}
	if sawCreate {
		t.Fatalf("pool must not re-CREATE an existing index (duplicates): %v", q)
	}
	if !sawReindex {
		t.Fatalf("pool should REINDEX an existing index on re-run, got %v", q)
	}
}

// TestPoolWorkersDefault clamps the worker count to the F-17 default of 2.
func TestPoolWorkersDefault(t *testing.T) {
	p := NewPool(0, false, false)
	if p.workers != 2 {
		t.Fatalf("workers = %d, want default 2", p.workers)
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
