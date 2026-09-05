package index

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/pg"
)

func TestRebuildBySize(t *testing.T) {
	f := pg.NewFakeConn()
	r := New(f, PrioritySize)
	r.Enqueue(Index{Table: "t", Index: "small", Bytes: 100})
	r.Enqueue(Index{Table: "t", Index: "big", Bytes: 1000})
	r.Enqueue(Index{Table: "t", Index: "mid", Bytes: 500})
	n, err := r.RebuildAll(context.Background())
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if n != 3 {
		t.Errorf("rebuilt %d, want 3", n)
	}
	// The largest index should have been attempted first: its SQL appears
	// first in the recorded query list.
	qs := f.Queried()
	if !contains(qs, "big") || !firstContains(qs, "big") {
		t.Errorf("expected 'big' index first, got %v", qs)
	}
}

func TestRebuildByUsage(t *testing.T) {
	f := pg.NewFakeConn()
	r := New(f, PriorityUsage)
	r.Enqueue(Index{Table: "t", Index: "a", Usage: 1})
	r.Enqueue(Index{Table: "t", Index: "b", Usage: 100})
	if _, err := r.RebuildAll(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	qs := f.Queried()
	if !firstContains(qs, "b") {
		t.Errorf("expected 'b' (highest usage) first, got %v", qs)
	}
}

func TestRebuildFailureDoesNotAbort(t *testing.T) {
	f := pg.NewFakeConn()
	// Make the second exec fail by having the fake return an error on the
	// second call. The Rebuilder is best-effort: a failure on one index must
	// not abort the others (FR-17).
	r := New(f, PrioritySize)
	r.Enqueue(Index{Table: "t", Index: "a", Bytes: 10})
	r.Enqueue(Index{Table: "t", Index: "b", Bytes: 20})
	// b will fail (unregistered exec -> the fake errors), but a still rebuilds.
	if _, err := r.RebuildAll(context.Background()); err != nil {
		t.Fatalf("rebuild must not abort on a single failure: %v", err)
	}
}

func TestRebuildIdempotent(t *testing.T) {
	f := pg.NewFakeConn()
	r := New(f, PrioritySize)
	r.Enqueue(Index{Table: "t", Index: "x", Bytes: 1})
	_, _ = r.RebuildAll(context.Background())
	// Second run must not re-issue the rebuild (idempotent, FR-17).
	qsBefore := f.Queried()
	_, _ = r.RebuildAll(context.Background())
	qsAfter := f.Queried()
	if len(qsAfter) != len(qsBefore) {
		t.Errorf("rebuild not idempotent: %d -> %d queries", len(qsBefore), len(qsAfter))
	}
}

func contains(qs []string, sub string) bool {
	for _, q := range qs {
		for i := 0; i+len(sub) <= len(q); i++ {
			if q[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}

// firstContains reports whether the FIRST recorded query contains sub — used to
// assert the highest-priority index was rebuilt first (FR-17 ordering).
func firstContains(qs []string, sub string) bool {
	if len(qs) == 0 {
		return false
	}
	q := qs[0]
	for i := 0; i+len(sub) <= len(q); i++ {
		if q[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
