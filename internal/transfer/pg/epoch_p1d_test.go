//go:build gp6lab

package pg

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/pgtest"
)

// TestP1dEpochCommitAllDivergent exercises the three Epoch methods the P1d
// source-agent wiring added (CommitAll, MarkDivergent, ProbeAndHeal) on a real
// PG 9.4.26.
func TestP1dEpochCommitAllDivergent(t *testing.T) {
	cl, err := pgtest.Start(5673, "gp_p1d_epoch")
	if err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(cl.Stop)
	ctx := context.Background()

	const n = 3
	p, err := NewPool(ctx, cl.URL(), "", "", "", "", n+4)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Close)

	e, err := NewEpoch(ctx, p, n)
	if err != nil {
		t.Fatalf("NewEpoch: %v", err)
	}
	t.Cleanup(e.Close) // on failure: release the epoch's conns so p.Close() cannot hang

	// ProbeAndHeal on a HEALTHY connection is a no-op (its snapshot point is
	// preserved — the ε-window is not widened).
	if err := e.ProbeAndHeal(0, time.Second); err != nil {
		t.Fatalf("ProbeAndHeal on healthy conn: %v", err)
	}

	// ProbeAndHeal on an ABORTED connection re-pins it (usable again). Borrow
	// conn 1, abort its transaction (a failed unit's SQL fault), return it.
	c1, err := e.Acquire()
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	idx := c1.Index() // Acquire hands out the first FREE conn (not necessarily #1)
	if _, aerr := c1.Exec(ctx, "SELECT 1/0"); aerr == nil {
		t.Fatalf("expected division-by-zero to abort conn %d's snapshot tx", idx)
	}
	c1.Release()
	if _, qerr := c1.Query(ctx, "SELECT 1"); qerr == nil {
		t.Fatalf("expected aborted conn %d to fail SELECT 1 before heal", idx)
	}
	if err := e.ProbeAndHeal(idx, time.Second); err != nil {
		t.Fatalf("ProbeAndHeal on aborted conn: %v", err)
	}
	if _, qerr := c1.Query(ctx, "SELECT 1"); qerr != nil {
		t.Fatalf("aborted conn %d should be re-pinned and usable after ProbeAndHeal: %v", idx, qerr)
	}

	// MarkDivergent excludes a connection from the free-list (it must not carry
	// a restarted unit after a divergent heal). Borrow the other two, then a
	// third borrow must fail (only conn 0/1 are free, conn 2 is divergent).
	e.MarkDivergent(2)
	borrowed := make([]*EpochConn, 0, 2)
	for i := 0; i < 2; i++ {
		c, aerr := e.Acquire()
		if aerr != nil {
			t.Fatalf("borrow %d after MarkDivergent(2): %v", i, aerr)
		}
		borrowed = append(borrowed, c)
	}
	if _, aerr := e.Acquire(); aerr == nil {
		t.Fatalf("expected the 3rd borrow to fail (conn 2 divergent, 0/1 in use)")
	}
	for _, c := range borrowed {
		c.Release()
	}

	// CommitAll commits all N (the run's successful Release) and returns the
	// connections to the pool. After it, the epoch is closed (a borrow fails).
	e.CommitAll()
	if _, aerr := e.Acquire(); aerr == nil {
		t.Fatalf("expected borrow after CommitAll to fail (epoch closed)")
	}
	// Idempotent.
	e.CommitAll()
}
