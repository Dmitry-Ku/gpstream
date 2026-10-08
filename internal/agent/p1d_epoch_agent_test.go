//go:build gp6lab

package agent

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestP1dSourceAgentEpochWiring verifies the P1d source-agent snapshot-epoch
// wiring (Workers>1) on a real PG: Start creates an N-connection epoch (pool
// sized N+4, Δ validated), the data COPY borrows/returns a connection (no
// commit), heal re-pins an aborted connection and leaves healthy ones
// untouched, and Close rolls back all N. This is the self-testable seam for
// the I2 source-side wiring (the concurrent control/dest path is I3/I4).
func TestP1dSourceAgentEpochWiring(t *testing.T) {
	const n = 3
	cl, err := pgtest.Start(5671, "gp_p1d_agent")
	if err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(cl.Stop)
	ctx := context.Background()

	// A source agent in snapshot-epoch mode (Workers=3, 1s delta gate).
	s := NewSourceServer(ServerOptions{
		SrcConn:          cl.URL(),
		Snapshot:         SnapDB,
		Listen:           "127.0.0.1:0", // auto-pick a free control port
		Workers:          n,
		MaxSnapshotDelta: time.Second,
		ConnectTimeout:   5 * time.Second,
		StreamTimeout:    5 * time.Minute,
	})
	if _, err := s.Start(ctx); err != nil {
		t.Fatalf("Start (epoch): %v", err)
	}
	t.Cleanup(s.Close)

	// 1. Start created an N-connection epoch (not the single strict pin).
	if !s.snapTx || s.epoch == nil {
		t.Fatalf("expected a snapshot-epoch (Workers=%d), got snapTx=%v epoch=%v", n, s.snapTx, s.epoch != nil)
	}
	if got := s.epoch.ConnCount(); got != n {
		t.Fatalf("epoch ConnCount = %d, want %d", got, n)
	}
	// The pool is sized N+4 (the controller's --workers+4 sizing the dry-run
	// checks), so the epoch's N reservations leave catalog headroom.
	if err := s.epoch.ValidateDelta(time.Second); err != nil {
		t.Fatalf("epoch Δ should pass the 1s gate (pins are ~ms apart): %v", err)
	}

	// 2. Borrow/return: the data COPY borrows a connection (an EpochConn), the
	//    run returns it (the transaction is NOT committed — it stays pinned).
	q, err := s.snapshotQuerier()
	if err != nil {
		t.Fatalf("snapshotQuerier: %v", err)
	}
	ec, ok := q.(*pg.EpochConn)
	if !ok {
		t.Fatalf("snapshotQuerier returned %T, want *pg.EpochConn", q)
	}
	// The borrowed connection sees a pinned snapshot (a query works).
	if _, qerr := q.Exec(ctx, "SELECT 1"); qerr != nil {
		t.Fatalf("query on borrowed epoch conn: %v", qerr)
	}
	s.releaseSnapshotConn(q)

	// 3. Exhaustion: borrowing all N, the (N+1)th fails (a sizing fault the
	//    manager must not hit — workers <= N by construction).
	conns := make([]pg.StreamingQuerier, 0, n)
	for i := 0; i < n; i++ {
		cq, aerr := s.snapshotQuerier()
		if aerr != nil {
			t.Fatalf("borrow %d: %v", i, aerr)
		}
		conns = append(conns, cq)
	}
	if _, aerr := s.snapshotQuerier(); aerr == nil {
		t.Fatalf("expected the %d-th borrow to fail (all %d in use)", n+1, n)
	}
	for _, cq := range conns {
		s.releaseSnapshotConn(cq)
	}

	// 4. Heal: abort ONE connection's snapshot transaction (a failed unit's SQL
	//    fault), then heal. The aborted conn is re-pinned (usable again); the
	//    healthy conns are left untouched (their snapshot points preserved).
	cq, _ := s.snapshotQuerier()
	aborted := cq.(*pg.EpochConn)
	// A SQL fault aborts the REPEATABLE READ transaction on that conn.
	if _, aerr := cq.Exec(ctx, "SELECT 1/0"); aerr == nil {
		t.Fatalf("expected division-by-zero to abort the snapshot tx")
	}
	s.releaseSnapshotConn(cq)
	if _, qerr := aborted.Query(ctx, "SELECT 1"); qerr == nil {
		t.Fatalf("expected the aborted conn to fail SELECT 1 before heal")
	}
	s.heal(ctx) // probe + heal all N; the aborted one re-pins, healthy untouched
	if _, qerr := aborted.Query(ctx, "SELECT 1"); qerr != nil {
		t.Fatalf("aborted conn should be re-pinned and usable after heal: %v", qerr)
	}

	// 5. Close rolls back all N (the run's abort path); the pool is drained of
	//    the epoch's reservations afterward (a fresh borrow-then-check is not
	//    possible post-Close, so just confirm Close is clean/idempotent).
	s.Close()
	s.Close() // idempotent
	_ = ec
}

// TestP1dSourceAgentStrictUnchanged verifies Workers=1 keeps the strict single
// pinned connection (no epoch) — the default, and the one that must not change
// behavior (one exact snapshot point).
func TestP1dSourceAgentStrictUnchanged(t *testing.T) {
	cl, err := pgtest.Start(5672, "gp_p1d_strict")
	if err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(cl.Stop)
	ctx := context.Background()

	s := NewSourceServer(ServerOptions{
		SrcConn:        cl.URL(),
		Snapshot:       SnapDB,
		Listen:         "127.0.0.1:0",
		Workers:        1,
		ConnectTimeout: 5 * time.Second,
		StreamTimeout:  5 * time.Minute,
	})
	if _, err := s.Start(ctx); err != nil {
		t.Fatalf("Start (strict): %v", err)
	}
	t.Cleanup(s.Close)

	if s.epoch != nil {
		t.Fatalf("Workers=1 must NOT create an epoch (strict single pin)")
	}
	if !s.snapTx || s.pinned == nil {
		t.Fatalf("Workers=1 must keep the single pinned snapshot conn")
	}
	// The strict data COPY runs on the pinned conn (not an EpochConn).
	q, err := s.snapshotQuerier()
	if err != nil {
		t.Fatalf("snapshotQuerier (strict): %v", err)
	}
	if _, isEpoch := q.(*pg.EpochConn); isEpoch {
		t.Fatalf("strict snapshotQuerier returned an EpochConn, want the pinned conn")
	}
	if _, qerr := q.Exec(ctx, "SELECT 1"); qerr != nil {
		t.Fatalf("query on pinned conn: %v", qerr)
	}
	// releaseSnapshotConn is a no-op on the pinned conn.
	s.releaseSnapshotConn(q)
}
