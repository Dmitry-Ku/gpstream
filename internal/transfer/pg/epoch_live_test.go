//go:build gp6lab

package pg

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/pgtest"
)

// startEpochLive brings up a real PG 9.4.26 cluster, a pool sized to serve the
// epoch (n + headroom), and returns both. The pool maxConns is n+4 to match the
// controller's --workers+4 sizing (P1d).
func startEpochLive(t *testing.T, port uint32, n int) (*pgtest.Cluster, *Pool) {
	t.Helper()
	cl, err := pgtest.Start(port, "gp_epoch")
	if err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(func() { cl.Stop() })
	ctx := context.Background()
	p, err := NewPool(ctx, cl.URL(), "", "", "", "", n+4)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Close)
	return cl, p
}

// TestLiveEpochPinAndDelta pins an N=3 snapshot epoch on a real PG and checks
// the ε-window width (Δ) is small (the N pins are taken back-to-back, ~ms) and
// passes the --max-snapshot-delta gate. This is the P1d foundation: N near-
// identical REPEATABLE READ snapshots, not one exact point.
func TestLiveEpochPinAndDelta(t *testing.T) {
	_, p := startEpochLive(t, 5661, 3)
	ctx := context.Background()

	e, err := NewEpoch(ctx, p, 3)
	if err != nil {
		t.Fatalf("NewEpoch(3): %v", err)
	}
	defer e.Close()

	if got := e.ConnCount(); got != 3 {
		t.Fatalf("ConnCount = %d, want 3", got)
	}
	if d := e.Delta(); d > 2*time.Second {
		t.Errorf("pin spread Δ = %s, want < 2s (live pins are back-to-back, ~ms)", d)
	}
	if err := e.ValidateDelta(1 * time.Second); err != nil {
		t.Errorf("ValidateDelta(1s) on a live ~ms spread: unexpected err %v (Δ=%s)", err, e.Delta())
	}
	// baseMin is the earliest original pin; FixTimes returns all N.
	ft := e.FixTimes()
	if len(ft) != 3 {
		t.Fatalf("FixTimes len = %d, want 3", len(ft))
	}
	if e.MinFix().After(ft[0]) && e.MinFix().After(ft[1]) && e.MinFix().After(ft[2]) {
		t.Error("MinFix is after all FixTimes, want the earliest")
	}
}

// TestLiveEpochDeltaReject constructs an epoch whose pins are deliberately
// spread wider than the limit (by nudging the recorded t_fix) and confirms
// ValidateDelta rejects it — the run must not start. This is the P1d
// "delta-валидация: пины сверх лимита → отказ старта" gate.
func TestLiveEpochDeltaReject(t *testing.T) {
	_, p := startEpochLive(t, 5662, 3)
	ctx := context.Background()

	e, err := NewEpoch(ctx, p, 3)
	if err != nil {
		t.Fatalf("NewEpoch(3): %v", err)
	}
	defer e.Close()
	// Force a wide spread: push one pin's recorded t_fix out by 3s past the
	// others. (We mutate the internal fix slice to simulate a pathological
	// ε-window; NewEpoch's real spread is ~ms.)
	e.mu.Lock()
	base := e.fix[0]
	e.fix[2] = base.Add(3 * time.Second)
	e.mu.Unlock()

	if err := e.ValidateDelta(1 * time.Second); err == nil {
		t.Error("ValidateDelta(1s) on a 3s spread: want error, got nil")
	}
	// A permissive limit (10s) accepts the same spread.
	if err := e.ValidateDelta(10 * time.Second); err != nil {
		t.Errorf("ValidateDelta(10s) on a 3s spread: unexpected err %v", err)
	}
	// Disabled check (0) accepts anything.
	if err := e.ValidateDelta(0); err != nil {
		t.Errorf("ValidateDelta(0) disabled: unexpected err %v", err)
	}
}

// TestLiveEpochConcurrentCopy is the P1d F-02 acceptance on the epoch: N
// pipelines stream N tables concurrently through N borrowed epoch connections,
// and a concurrent committed write (an INSERT that commits AFTER the epoch's
// pins) is NOT visible to any of the pinned snapshots. Each connection's
// REPEATABLE READ snapshot is frozen at its pin, so the concurrent write must
// not leak into any pipeline's data.
func TestLiveEpochConcurrentCopy(t *testing.T) {
	_, p := startEpochLive(t, 5663, 3)
	ctx := context.Background()

	// Seed three tables with the SAME committed content (one row each).
	for _, tbl := range []string{"et1", "et2", "et3"} {
		if _, err := p.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id int, v text)`, tbl)); err != nil {
			t.Fatalf("create %s: %v", tbl, err)
		}
		if _, err := p.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'seed')`, tbl)); err != nil {
			t.Fatalf("seed %s: %v", tbl, err)
		}
	}

	e, err := NewEpoch(ctx, p, 3)
	if err != nil {
		t.Fatalf("NewEpoch(3): %v", err)
	}
	defer e.Close()

	// All three pipelines hold a connection at once (N concurrent in-flight
	// reads). Acquire must hand out distinct connections.
	ecs := make([]*EpochConn, 3)
	for i := range ecs {
		ec, aerr := e.Acquire()
		if aerr != nil {
			t.Fatalf("Acquire[%d]: %v", i, aerr)
		}
		ecs[i] = ec
	}
	seen := map[int]bool{}
	for _, ec := range ecs {
		if seen[ec.Index()] {
			t.Fatalf("two pipelines got the same epoch connection (index %d)", ec.Index())
		}
		seen[ec.Index()] = true
	}

	// A concurrent committed write AFTER the pins: it must NOT be visible to
	// any pinned snapshot (FR-02 on the epoch).
	if _, err := p.Exec(ctx, `INSERT INTO et1 VALUES (2, 'concurrent')`); err != nil {
		t.Fatalf("concurrent insert: %v", err)
	}

	// Each pipeline streams its table via the borrowed connection's COPY.
	// et1 must read exactly 1 row (the seed) — the concurrent 2nd row is
	// invisible to its frozen snapshot. et2/et3 read their 1 seed row.
	// Run them concurrently (the point of the epoch).
	var wg sync.WaitGroup
	errs := make([]error, 3)
	wantRows := []int64{1, 1, 1}
	tables := []string{"et1", "et2", "et3"}
	for i := range ecs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			buf := &bytes.Buffer{}
			rows, cerr := ecs[i].ReadCopy(ctx, buf, fmt.Sprintf(`COPY public.%s TO STDOUT (FORMAT binary)`, tables[i]))
			if cerr != nil {
				errs[i] = fmt.Errorf("COPY %s: %v", tables[i], cerr)
				return
			}
			if rows != wantRows[i] {
				errs[i] = fmt.Errorf("COPY %s: rows = %d, want %d (F-02: the concurrent committed write leaked into the pinned snapshot)", tables[i], rows, wantRows[i])
				return
			}
			// The binary COPY payload is non-empty (1 row) and the same shape
			// every connection sees (the frozen seed row).
			if buf.Len() == 0 {
				errs[i] = fmt.Errorf("COPY %s: empty payload", tables[i])
			}
		}(i)
	}
	wg.Wait()
	for i, ec := range ecs {
		if errs[i] != nil {
			t.Errorf("pipeline %d: %v", i, errs[i])
		}
		ec.Release()
	}

	// A fresh (non-pinned) connection DOES see the concurrent row: the snapshot
	// isolation is per-transaction, not a global freeze.
	n, err := p.Query(ctx, `SELECT count(*) FROM et1`)
	if err != nil || n[0][0].(int64) != 2 {
		t.Errorf("fresh conn sees et1 = %v, want 2 (the concurrent write is visible outside the snapshot)", n)
	}
}

// TestLiveEpochHeal aborts one pinned connection's snapshot transaction, heals
// it (re-pin), and confirms: (a) the healed connection reads a fresh snapshot
// (it sees a write committed after the ORIGINAL pin), and (b) Heal validates the
// re-pin against the epoch base (baseMin). This is the P1d "heal пере-пина"
// path.
func TestLiveEpochHeal(t *testing.T) {
	_, p := startEpochLive(t, 5664, 2)
	ctx := context.Background()

	if _, err := p.Exec(ctx, `CREATE TABLE eh (id int)`); err != nil {
		t.Fatalf("create eh: %v", err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO eh VALUES (1)`); err != nil {
		t.Fatalf("seed eh: %v", err)
	}

	e, err := NewEpoch(ctx, p, 2)
	if err != nil {
		t.Fatalf("NewEpoch(2): %v", err)
	}
	defer e.Close()

	// Abort connection 0's snapshot transaction (the P1d failed-unit SQL fault).
	ec0, aerr := e.Acquire()
	if aerr != nil {
		t.Fatalf("Acquire[0]: %v", aerr)
	}
	if _, err := ec0.Exec(ctx, `INSERT INTO eh VALUES (1)`); err == nil {
		// A unique-constraint violation would be the real fault; force an
		// aborted tx with a definite error: reference a missing table.
		_, _ = ec0.Exec(ctx, `SELECT * FROM no_such_table_heal`)
	}
	// Confirm the tx is now aborted (any query errors with 25P02).
	if _, err := ec0.Query(ctx, `SELECT 1`); err == nil {
		t.Fatal("expected the snapshot tx to be aborted (25P02) before heal")
	}

	// A write committed AFTER the original pin, before the heal: the healed
	// snapshot must see it.
	if _, err := p.Exec(ctx, `INSERT INTO eh VALUES (2)`); err != nil {
		t.Fatalf("post-pin insert: %v", err)
	}

	// Heal connection 0 (re-pin a fresh REPEATABLE READ).
	nf, herr := e.Heal(0, 10*time.Second)
	if herr != nil {
		t.Fatalf("Heal(0, 10s): %v", err)
	}
	if nf.IsZero() {
		t.Error("Heal returned a zero t_fix")
	}

	// The healed connection reads a FRESH snapshot: it sees both committed rows.
	n, err := ec0.Query(ctx, `SELECT count(*) FROM eh`)
	if err != nil {
		t.Fatalf("healed conn query: %v", err)
	}
	if n[0][0].(int64) != 2 {
		t.Errorf("healed conn sees eh = %d, want 2 (the post-pin write must be visible to the re-pinned snapshot)", n[0][0].(int64))
	}
	ec0.Release()
}
