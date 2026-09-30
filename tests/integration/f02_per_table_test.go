//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/jackc/pgx/v5"
)

// TestPerTableFallbackSnapshot — FR-02 per-table fallback (--consistent-tables,
// spec F-02: «допустимый режим "по таблицам" (каждая таблица — свой короткий
// снимок, расхождение задокументировано) — только как явный fallback-флаг»).
//
// The discriminating property of the fallback is that EACH table is read under
// its OWN short snapshot, so two tables can legitimately see DIFFERENT points
// in the source history (the documented per-table discrepancy). A run-wide
// snapshot (the default --consistent-db) pins ONE point before any table is
// copied, so every table would see the SAME point.
//
// Two tables t_a (copied first) and t_b (copied second), serial workers. The
// writer inserts into BOTH tables AFTER t_a's snapshot is pinned but BEFORE t_b
// starts (t_b is copied after t_a completes). Then:
//   - dst t_a EXCLUDES the writer rows (inserted after t_a's snapshot), and
//     holds exactly the pre-copy rows.
//   - dst t_b INCLUDES the writer rows (inserted before t_b's snapshot).
//
// t_b containing the writer rows is impossible under a run-wide snapshot
// (which would have been pinned before the run, excluding them from every
// table). So this asserts the per-table semantics, not just "the copy works".
//
// It also implicitly verifies the checksum read runs on the SAME per-table
// snapshot as the data COPY: t_a's source checksum (step 6) must not see the
// writer rows, or the verify would mismatch the destination and fail the run.
func TestPerTableFallbackSnapshot(t *testing.T) {
	src, dst, srcCl := startPGPair(t, 5661, 5662)
	defer func() { src.Close(); dst.Close() }()

	const (
		n      = 10_000  // seed rows per table (ids 1..n)
		writes = 500     // writer rows per table (ids >= 100000)
		idBase = 100_000 // writer ids start here (well above the seed range)
	)
	ctx := context.Background()
	for _, name := range []string{"t_a", "t_b"} {
		mustExec(t, src, `CREATE TABLE public.`+name+` (id int, val text)`)
		if _, err := src.Exec(ctx,
			`INSERT INTO public.`+name+` SELECT g, 'v' || g FROM generate_series(1,`+itoa(n)+`) g`); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	cfg := config.Default()
	// Per-table fallback: NO run-wide snapshot, one short snapshot per table.
	cfg.ConsistentDB = false
	cfg.ConsistentCluster = false
	cfg.ConsistentTables = true
	// Serial workers: t_a is fully processed (copy+verify+swap) before t_b
	// starts, so t_b's snapshot is taken after the writer's inserts.
	cfg.WorkersPerPair = 1
	// Stretch the wire phase so t_a is still copying when the writer fires.
	cfg.RateLimit = 0.1
	cfg.Verify = config.VerifyFull

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 2 {
		t.Fatalf("plan has %d units, want 2", len(p.Units))
	}
	// t_a must be unit 1 (copied first): the plan sorts by (schema, name).
	if p.Units[0].Name != "t_a" || p.Units[1].Name != "t_b" {
		t.Fatalf("plan order = %s,%s; want t_a,t_b", p.Units[0].Name, p.Units[1].Name)
	}

	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")
	mgr := manifest.NewManager(&cfg, src, dst, j)

	runDone := make(chan error, 1)
	go func() { runDone <- mgr.Run(context.Background(), p) }()

	// Wait until t_a leaves pending: its per-table snapshot is pinned and its
	// data COPY is streaming. t_b has NOT started yet (serial, t_a first).
	snapshotLive := false
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for _, u := range j.Units() {
			if u.Table == "t_a" && u.State != manifest.StatePending {
				snapshotLive = true
				break
			}
		}
		if snapshotLive {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !snapshotLive {
		t.Fatalf("t_a never left pending within 60s; cannot guarantee writer fires post-snapshot")
	}

	// Fire the writer NOW: after t_a's snapshot (t_a is copying), before t_b
	// starts (t_b is copied after t_a completes). Insert into BOTH tables.
	var stop atomic.Bool
	var committed int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		wc, err := pgx.Connect(context.Background(), srcCl.URL())
		if err != nil {
			return
		}
		defer wc.Close(context.Background())
		for _, name := range []string{"t_a", "t_b"} {
			if _, err := wc.Exec(context.Background(),
				`INSERT INTO public.`+name+` SELECT `+itoa(idBase)+` + g, 'w' || g FROM generate_series(1,`+itoa(writes)+`) g`); err == nil {
				atomic.AddInt64(&committed, int64(writes))
			}
		}
		stop.Store(true)
	}()

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("manager.Run: %v", err)
		}
	case <-time.After(300 * time.Second):
		t.Fatalf("manager.Run did not return within 300s (data-path deadlock?)")
	}
	stop.Store(true)
	<-writerDone

	if committed != 2*int64(writes) {
		t.Fatalf("writer committed %d rows, want %d (test is vacuous)", committed, 2*writes)
	}

	// t_a: EXCLUDES the writer rows (inserted after t_a's snapshot) and holds
	// exactly the pre-copy rows.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t_a WHERE id >= `+itoa(idBase)); got != 0 {
		t.Fatalf("t_a has %d writer rows — per-table snapshot leaked (writer rows must be excluded)", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t_a`); got != n {
		t.Fatalf("t_a has %d rows, want %d (pre-copy)", got, n)
	}

	// t_b: INCLUDES the writer rows (inserted before t_b's snapshot) plus the
	// pre-copy rows. This is the discriminating assertion: impossible under a
	// run-wide snapshot pinned before the run.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t_b WHERE id >= `+itoa(idBase)); got != writes {
		t.Fatalf("t_b has %d writer rows, want %d (per-table snapshot must include them)", got, writes)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t_b`); got != n+int64(writes) {
		t.Fatalf("t_b has %d rows, want %d (pre-copy + writer)", got, n+writes)
	}
}
