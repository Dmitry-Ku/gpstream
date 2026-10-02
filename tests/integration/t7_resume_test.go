//go:build gp6lab

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// T7 (DESIGN §6): live crash-recovery + resume strict/relaxed scenario
// (FR-06/FR-07, T5 DESIGN §2.2) on real PG 9.4.26 clusters.
//
// A job with two tables is cancelled mid-copy (simulating a crash, FR-07 —
// the operator re-invokes `gpstream run` with the same operation id, the
// documented recovery path) after the small table has already been swapped
// and while the big table is in flight:
//
//   - run 2 (relaxed, default): the swapped table is KEPT (verified in one
//     snapshot epoch, DESIGN §2.2) and not re-copied; the in-flight table is
//     RE-READ under the fresh snapshot (its surviving partial staging is
//     cleared first, a56be8b — no duplicates).
//   - run 3 (strict): EVERY table is re-read under the fresh snapshot.
//
// Final counts must be exact in both modes (idempotence, no duplicates).
func TestResumeStrictRelaxedLive(t *testing.T) {
	ctx := context.Background()
	const (
		nKeep   = 5_000      // small: swaps within seconds of the run start
		nReRead = 20_000_000 // ~1 GB: at ~100 MB/s the copy takes ~10 s, so the
		// cancel (a few seconds after both units are up) lands mid-copy.
	)

	srcCl, dstCl, src, dst := startPGPairFull(t, 5541, 5542)
	defer func() { src.Close(); dst.Close() }()

	mustExec(t, src, `CREATE TABLE public.t_keep (id int, v text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.t_keep SELECT g, 'k' || g FROM generate_series(1,`+itoa(nKeep)+`) g`); err != nil {
		t.Fatalf("seed keep: %v", err)
	}
	mustExec(t, src, `CREATE TABLE public.t_reread (id int, v text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.t_reread SELECT g, repeat('r', 40) || g FROM generate_series(1,`+itoa(nReRead)+`) g`); err != nil {
		t.Fatalf("seed reread: %v", err)
	}

	base := config.Default()
	// 1 worker: serial units (t_keep = unit 1, t_reread = unit 2, by name).
	base.WorkersPerPair = 1
	stateDir := t.TempDir()

	// --- Run 1: cancelled mid-copy of t_reread, after t_keep is swapped. ---
	srcP, dstP := poolsFor(t, srcCl, dstCl)
	cfg1 := base
	p1 := buildPlan(t, cfg1, srcP, dstP)
	j1 := manifest.NewJournal(dstP, stateDir+"/journal.jsonl")
	m1 := newTwoAgentManager(t, &cfg1, srcP, dstP, j1, srcP.DSN(), dstP.DSN())
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1Done := make(chan error, 1)
	go func() { run1Done <- m1.Run(ctx1, p1) }()
	// t_keep (unit 1) must have landed; t_reread (unit 2) must be copying.
	waitUnitState(t, j1, 1, manifest.StateSwapped, 120*time.Second)
	waitUnitState(t, j1, 2, manifest.StateCopying, 120*time.Second)
	time.Sleep(300 * time.Millisecond) // let the big copy get a little in flight
	cancel1()                          // the "crash"
	err1 := <-run1Done
	if err1 == nil {
		t.Fatalf("run 1 returned nil after cancel, want an error")
	}
	srcP.Close()
	dstP.Close()

	// --- Run 2 (relaxed, default): keep t_keep, re-read t_reread. ---
	srcP2, dstP2 := poolsFor(t, srcCl, dstCl)
	cfg2 := base
	p2 := buildPlan(t, cfg2, srcP2, dstP2)
	j2 := manifest.NewJournal(dstP2, stateDir+"/journal.jsonl")
	m2 := newTwoAgentManager(t, &cfg2, srcP2, dstP2, j2, srcP2.DSN(), dstP2.DSN())
	if err := runWithTimeout(t, m2, p2, 300*time.Second); err != nil {
		t.Fatalf("run 2 (relaxed resume): %v", err)
	}
	if got := mustCount(t, dstP2, `SELECT count(*) FROM public.t_keep`); got != nKeep {
		t.Fatalf("relaxed resume: t_keep has %d rows, want %d (duplicates or missing)", got, nKeep)
	}
	if got := mustCount(t, dstP2, `SELECT count(*) FROM public.t_reread`); got != nReRead {
		t.Fatalf("relaxed resume: t_reread has %d rows, want %d (duplicates or missing)", got, nReRead)
	}
	// t_keep was KEPT: its unit still carries run 1's epoch (1) — it was not
	// re-copied under run 2's fresh snapshot.
	if u := j2.Get(1); u == nil || u.Epoch != 1 {
		t.Fatalf("relaxed resume: t_keep unit epoch=%v, want 1 (kept, not re-copied)", u)
	}
	// t_reread was RE-READ under run 2's snapshot (epoch 2).
	if u := j2.Get(2); u == nil || u.Epoch != 2 {
		t.Fatalf("relaxed resume: t_reread unit epoch=%v, want 2 (re-read under fresh snapshot)", u)
	}
	srcP2.Close()
	dstP2.Close()

	// --- Run 3 (relaxed resume again): both tables are now completed in a
	// single epoch (t_keep@1, t_reread@2), so nothing is re-read — the
	// resume is idempotent: counts stay exact and epochs are untouched. ---
	srcP3, dstP3 := poolsFor(t, srcCl, dstCl)
	cfg3 := base
	p3 := buildPlan(t, cfg3, srcP3, dstP3)
	j3 := manifest.NewJournal(dstP3, stateDir+"/journal.jsonl")
	m3 := newTwoAgentManager(t, &cfg3, srcP3, dstP3, j3, srcP3.DSN(), dstP3.DSN())
	if err := runWithTimeout(t, m3, p3, 300*time.Second); err != nil {
		t.Fatalf("run 3 (relaxed resume): %v", err)
	}
	if got := mustCount(t, dstP3, `SELECT count(*) FROM public.t_keep`); got != nKeep {
		t.Fatalf("run 3: t_keep has %d rows, want %d (duplicates or missing)", got, nKeep)
	}
	if got := mustCount(t, dstP3, `SELECT count(*) FROM public.t_reread`); got != nReRead {
		t.Fatalf("run 3: t_reread has %d rows, want %d (duplicates or missing)", got, nReRead)
	}
	// Both tables are kept: epochs are unchanged from run 2 (1 and 2).
	if u := j3.Get(1); u == nil || u.Epoch != 1 {
		t.Fatalf("run 3: t_keep unit epoch=%v, want 1 (kept, not re-copied)", u)
	}
	if u := j3.Get(2); u == nil || u.Epoch != 2 {
		t.Fatalf("run 3: t_reread unit epoch=%v, want 2 (kept, not re-copied)", u)
	}
}

// --- helpers for the resume test ------------------------------------------

// startPGPairFull boots two clusters and returns both clusters (for
// reconnects) and one fresh pool pair (for the first run).
func startPGPairFull(t *testing.T, srcPort, dstPort uint32) (*pgtest.Cluster, *pgtest.Cluster, *pg.Pool, *pg.Pool) {
	t.Helper()
	ctx := context.Background()
	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start src: %v", err)
	}
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dst: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	t.Cleanup(func() { dstCl.Stop() })
	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 8)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 8)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	return srcCl, dstCl, src, dst
}

// poolsFor builds a fresh src+dst pool pair (the cmdRun shape: every
// `gpstream run` invocation builds new pools).
func poolsFor(t *testing.T, srcCl, dstCl *pgtest.Cluster) (*pg.Pool, *pg.Pool) {
	t.Helper()
	ctx := context.Background()
	srcP, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 8)
	if err != nil {
		t.Fatalf("src pool: %v", err)
	}
	dstP, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 8)
	if err != nil {
		srcP.Close()
		t.Fatalf("dst pool: %v", err)
	}
	t.Cleanup(func() { srcP.Close() })
	t.Cleanup(func() { dstP.Close() })
	return srcP, dstP
}

// waitUnitState polls the journal until the unit reaches the wanted state.
func waitUnitState(t *testing.T, j *manifest.Journal, unitID int64, want manifest.State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if u := j.Get(unitID); u != nil && u.State == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if u := j.Get(unitID); u == nil {
		t.Fatalf("journal has no unit %d within %s", unitID, timeout)
	} else {
		t.Fatalf("unit %d never reached %s (state=%s) within %s", unitID, want, u.State, timeout)
	}
}
