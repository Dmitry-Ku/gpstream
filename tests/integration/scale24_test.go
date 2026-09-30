//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/jackc/pgx/v5"
)

// M2 acceptance (spec §11): "F-02/F-06/F-07 on 20+ tables". These three tests
// run the live manager over 24 tables on real PG 9.4.26 clusters — one scale
// scenario per mandatory feature:
//
//   - F-02 (consistent snapshot): a writer hammers ALL 24 source tables while
//     the copy runs; the destination must hold exactly the snapshot point in
//     every table.
//   - F-07 (crash + resume): the run is cancelled mid-copy of the 24th (big)
//     table after 23 small ones are swapped; the relaxed resume keeps the
//     swapped tables (epoch 1) and re-reads the in-flight one (epoch 2), with
//     exact final counts for all 24.
//   - F-06 (unit failure isolation): one source table disappears after the
//     plan is built; that unit fails, the run surfaces the error, but the
//     other 23 tables complete exactly.
//
// Table names t00..t23 sort before t_big, so the big table is unit 24 in the
// deterministic (schema, name) plan order.

const nScaleTables = 24 // >= 20 per spec

// scaleTableName renders the i-th of the small 24 tables: t00 .. t23.
func scaleTableName(i int) string {
	return fmt.Sprintf("t%02d", i)
}

// seedScaleTables creates and fills the 24 small tables on the source.
func seedScaleTables(t *testing.T, src *pg.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < nScaleTables; i++ {
		name := scaleTableName(i)
		mustExec(t, src, `CREATE TABLE public.`+name+` (id int, val text)`)
		if _, err := src.Exec(ctx,
			`INSERT INTO public.`+name+` SELECT g, 'v' || g FROM generate_series(1,`+itoa(n)+`) g`); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
}

// scaleCount checks every one of the 24 destination tables holds exactly n rows.
func scaleCount(t *testing.T, dst *pg.Pool, n int64) {
	t.Helper()
	for i := 0; i < nScaleTables; i++ {
		name := scaleTableName(i)
		if got := mustCount(t, dst, `SELECT count(*) FROM public.`+name); got != n {
			t.Fatalf("destination %s has %d rows, want %d", name, got, n)
		}
	}
}

// scaleDigestsEqual compares the pre-copy source digests against the
// destination digests for all 24 tables (content, not just counts).
func scaleDigestsEqual(t *testing.T, src, dst *pg.Pool, want map[string]string) {
	t.Helper()
	for i := 0; i < nScaleTables; i++ {
		name := scaleTableName(i)
		if got := mustDigest(t, dst, "public."+name); got != want[name] {
			t.Fatalf("destination %s digest %q != source pre-copy digest %q (not one consistent point)", name, got, want[name])
		}
	}
}

// TestScale24TablesConsistentSnapshot — F-02 at 24-table scale. A writer loop
// inserts into all 24 source tables (round-robin) while the manager copies
// under a REPEATABLE READ snapshot; every destination table must contain
// exactly the pre-copy rows, and the source must show the writer DID write
// (non-vacuous).
func TestScale24TablesConsistentSnapshot(t *testing.T) {
	src, dst, srcCl := startPGPair(t, 5621, 5622)
	defer func() { src.Close(); dst.Close() }()

	const n = 5_000 // per table: 24 x 5000 = 120k rows
	seedScaleTables(t, src, n)

	// Digest of every source table before the run (no writer yet): the
	// snapshot point, since the writer starts only after the snapshot is pinned.
	want := make(map[string]string, nScaleTables)
	for i := 0; i < nScaleTables; i++ {
		name := scaleTableName(i)
		want[name] = mustDigest(t, src, "public."+name)
	}

	cfg := config.Default()
	// 0.1 Gbps = 12.5 MB/s stretches the wire phase; per-unit overhead
	// (staging/verify/swap x24) dominates, giving the writer several seconds.
	cfg.RateLimit = 0.1
	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != nScaleTables {
		t.Fatalf("plan has %d units, want %d", len(p.Units), nScaleTables)
	}
	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")
	mgr := manifest.NewManager(&cfg, src, dst, j)

	runDone := make(chan error, 1)
	go func() { runDone <- mgr.Run(context.Background(), p) }()

	// The run's REPEATABLE READ snapshot is pinned BEFORE any unit is processed,
	// so wait until ANY unit leaves pending — that proves the snapshot is live.
	// (Waiting for a specific unit to be "copying" can be missed for a small
	// table whose copy outlasts the poll interval, flaking the writer start.)
	snapshotLive := false
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for _, u := range j.Units() {
			if u.State != manifest.StatePending {
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
		t.Fatalf("no unit left pending within 60s; cannot guarantee writer starts post-snapshot")
	}

	// Start the writer NOW (strictly after the snapshot point): round-robin
	// over all 24 tables, ids >= 100000 (seeds are 1..n).
	var stop atomic.Bool
	var writerCommitted int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		wc, err := pgx.Connect(context.Background(), srcCl.URL())
		if err != nil {
			return
		}
		defer wc.Close(context.Background())
		for i := 0; !stop.Load(); i++ {
			name := scaleTableName(i % nScaleTables)
			if _, err := wc.Exec(context.Background(),
				`INSERT INTO public.`+name+` SELECT 100000 + g, 'w' || g FROM generate_series(1,200) g`); err == nil {
				atomic.AddInt64(&writerCommitted, 200)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	select {
	case err := <-runDone:
		if err != nil {
			stop.Store(true)
			<-writerDone
			t.Fatalf("manager.Run: %v", err)
		}
	case <-time.After(300 * time.Second):
		t.Fatalf("manager.Run did not return within 300s (data-path deadlock?)")
	}
	stop.Store(true)
	<-writerDone

	if writerCommitted == 0 {
		t.Fatalf("writer committed nothing — test is vacuous")
	}
	// Every destination table: exactly the snapshot point, no writer rows.
	scaleCount(t, dst, n)
	for i := 0; i < nScaleTables; i++ {
		name := scaleTableName(i)
		if got := mustCount(t, dst, `SELECT count(*) FROM public.`+name+` WHERE id >= 100000`); got != 0 {
			t.Fatalf("%s has %d writer rows (concurrent source writes leaked into the snapshot)", name, got)
		}
	}
	scaleDigestsEqual(t, src, dst, want)
}

// TestScale24TablesCrashResume — F-07 at 24-table scale. 23 small tables +
// one big (unit 24, last in plan order). Run 1 is cancelled mid-copy of the
// big table; run 2 (relaxed) keeps the 23 swapped tables (epoch 1) and
// re-reads the big one (epoch 2). Final counts must be exact for all 24.
func TestScale24TablesCrashResume(t *testing.T) {
	srcCl, dstCl, src, dst := startPGPairFull(t, 5631, 5632)
	ctx := context.Background()
	defer func() { src.Close(); dst.Close() }()

	const (
		nSmall = 2_000     // per small table
		nBig   = 3_000_000 // big table (~110 MB: ~1 s copy, mid-copy cancel lands safely)
	)
	seedScaleTables(t, src, nSmall)
	mustExec(t, src, `CREATE TABLE public.t_big (id int, val text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.t_big SELECT g, repeat('b', 30) || g FROM generate_series(1,`+itoa(nBig)+`) g`); err != nil {
		t.Fatalf("seed t_big: %v", err)
	}

	base := config.Default()
	base.WorkersPerPair = 1 // serial: by the time unit 25 (t_big) copies, 1..24 are swapped
	stateDir := t.TempDir()

	// --- Run 1: cancelled mid-copy of t_big (unit 25). ---
	srcP, dstP := poolsFor(t, srcCl, dstCl)
	cfg1 := base
	p1 := buildPlan(t, cfg1, srcP, dstP)
	if len(p1.Units) != nScaleTables+1 { // 24 small + t_big
		t.Fatalf("plan has %d units, want %d", len(p1.Units), nScaleTables+1)
	}
	j1 := manifest.NewJournal(dstP, stateDir+"/journal.jsonl")
	m1 := manifest.NewManager(&cfg1, srcP, dstP, j1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1Done := make(chan error, 1)
	go func() { run1Done <- m1.Run(ctx1, p1) }()
	// Unit 25 (t_big, last in plan order) copying implies units 1..24 are
	// already swapped (1 worker).
	waitUnitState(t, j1, 25, manifest.StateCopying, 300*time.Second)
	time.Sleep(300 * time.Millisecond) // let the big copy get in flight
	cancel1()                          // the "crash"
	err1 := <-run1Done
	if err1 == nil {
		t.Fatalf("run 1 returned nil after cancel, want an error")
	}
	srcP.Close()
	dstP.Close()

	// --- Run 2 (relaxed, default): keep 1..23, re-read t_big. ---
	srcP2, dstP2 := poolsFor(t, srcCl, dstCl)
	cfg2 := base
	p2 := buildPlan(t, cfg2, srcP2, dstP2)
	j2 := manifest.NewJournal(dstP2, stateDir+"/journal.jsonl")
	m2 := manifest.NewManager(&cfg2, srcP2, dstP2, j2)
	if err := runWithTimeout(t, m2, p2, 600*time.Second); err != nil {
		t.Fatalf("run 2 (relaxed resume): %v", err)
	}
	scaleCount(t, dstP2, nSmall)
	if got := mustCount(t, dstP2, `SELECT count(*) FROM public.t_big`); got != nBig {
		t.Fatalf("t_big has %d rows, want %d (duplicates or missing after resume)", got, nBig)
	}
	// Units 1..24 were KEPT (run 1's epoch), unit 25 (t_big) RE-READ under epoch 2.
	for id := int64(1); id <= nScaleTables; id++ {
		if u := j2.Get(id); u == nil || u.Epoch != 1 {
			t.Fatalf("unit %d epoch=%v, want 1 (kept, not re-copied)", id, u)
		}
	}
	if u := j2.Get(25); u == nil || u.Epoch != 2 {
		t.Fatalf("unit 25 (t_big) epoch=%v, want 2 (re-read under fresh snapshot)", u)
	}
	srcP2.Close()
	dstP2.Close()
}

// TestScale24TablesUnitFailureContinue — F-06 at 24-table scale. One source
// table is dropped AFTER the plan is built, so its unit fails at read time
// (relation does not exist). The run must surface the failure but complete
// the other 23 tables exactly, and record the unit as failed for a resume.
func TestScale24TablesUnitFailureContinue(t *testing.T) {
	src, dst, _ := startPGPair(t, 5641, 5642)
	defer func() { src.Close(); dst.Close() }()

	const n = 2_000
	seedScaleTables(t, src, n)

	cfg := config.Default()
	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != nScaleTables {
		t.Fatalf("plan has %d units, want %d", len(p.Units), nScaleTables)
	}

	// Break t05 AFTER the plan exists: the manager reads its schema at run
	// time and fails that unit, while the other 23 proceed.
	const broken = 5
	mustExec(t, src, `DROP TABLE public.`+scaleTableName(broken))

	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")
	mgr := manifest.NewManager(&cfg, src, dst, j)
	runErr := runWithTimeout(t, mgr, p, 300*time.Second)
	if runErr == nil {
		t.Fatalf("run returned nil, want the failed unit's error (F-06: failure is surfaced)")
	}
	t.Logf("runErr: %v", runErr)

	// Exactly one failed unit (t05), the other 23 completed.
	failed, completed := 0, 0
	states := make(map[string]manifest.State, len(j.Units()))
	for _, u := range j.Units() {
		states[u.Table] = u.State
		switch u.State {
		case manifest.StateFailed:
			failed++
			if u.Table != scaleTableName(broken) {
				t.Fatalf("unexpected failed unit: %s (want t%02d)", u.Table, broken)
			}
		case manifest.StateSwapped, manifest.StateCompleted:
			completed++
		}
	}
	t.Logf("journal states: %v", states)
	if failed != 1 || completed != nScaleTables-1 {
		t.Fatalf("journal: %d failed / %d completed, want 1 failed / %d completed", failed, completed, nScaleTables-1)
	}
	// The broken table must NOT have landed on the destination.
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_class WHERE relname = `+quote(scaleTableName(broken))); got != 0 {
		t.Fatalf("broken table %s exists on destination", scaleTableName(broken))
	}
	// The other 23 are exact.
	for i := 0; i < nScaleTables; i++ {
		if i == broken {
			continue
		}
		name := scaleTableName(i)
		if got := mustCount(t, dst, `SELECT count(*) FROM public.`+name); got != n {
			t.Fatalf("%s has %d rows, want %d (failed unit corrupted a sibling?)", name, got, n)
		}
	}
}

// quote renders a string literal for the pg_class check above.
func quote(s string) string { return "'" + s + "'" }
