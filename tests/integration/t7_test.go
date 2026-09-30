//go:build integration

// T7 (DESIGN §6 / TASK-SPEC v4 FR-02 mandatory test): live integration
// scenarios on real PostgreSQL 9.4.26 clusters (the GP6 kernel is 9.4.26;
// the GP6-specific items — iptables drop on a segment, frame-ACK fault
// injection — need the lab self-hosted runner and live there, not here).
//
// This file covers the parts of T7 that can be exercised on the local
// toolchain:
//
//   - FR-02 "обязательный тест": a writer loop hammering the SOURCE during
//     the copy; the result must equal one consistent point in time — the
//     concurrent writes must NOT reach the destination.
//   - retry-сценарий (DESIGN §6): a unit whose destination rejects the data
//     stream mid-copy must (a) not deadlock the run (the source must
//     unblock when the destination stops reading the pipe), (b) be journaled
//     as failed, and (c) be re-copied to success by a resume re-run.
//
// Both tests drive the Manager over pg.Pools, exactly like cmdRun (a single
// pgx.Conn is not safe for the data path, and pgx closes a conn after a
// failed COPY — a pool is the production shape).
package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/jackc/pgx/v5"
)

// startPGPair boots two real PG 9.4.26 clusters and returns pools for both
// (mirroring cmdRun) plus the source cluster (for its conn URL, used by the
// raw-writer helper).
func startPGPair(t *testing.T, srcPort, dstPort uint32) (*pg.Pool, *pg.Pool, *pgtest.Cluster) {
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
	t.Cleanup(func() { src.Close() })
	t.Cleanup(func() { dst.Close() })
	return src, dst, srcCl
}

// buildPlan builds the copy plan against the live clusters.
func buildPlan(t *testing.T, cfg config.Config, src, dst *pg.Pool) *plan.Plan {
	t.Helper()
	p, err := plan.Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("plan.Build: %v", err)
	}
	if len(p.Units) == 0 {
		t.Fatalf("plan has no units")
	}
	return p
}

// runWithTimeout runs mgr.Run in a goroutine and fails the test if it does not
// return within timeout (a data-path deadlock would otherwise hang forever).
func runWithTimeout(t *testing.T, mgr *manifest.Manager, p *plan.Plan, timeout time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- mgr.Run(context.Background(), p) }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatalf("mgr.Run did not return within %s (data-path deadlock?)", timeout)
	}
	return nil
}

// --- FR-02 mandatory consistency test ------------------------------------

// TestConsistentSnapshotWriterLoop is the TASK-SPEC v4 FR-02 "обязательный
// тест": конкурентные изменения на источнике во время копирования не должны
// попасть в результат. A writer loop inserts into the source continuously
// while the manager copies under a REPEATABLE READ snapshot; the destination
// must contain exactly the pre-copy rows (one consistent point in time), and
// the source must show the writer DID write (so the test is not vacuous).
//
// The writer is started only after the run's snapshot is established (unit 1
// reaches state "copying", which happens after the first source query that
// pins the REPEATABLE READ snapshot), so every writer commit is strictly
// after the snapshot point and provably not in the copied data.
func TestConsistentSnapshotWriterLoop(t *testing.T) {
	src, dst, srcCl := startPGPair(t, 5521, 5522)
	ctx := context.Background()

	const n = 1_000_000
	mustExec(t, src, `CREATE TABLE public.t (id int, val text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.t SELECT g, 'v' || g FROM generate_series(1,`+itoa(n)+`) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Digest of the source before the run (no concurrent writer yet): this is
	// the digest of the snapshot point, since the writer starts only after
	// the snapshot is pinned.
	want := mustDigest(t, src, "public.t")

	cfg := config.Default()
	// 0.1 Gbps = 12.5 MB/s: stretches the ~24 MB copy to ~2 s so the writer
	// loop is guaranteed active during the snapshot read.
	cfg.RateLimit = 0.1
	p := buildPlan(t, cfg, src, dst)
	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")
	mgr := manifest.NewManager(&cfg, src, dst, j)

	runDone := make(chan error, 1)
	go func() { runDone <- mgr.Run(context.Background(), p) }()

	// Wait until unit 1 is copying: the run's first source query (schema
	// read) has pinned the REPEATABLE READ snapshot by now.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if u := j.Get(1); u != nil && u.State == manifest.StateCopying {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if u := j.Get(1); u == nil || u.State != manifest.StateCopying {
		t.Fatalf("unit 1 never reached copying (state=%v); cannot guarantee writer starts post-snapshot", u)
	}

	// Start the writer loop NOW (strictly after the snapshot point). It runs
	// for the whole copy and is stopped only after the run completes, so the
	// destination assertions (below) see the writer's commits are NOT in the
	// copied data.
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
		for !stop.Load() {
			_, err := wc.Exec(context.Background(),
				`INSERT INTO public.t SELECT `+itoa(2*n)+` + g, 'w' || g FROM generate_series(1,500) g`)
			if err == nil {
				atomic.AddInt64(&writerCommitted, 500)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case err := <-runDone:
		if err != nil {
			stop.Store(true)
			<-writerDone
			t.Fatalf("manager.Run: %v", err)
		}
	case <-time.After(120 * time.Second):
		t.Fatalf("manager.Run did not return within 120s (data-path deadlock?)")
	}
	// Stop the writer only once the run is done.
	stop.Store(true)
	<-writerDone

	// The destination must hold EXACTLY the snapshot point: n rows, no writer
	// rows (ids > n), and the same content digest as the pre-copy source.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t`); got != n {
		t.Fatalf("destination has %d rows, want %d (writer rows leaked into the copy?)", got, n)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t WHERE id > `+itoa(n)); got != 0 {
		t.Fatalf("destination has %d writer rows (concurrent source writes leaked into the snapshot)", got)
	}
	if got := mustDigest(t, dst, "public.t"); got != want {
		t.Fatalf("destination digest %q != source pre-copy digest %q (result is not one consistent point)", got, want)
	}
	// The test must not be vacuous: the writer committed a substantial amount
	// while the copy ran, and the source now has more than n rows (the
	// snapshot simply did not see them).
	if c := atomic.LoadInt64(&writerCommitted); c < 2000 {
		t.Fatalf("writer committed only %d rows — too few to prove concurrency", c)
	}
	if got := mustCount(t, src, `SELECT count(*) FROM public.t`); got <= n {
		t.Fatalf("source still has %d rows — the writer loop never ran during the copy", got)
	}
}

// --- retry / crash-recovery scenario (DESIGN §6) --------------------------

// TestUnitFailureNoDeadlockThenResume: a unit whose destination rejects the
// data stream mid-copy must (a) not deadlock the run (the source must
// unblock when the destination stops reading the pipe — FramedPipe.Abort),
// (b) be journaled as failed, and (c) be re-copied to success by a resume
// re-run once the destination is corrected. The poisoned destination (wrong
// column type) makes the first COPY FROM row fail, which is the mid-copy
// dest-death the data path must survive. The source table is large enough
// (>256 KB, one frame) that the source outpaces the dead reader and fills
// the pipe — without the abort path this deadlocks (the "hang" shape from
// the E2 on_segment incident, on the coordinator path).
//
// The resume re-run uses a FRESH source pool: pgx closes a conn after a
// failed COPY (a sink write error during CopyTo), and the production binary
// (cmdRun) builds new pools on every invocation, so a resume re-run never
// reuses a dead conn. The destination pool is safe to keep (the pool
// recycles dead conns).
func TestUnitFailureNoDeadlockThenResume(t *testing.T) {
	src, dst, srcCl := startPGPair(t, 5531, 5532)
	ctx := context.Background()

	const n = 20_000 // ~1.2 MB: several 256 KB frames -> the pipe fills
	mustExec(t, src, `CREATE TABLE public.e (id int, v text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.e SELECT g, repeat('x', 50) || g FROM generate_series(1,`+itoa(n)+`) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Poison the destination: same name, but the 'v' column is INTEGER, so
	// the binary stream's text field is undecodable -> COPY FROM fails on
	// the first row, mid-copy.
	mustExec(t, dst, `CREATE TABLE public.e (id int, v integer)`)

	cfg := config.Default()
	// In-place copy into the final (poisoned) table: the COPY FROM target is
	// the wrong-typed table, so the stream is rejected on row 1.
	cfg.ConflictAction = config.Truncate
	p := buildPlan(t, cfg, src, dst)
	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")

	// Run 1: the unit must FAIL (dest rejects the stream) and must NOT hang.
	mgr := manifest.NewManager(&cfg, src, dst, j)
	runErr := runWithTimeout(t, mgr, p, 30*time.Second)
	if runErr == nil {
		t.Fatalf("run 1 succeeded, want the poisoned dest to fail the copy")
	}
	u := j.Get(1)
	if u == nil {
		t.Fatalf("journal has no unit 1")
	}
	if u.State != manifest.StateFailed {
		t.Fatalf("unit 1 state=%s, want failed", u.State)
	}

	// Operator fixes the destination (drop the poisoned table); a resume
	// re-run must re-copy the failed unit to success (no duplicates).
	mustExec(t, dst, `DROP TABLE public.e`)
	// Fresh source pool (cmdRun shape): run 1's COPY failure closed its
	// pinned source conn, and a resume never reuses a dead conn.
	src2, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 8)
	if err != nil {
		t.Fatalf("reconnect src: %v", err)
	}
	t.Cleanup(func() { src2.Close() })
	mgr2 := manifest.NewManager(&cfg, src2, dst, j)
	if err := runWithTimeout(t, mgr2, p, 30*time.Second); err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.e`); got != n {
		t.Fatalf("destination e has %d rows, want %d", got, n)
	}
	u2 := j.Get(1)
	if u2 == nil || u2.State != manifest.StateSwapped {
		t.Fatalf("unit 1 after resume state=%v, want swapped", u2)
	}
}

// --- helpers --------------------------------------------------------------

// mustDigest returns the order-independent content digest of a table (the
// same transfer.ContentDigest the data path verifies with).
func mustDigest(t *testing.T, c pg.StreamingQuerier, table string) string {
	t.Helper()
	buf := &appendBuf{}
	if _, err := c.ReadCopy(context.Background(), buf, `COPY `+table+` TO STDOUT (FORMAT binary)`); err != nil {
		t.Fatalf("readcopy %s: %v", table, err)
	}
	d, err := transfer.ContentDigest(buf.data)
	if err != nil {
		t.Fatalf("digest %s: %v", table, err)
	}
	return d
}

// appendBuf is a plain io.Writer that accumulates bytes.
type appendBuf struct{ data []byte }

func (b *appendBuf) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func itoa(n int) string { return fmt.Sprint(n) }
