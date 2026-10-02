//go:build gp6lab

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestDryRunAcceptance is the TASK-SPEC v4 F-10 (M3) acceptance test, live on
// real PG 9.4.26 clusters:
//
//	dry-run against a destination holding deliberately conflicting tables
//	with a different collation => an exact report; two consecutive dry-runs
//	with no environment change => a byte-identical plan (except the marked
//	prediction fields).
//
// Setup: source cluster, DB "srcdb" (LC_COLLATE C); destination cluster, DB
// "srcdb" (LC_COLLATE C.utf8 — the only other locale the --no-locale C
// cluster admits). Source has tables a, b; the destination already holds b
// (a deliberate conflict). The plan must report exactly that one conflict
// with the stop action, exactly one collation diff for the copied
// DB (not the postgres service DB), and two plan builds must be
// byte-identical (deterministic hash + stable JSON). The destination must be
// left untouched.
func TestDryRunAcceptance(t *testing.T) {
	ctx := context.Background()

	srcCl, err := pgtest.Start(5701, "srcdb")
	if err != nil {
		t.Fatalf("start src: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	// Same DB name as the source (the standard migration case) but a
	// different LC_COLLATE: a genuine collation difference on the copied DB.
	dstCl, err := pgtest.StartWithLocale(5702, "srcdb", "C.utf8", "C.utf8")
	if err != nil {
		t.Fatalf("start dst: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	t.Cleanup(func() { dst.Close() })

	// Source: two tables. Destination: table b already exists (conflict),
	// table a does not.
	mustExec(t, src, `CREATE TABLE public.a (id int, val text)`)
	mustExec(t, src, `CREATE TABLE public.b (id int, val text)`)
	mustExec(t, src, `INSERT INTO public.a SELECT g, 'a'||g FROM generate_series(1,100) g`)
	mustExec(t, src, `INSERT INTO public.b SELECT g, 'b'||g FROM generate_series(1,100) g`)
	mustExec(t, dst, `CREATE TABLE public.b (id int, val text)`)
	mustExec(t, dst, `INSERT INTO public.b SELECT g, 'old'||g FROM generate_series(1,10) g`)
	dstBBefore := mustCount(t, dst, `SELECT count(*) FROM public.b`)

	cfg := config.Default() // default conflict action = stop
	p1, err := plan.Build(ctx, &cfg, src, dst)
	if err != nil {
		t.Fatalf("plan.Build: %v", err)
	}

	// 1. Exact conflict report: only b. The DEFAULT (--resolve-conflicts stop) is
	// strict per FR-10 §8: the dry-run must say the run would STOP with an
	// error (spec §3 verdict), not silently skip.
	if len(p1.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1 (only b); got %+v", len(p1.Conflicts), p1.Conflicts)
	}
	c := p1.Conflicts[0]
	if c.Schema != "public" || c.Table != "b" {
		t.Errorf("conflict = %s.%s, want public.b", c.Schema, c.Table)
	}
	if !strings.HasPrefix(c.Action, "STOP (default:") {
		t.Errorf("conflict action = %q, want the strict STOP verdict", c.Action)
	}

	// 2. Exact collation report: exactly one diff, for the copied DB srcdb,
	// C -> C.utf8. The postgres service DB (identical on both clusters) must
	// NOT be reported.
	if len(p1.Collations) != 1 {
		t.Fatalf("collation diffs = %d, want 1; got %+v", len(p1.Collations), p1.Collations)
	}
	cd := p1.Collations[0]
	if cd.DB != "srcdb" {
		t.Errorf("collation diff db = %q, want srcdb (the copied DB, not postgres)", cd.DB)
	}
	if cd.LCCollate != "C -> C.utf8" {
		t.Errorf("lc_collate = %q, want %q", cd.LCCollate, "C -> C.utf8")
	}

	// 3. Determinism: a second dry-run on the unchanged environment yields a
	// byte-identical plan (stable hash + stable JSON), CR-4/F-10 §7.
	p2, err := plan.Build(ctx, &cfg, src, dst)
	if err != nil {
		t.Fatalf("plan.Build 2: %v", err)
	}
	if p1.Hash == "" || p1.Hash != p2.Hash {
		t.Errorf("plan hash: %q vs %q (must be non-empty and identical)", p1.Hash, p2.Hash)
	}
	j1, err := json.Marshal(p1)
	if err != nil {
		t.Fatalf("marshal p1: %v", err)
	}
	j2, err := json.Marshal(p2)
	if err != nil {
		t.Fatalf("marshal p2: %v", err)
	}
	if string(j1) != string(j2) {
		t.Errorf("two dry-runs are not byte-identical:\n%v\n%v", j1, j2)
	}

	// 4. Dry-run makes NO changes to the destination (F-10: no destination
	// changes): a still absent, b still 10 rows.
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename='a'`); got != 0 {
		t.Errorf("dry-run created table a on the destination (%d), want 0", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.b`); got != dstBBefore {
		t.Errorf("destination b rows = %d, want %d (dry-run must not touch it)", got, dstBBefore)
	}
}

// TestDryRunOverwriteAction verifies the F-10 conflict report reflects the
// operator's policy: with --overwrite the same conflicting table is reported
// with the overwrite action (the report shows the impact BEFORE running).
func TestDryRunOverwriteAction(t *testing.T) {
	ctx := context.Background()
	srcCl, err := pgtest.Start(5711, "srcdb")
	if err != nil {
		t.Fatalf("start src: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(5712, "srcdb")
	if err != nil {
		t.Fatalf("start dst: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	t.Cleanup(func() { dst.Close() })

	mustExec(t, src, `CREATE TABLE public.t (id int)`)
	mustExec(t, src, `INSERT INTO public.t SELECT g FROM generate_series(1,50) g`)
	mustExec(t, dst, `CREATE TABLE public.t (id int)`)
	mustExec(t, dst, `INSERT INTO public.t SELECT g FROM generate_series(1,5) g`)

	cfg := config.Default()
	cfg.ConflictAction = config.Overwrite // --overwrite
	p, err := plan.Build(ctx, &cfg, src, dst)
	if err != nil {
		t.Fatalf("plan.Build: %v", err)
	}
	if len(p.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1; got %+v", len(p.Conflicts), p.Conflicts)
	}
	if p.Conflicts[0].Action != "overwrite" {
		t.Errorf("conflict action = %q, want overwrite", p.Conflicts[0].Action)
	}
}

// TestDryRunStrictConflictStops is the FR-10 §8 DEFAULT, live: with a
// conflicting table already on the destination and NO conflict policy, the
// run STOPS — it returns a ConflictError and leaves the destination table
// untouched (nothing copied, nothing clobbered). (The --resolve-conflicts skip path is
// covered by TestManagerSkipConflict / TestManagerOverwriteChecksumSkip.)
func TestDryRunStrictConflictStops(t *testing.T) {
	ctx := context.Background()
	srcCl, err := pgtest.Start(5721, "srcdb")
	if err != nil {
		t.Fatalf("start src: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(5722, "srcdb")
	if err != nil {
		t.Fatalf("start dst: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	t.Cleanup(func() { dst.Close() })

	// The destination already holds "keep" with DIFFERENT data than the source
	// (so a copy would clobber it) — a genuine conflict.
	mustExec(t, src, `CREATE TABLE public.keep (id int)`)
	mustExec(t, src, `INSERT INTO public.keep SELECT g FROM generate_series(1,100) g`)
	mustExec(t, dst, `CREATE TABLE public.keep (id int)`)
	mustExec(t, dst, `INSERT INTO public.keep SELECT g FROM generate_series(1,3) g`)

	cfg := config.Default() // strict: stop
	p, err := plan.Build(ctx, &cfg, src, dst)
	if err != nil {
		t.Fatalf("plan.Build: %v", err)
	}
	if len(p.Conflicts) != 1 || p.Conflicts[0].Table != "keep" {
		t.Fatalf("conflicts = %+v, want exactly [keep]", p.Conflicts)
	}

	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")
	mgr := newTwoAgentManager(t, &cfg, src, dst, j, src.DSN(), dst.DSN())
	err = runWithTimeout(t, mgr, p, 60*time.Second)
	if err == nil {
		t.Fatal("run should stop on the 'keep' conflict (strict default)")
	}
	var ce *manifest.ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %v, want *manifest.ConflictError", err)
	}
	if ce.Table != "keep" {
		t.Errorf("conflict target = %s, want keep", ce.Table)
	}
	// Nothing was clobbered: the destination still holds its original 3 rows.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.keep`); got != 3 {
		t.Errorf("destination keep rows = %d, want 3 (strict conflict must not clobber)", got)
	}
	if mgr.Units != 0 {
		t.Errorf("completed units = %d, want 0 (nothing copied)", mgr.Units)
	}
}
