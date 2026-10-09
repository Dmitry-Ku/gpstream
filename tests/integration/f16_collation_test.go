//go:build gp6lab

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestColumnCollationLive is the TASK-SPEC v4 F-16 acceptance, live on two
// real PG 9.4.26 clusters (the GP6 kernel):
//
//  1. A source column carries an explicit collation (COLLATE "C.utf8" — the
//     only non-default collation a --no-locale C cluster admits). The copy
//     must REPRODUCE that collation in the destination DDL (the destination
//     column's pg_collation name equals the source's), so string sort/index
//     semantics survive the transfer.
//  2. The plan (dry-run) detects a COLUMN-LEVEL diff when a pre-existing
//     destination column carries a DIFFERENT explicit collation (the spec's
//     mandatory "column-level collation comparison"), and NO column diff for
//     a fresh (absent) destination table (gpstream recreates it from the
//     source DDL, which now carries the COLLATE clause).
//  3. The run's events.jsonl records the DB-level and column-level
//     collation_diff events (F-04), and the final-report input (the plan)
//     carries them.
//
// Both DBs use LC_COLLATE C, so C.utf8 is an available explicit collation on
// both sides and the comparison is purely column-level (the DB-level diff is
// reported separately from pg_database).
func TestColumnCollationLive(t *testing.T) {
	if pgtest.LiveMode() {
		// The acceptance needs a per-DB LC_COLLATE=C + explicit C.utf8
		// column collation on a --no-locale cluster. A live GP6 database
		// inherits the cluster's locale and cannot be re-initialized, so
		// the collation-diff assertions cannot hold there (the copy itself
		// is covered by the live manager tests).
		t.Skip("live mode: per-DB locale (LC_COLLATE=C) not available on a live GP6 cluster")
	}
	ctx := context.Background()

	srcCl, err := pgtest.StartWithLocale(5761, "srcdb", "C", "C")
	if err != nil {
		t.Fatalf("start src: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.StartWithLocale(5762, "srcdb", "C.utf8", "C.utf8")
	if err != nil {
		t.Fatalf("start dst: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { dst.Close() })

	// Source: t_new is a FRESH table (absent on dst) with an explicit
	// column collation; t_conflict pre-exists on dst with a DIFFERENT
	// explicit collation (the column-level diff the plan must detect).
	mustExec(t, src, `CREATE TABLE public.t_new (id int, name text COLLATE "C.utf8")`)
	mustExec(t, src, `INSERT INTO public.t_new SELECT g, 'v'||g FROM generate_series(1,100) g`)
	mustExec(t, src, `CREATE TABLE public.t_conflict (id int, name text COLLATE "C.utf8")`)
	mustExec(t, src, `INSERT INTO public.t_conflict SELECT g, 'v'||g FROM generate_series(1,50) g`)
	mustExec(t, dst, `CREATE TABLE public.t_conflict (id int, name text COLLATE "C")`)
	mustExec(t, dst, `INSERT INTO public.t_conflict SELECT g, 'old'||g FROM generate_series(1,10) g`)

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	cfg.ConflictAction = config.Overwrite // t_conflict is a deliberate conflict

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 2 {
		t.Fatalf("plan has %d units, want 2", len(p.Units))
	}
	// (2) Exactly ONE column-level diff: t_conflict.name (C.utf8 src vs C dst).
	// t_new is absent on dst -> gpstream recreates it from the source DDL ->
	// no column diff for it.
	var colDiffs []plan.CollationDiff
	for _, cd := range p.Collations {
		if cd.IsColumn() {
			colDiffs = append(colDiffs, cd)
		}
	}
	if len(colDiffs) != 1 {
		t.Fatalf("got %d column-level collation diffs, want 1: %+v", len(colDiffs), p.Collations)
	}
	if colDiffs[0].Table != "public.t_conflict" || colDiffs[0].Column != "name" ||
		colDiffs[0].SrcCollation != "C.utf8" || colDiffs[0].DstCollation != "C" {
		t.Fatalf("column diff = %+v, want public.t_conflict.name C.utf8 -> C", colDiffs[0])
	}

	// Run the copy (t_new fresh, t_conflict overwrite -> both recreated from
	// the source DDL, which now carries the COLLATE clause).
	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, j, src.DSN(), dst.DSN())
	if err := runWithTimeout(t, mgr, p, 60*time.Second); err != nil {
		t.Fatalf("manager.Run: %v", err)
	}

	// (1) The destination columns reproduce the source's explicit collation.
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_attribute a
		JOIN pg_collation co ON co.oid = a.attcollation
		WHERE a.attrelid = 'public.t_new'::regclass AND a.attname = 'name' AND co.collname = 'C.utf8'`); got != 1 {
		t.Fatalf("t_new.name did not reproduce the C.utf8 collation on the destination")
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_attribute a
		JOIN pg_collation co ON co.oid = a.attcollation
		WHERE a.attrelid = 'public.t_conflict'::regclass AND a.attname = 'name' AND co.collname = 'C.utf8'`); got != 1 {
		t.Fatalf("t_conflict.name was not recreated with the source C.utf8 collation (overwrite)")
	}

	// Data correctness (the collation DDL must not touch the data path).
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t_new`); got != 100 {
		t.Fatalf("t_new: %d rows, want 100", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.t_conflict WHERE name = 'v50'`); got != 1 {
		t.Fatalf("t_conflict overwrite: v50 missing (old rows not replaced)")
	}

	// (3) events.jsonl records both the DB-level and column-level diffs (F-04).
	evs := readEventLines(t, jdir)
	if !evsContain(evs, `"kind":"collation_diff"`) {
		t.Fatalf("events.jsonl has no collation_diff event: %v", evs)
	}
	if !evsContain(evs, `"level":"column"`) || !evsContain(evs, `"level":"db"`) {
		t.Fatalf("events.jsonl must carry both a column and a db collation_diff: %v", evs)
	}
}
