//go:build gp6lab

package integration

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestResumeDestJournalTwoAgent (2E-2, architecture 08.10) drives the REAL
// `gpstream run` CLI twice end to end. Run 1 copies two tables (state dir 1).
// Run 2 re-runs the SAME operation-id with a FRESH state dir (the controller's
// state is lost — its file mirror is empty), so the "already copied" decision
// can ONLY come from the destination: the source agent requests the
// already-copied list directly from the destination journal (gpstream_units,
// the writer of record) over the source->dest channel (no controller). Both
// tables are kept (not re-copied, no duplicates) — the destination is exactly
// as after run 1.
func TestResumeDestJournalTwoAgent(t *testing.T) {
	const (
		srcPort = 5795
		dstPort = 5796
	)
	bin := buildTwoAgentBinaries(t)

	ctx := context.Background()
	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(src.Close)
	mustExec(t, src, `CREATE TABLE public.e2a (id int, v text)`)
	mustExec(t, src, `INSERT INTO public.e2a SELECT g, repeat('a', 50) || g FROM generate_series(1, 500) g`)
	mustExec(t, src, `CREATE TABLE public.e2b (id int, v text)`)
	mustExec(t, src, `INSERT INTO public.e2b SELECT g, repeat('b', 50) || g FROM generate_series(1, 200) g`)

	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(dst.Close)

	controllerBin := liveControllerBinary(t, bin.gpstream)
	runArgs := liveCLIArgs(t, srcCl.URL(), dstCl.URL())
	opID := "s2e2-resume"

	runOnce := func(stateDir string) string {
		cmd := exec.Command(controllerBin, "run",
			"--source", srcCl.URL(), "--dest", dstCl.URL(),
			"--operation-id", opID, "--state-dir", stateDir,
			"--workers", "1", "--verify", "on")
		cmd.Args = append(cmd.Args, runArgs...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("run (state-dir %s) failed: %v\n%s", stateDir, err, out)
		}
		return string(out)
	}

	// --- Run 1: the full copy (state dir 1) -------------------------------
	out1 := runOnce(t.TempDir())
	if !strings.Contains(out1, "2 unit(s) completed") {
		t.Fatalf("run 1 must copy both tables:\n%s", out1)
	}
	if !strings.Contains(out1, "0 failed") {
		t.Fatalf("run 1 must not fail:\n%s", out1)
	}
	if n := dstCount(t, dst, `SELECT count(*) FROM public.e2a`); n != 500 {
		t.Fatalf("after run 1: e2a rows = %d, want 500", n)
	}
	if n := dstCount(t, dst, `SELECT count(*) FROM gpstream_units WHERE state = 'swapped'`); n != 2 {
		t.Fatalf("after run 1: gpstream_units swapped rows = %d, want 2 (the destination is the writer of record)", n)
	}

	// --- Run 2: same op-id, FRESH state dir (the controller's state is
	// lost) — the already-copied decision must come from the destination ---
	out2 := runOnce(t.TempDir())
	if !strings.Contains(out2, "2 skipped") {
		t.Fatalf("run 2 must keep (skip) both already-copied tables (destination journal, 2E-2):\n%s", out2)
	}
	if !strings.Contains(out2, "0 failed") {
		t.Fatalf("run 2 must not fail:\n%s", out2)
	}
	// No duplicates: the destination is exactly as after run 1.
	if n := dstCount(t, dst, `SELECT count(*) FROM public.e2a`); n != 500 {
		t.Fatalf("after run 2: e2a rows = %d, want 500 (no duplicates)", n)
	}
	if n := dstCount(t, dst, `SELECT count(*) FROM public.e2b`); n != 200 {
		t.Fatalf("after run 2: e2b rows = %d, want 200 (no duplicates)", n)
	}
	if n := dstCount(t, dst, `SELECT count(*) FROM gpstream_units WHERE state = 'swapped'`); n != 2 {
		t.Fatalf("after run 2: gpstream_units swapped rows = %d, want 2 (no re-copy)", n)
	}
	if n := dstCount(t, dst, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname LIKE '_gs\_%'`); n != 0 {
		t.Fatalf("after run 2: staging tables remain = %d, want 0", n)
	}
}

// dstCount runs one SELECT and returns its single int cell (test helper).
func dstCount(t *testing.T, q *pg.Pool, sql string) int64 {
	t.Helper()
	rows, err := q.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("dst query %q: %v", sql, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("dst query %q: expected 1 cell, got %v", sql, rows)
	}
	switch v := rows[0][0].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	default:
		t.Fatalf("dst query %q: cell type %T", sql, rows[0][0])
		return 0
	}
}
