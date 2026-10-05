//go:build gp6lab

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestF02Kill9CleanupRestoresReceiver — spec F-02 acceptance (kill -9 part):
// "kill -9 mid-operation => cleanup leaves the destination in its
// pre-operation state (or 'fully copied' for completed tables)".
//
// Phase 1: `gpstream run` (real binary, subprocess) copies one table at a
// crawling rate-limit; the test waits until the durable journal shows the
// unit in `copying` and the staging table exists on the destination —
// mid-copy, guaranteed — then kill -9 the process. The killed COPY leaves
// its partial rows in the staging table (PG does not roll back a killed
// COPY); the final table was never swapped in.
// Phase 2: `gpstream cleanup --mode drop --dest ...` must remove exactly the
// gpstream-owned objects (the surviving staging; the never-created final is
// a no-op) and restore the receiver to its pre-operation state — a
// pre-existing operator table must survive untouched.
// Phase 3: a plain re-run of the SAME operation-id completes the killed
// unit (resume semantics) with the exact source count.
func TestF02Kill9CleanupRestoresReceiver(t *testing.T) {
	srcCl, dstCl, src, dst := startPGPairFull(t, 5681, 5682)
	ctx := context.Background()

	// Operator data on the destination that MUST survive the cleanup.
	mustExec(t, dst, `CREATE TABLE public.keep_me (id int)`)
	mustExec(t, dst, `INSERT INTO public.keep_me VALUES (42)`)

	// Source table: ~130 MB. The unit's lifecycle (copying -> verifying ->
	// swapped) gives a multi-second kill window before the atomic swap.
	const rows = 1_200_000
	mustExec(t, src, `CREATE TABLE public.f02kill (id int, v text)`)
	if _, err := src.Exec(ctx,
		fmt.Sprintf(`INSERT INTO public.f02kill SELECT g, repeat('x',100)||g FROM generate_series(1,%d) g`, rows)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	bin := buildTwoAgentBinaries(t)
	// live mode: the controller is the DEPLOYED /usr/bin/gpstream on the
	// controller host and the agents are driven over SSH (env); local mode
	// keeps the convention-resolved local build.
	controllerBin := liveControllerBinary(t, bin.gpstream)
	runEnv := liveCLIEnv(t, srcCl.URL(), dstCl.URL())
	stateDir := t.TempDir()
	opID := "f02kill9"

	// --- Phase 1: run + kill -9 mid-operation -------------------------
	// No rate limit: the 130 MB table streams in seconds, and the kill
	// window is the `verifying` phase (two full-table checksum re-reads).
	cmd := exec.Command(controllerBin, "run",
		"--source", srcCl.URL(), "--dest", dstCl.URL(),
		"--operation-id", opID, "--state-dir", stateDir,
		// agent binaries resolve by convention / GPSTREAM_AGENT_BIN_DIR
		"--workers", "1", "--verify", "on")
	if runEnv != nil {
		cmd.Env = append(os.Environ(), runEnv...)
	}
	var runErr strings.Builder
	cmd.Stderr = &runErr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start run: %v", err)
	}
	killed := false
	defer func() {
		if !killed && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}()

	journal := filepath.Join(stateDir, opID, "journal.jsonl")
	// Wait for the unit to reach `verifying` — the COPY has committed (the
	// staging table now holds ALL rows) but the atomic swap to the final
	// name has NOT happened yet. Killing here is the exact spec "mid-
	// operation" moment: data is on the destination in scratch form, the
	// final table is still absent. (Killing during `copying` is weaker: the
	// COPY is one uncommitted transaction, so staging shows 0 rows until it
	// commits.)
	var jobID string
	var unitID int64
	deadline := time.Now().Add(240 * time.Second)
	for time.Now().Before(deadline) {
		jobID, unitID = readStateUnit(journal, "verifying")
		if jobID != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if jobID == "" {
		_ = cmd.Process.Kill()
		t.Fatalf("unit never reached verifying (journal stuck?)\nstderr:\n%s", runErr.String())
	}
	staging := dest.StagingName(jobID, unitID)
	// Kill -9 the copying process (the exact spec scenario).
	if err := exec.Command("kill", "-9", strconv.Itoa(cmd.Process.Pid)).Run(); err != nil {
		t.Fatalf("kill -9: %v", err)
	}
	killed = true
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed process must not exit cleanly")
	}

	// Post-kill state: the committed COPY left ALL rows in staging (a
	// killed process does not roll back an already-committed COPY), and the
	// final table is still absent (no swap happened).
	stRows := mustCount(t, dst, `SELECT count(*) FROM `+staging)
	if stRows != rows {
		t.Fatalf("staging rows = %d, want %d (committed COPY, pre-swap kill)", stRows, rows)
	}
	if stagingExists(ctx, dst, "public.f02kill") {
		t.Fatal("final table must not exist after a pre-swap kill (no swap)")
	}

	// --- Phase 2: cleanup --mode drop restores the pre-operation state --
	cl := exec.Command(controllerBin, "cleanup",
		"--state-dir", stateDir, "--operation-id", opID,
		"--mode", "drop", "--dest", dstCl.URL(), "--yes")
	if runEnv != nil {
		cl.Env = append(os.Environ(), runEnv...)
	}
	out, err := cl.CombinedOutput()
	if err != nil {
		t.Fatalf("cleanup --mode drop: %v\n%s", err, out)
	}
	if stagingExists(ctx, dst, staging) {
		t.Fatalf("staging %s must be dropped by cleanup", staging)
	}
	if stagingExists(ctx, dst, "public.f02kill") {
		t.Fatal("final table must remain absent")
	}
	// Operator data survives untouched.
	keep := mustCount(t, dst, `SELECT count(*) FROM public.keep_me WHERE id = 42`)
	if keep != 1 {
		t.Fatalf("operator table keep_me damaged by cleanup (row 42 count=%d)", keep)
	}
	// The operation state dir is gone.
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatalf("journal should be removed after cleanup: %v", err)
	}

	// --- Phase 3: re-run the same operation completes the killed unit ---
	// (the orphaned remote agents should have exited on control loss; a
	// best-effort pkill clears any laggard holding the fixed ports).
	liveKillAgents(t, srcCl.URL(), dstCl.URL())
	time.Sleep(2 * time.Second)
	rr := exec.Command(controllerBin, "run",
		"--source", srcCl.URL(), "--dest", dstCl.URL(),
		"--operation-id", opID, "--state-dir", stateDir,
		// agent binaries resolve by convention / GPSTREAM_AGENT_BIN_DIR
	)
	if runEnv != nil {
		rr.Env = append(os.Environ(), runEnv...)
	}
	rrOut, err := rr.CombinedOutput()
	if err != nil {
		t.Fatalf("re-run: %v\n%s", err, rrOut)
	}
	got := mustCount(t, dst, `SELECT count(*) FROM public.f02kill`)
	if got != rows {
		t.Fatalf("re-run count = %d, want %d", got, rows)
	}
}

// TestF02TwoDBConcurrentSnapshot — spec F-02 acceptance ("two-database
// concurrent-write tests"). The CLI is one database per operation (a
// connstring), so two databases = two CONCURRENT operations, each with its
// own REPEATABLE READ snapshot. A writer hammers BOTH databases while both
// copies run; every destination table must equal exactly its own snapshot
// point — no writer rows, and no cross-operation interference.
func TestF02TwoDBConcurrentSnapshot(t *testing.T) {
	ctx := context.Background()
	// Two independent source/destination database pairs.
	src1Cl, dst1Cl, src1, dst1 := startPGPairFull(t, 5691, 5692)
	src2Cl, dst2Cl, src2, dst2 := startPGPairFull(t, 5693, 5694)
	defer func() {
		src1.Close()
		dst1.Close()
		src2.Close()
		dst2.Close()
	}()

	const n = 20_000 // per table
	for _, src := range []*pg.Pool{src1, src2} {
		mustExec(t, src, `CREATE TABLE public.t_a (id int, v text)`)
		mustExec(t, src, `CREATE TABLE public.t_b (id int, v text)`)
		if _, err := src.Exec(ctx,
			fmt.Sprintf(`INSERT INTO public.t_a SELECT g, 'a'||g FROM generate_series(1,%d) g`, n)); err != nil {
			t.Fatalf("seed t_a: %v", err)
		}
		if _, err := src.Exec(ctx,
			fmt.Sprintf(`INSERT INTO public.t_b SELECT g, 'b'||g FROM generate_series(1,%d) g`, n)); err != nil {
			t.Fatalf("seed t_b: %v", err)
		}
	}

	type leg struct {
		srcCl, dstCl *pgtest.Cluster
		src, dst     *pg.Pool
		journal      string
	}
	legs := []leg{
		{src1Cl, dst1Cl, src1, dst1, filepath.Join(t.TempDir(), "j1.jsonl")},
		{src2Cl, dst2Cl, src2, dst2, filepath.Join(t.TempDir(), "j2.jsonl")},
	}
	type result struct {
		i   int
		err error
	}
	results := make(chan result, len(legs))
	for i, l := range legs {
		cfg := config.Default()
		cfg.RateLimit = 0.1 // stretch the wire phase for the writers
		cfg.WorkersPerPair = 1
		p := buildPlan(t, cfg, l.src, l.dst)
		if len(p.Units) != 2 {
			t.Fatalf("leg %d: plan has %d units, want 2", i, len(p.Units))
		}
		j := manifest.NewJournal(l.dst, l.journal)
		mgr := newTwoAgentManager(t, &cfg, l.src, l.dst, j, l.src.DSN(), l.dst.DSN())
		go func(i int, mgr *manifest.Manager, p *plan.Plan) {
			results <- result{i, mgr.Run(ctx, p)}
		}(i, mgr, p)
	}

	// Wait until BOTH operations' snapshots are live (any unit left
	// pending in the durable journal), then start one writer per database.
	deadline := time.Now().Add(liveDeadline(60 * time.Second))
	for {
		live := 0
		for _, l := range legs {
			if anyUnitLeftPending(l.journal) {
				live++
			}
		}
		if live == len(legs) || time.Now().After(deadline) {
			if live < len(legs) {
				t.Fatal("both operations did not reach their snapshot within 60s")
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	var stop atomic.Bool
	writersDone := make(chan struct{}, 2)
	var committed atomic.Int64
	for _, l := range legs {
		go func(l leg) {
			defer func() { writersDone <- struct{}{} }()
			wc, err := pgx.Connect(ctx, l.srcCl.URL())
			if err != nil {
				return
			}
			defer wc.Close(ctx)
			for !stop.Load() {
				for _, tbl := range []string{"t_a", "t_b"} {
					if _, err := wc.Exec(ctx,
						`INSERT INTO public.`+tbl+` SELECT 1000000+g, 'w'||g FROM generate_series(1,500) g`); err == nil {
						committed.Add(500)
					}
				}
				time.Sleep(15 * time.Millisecond)
			}
		}(l)
	}

	errs := make([]error, len(legs))
	for range legs {
		select {
		case r := <-results:
			errs[r.i] = r.err
		case <-time.After(240 * time.Second):
			stop.Store(true)
			t.Fatal("a leg did not return within 240s (data-path deadlock?)")
		}
	}
	stop.Store(true)
	for range 2 {
		<-writersDone
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("leg %d run: %v", i, e)
		}
	}
	if committed.Load() == 0 {
		t.Fatal("writers committed nothing — test is vacuous")
	}

	// Both databases: exactly the snapshot point (no writer rows) in both
	// tables of each destination.
	for i, l := range legs {
		for _, tbl := range []string{"t_a", "t_b"} {
			got := mustCount(t, l.dst, `SELECT count(*) FROM public.`+tbl)
			if got != n {
				t.Fatalf("leg %d %s: %d rows, want exactly the snapshot point %d (writer rows must be excluded)",
					i, tbl, got, n)
			}
		}
		// And the source actually grew (non-vacuous).
		srcN := mustCount(t, l.src, `SELECT count(*) FROM public.t_a`)
		if srcN <= n {
			t.Fatalf("leg %d source t_a = %d, writer did not write", i, srcN)
		}
	}
}

// --- helpers ---------------------------------------------------------------

// stagingExists reports whether the relation (qualified name) exists.
func stagingExists(ctx context.Context, q *pg.Pool, rel string) bool {
	v, _ := pg.Row(ctx, q, `SELECT to_regclass($1)`, rel)
	return v != nil
}

// readStateUnit returns (jobID, unitID) of the first journal record in the
// given state (the durable mirror is appended live during the run).
func readStateUnit(journal, state string) (string, int64) {
	var jobID string
	var unitID int64
	if ok := scanJournal(journal, func(u journalUnit) bool {
		if u.State == state {
			jobID, unitID = u.JobID, u.UnitID
			return true
		}
		return false
	}); !ok {
		return "", 0
	}
	return jobID, unitID
}

// anyUnitLeftPending reports whether the journal still holds a unit in a
// non-pending state (i.e. the run has passed the snapshot-pinning point).
func anyUnitLeftPending(journal string) bool {
	found := false
	scanJournal(journal, func(u journalUnit) bool {
		if u.State != "pending" {
			found = true
			return true
		}
		return false
	})
	return found
}

type journalUnit struct {
	JobID  string `json:"job_id"`
	UnitID int64  `json:"unit_id"`
	State  string `json:"state"`
}

// scanJournal streams the file mirror, calling fn for each record until fn
// returns true or the file is exhausted. A missing/truncated file yields
// false (the run is still starting).
func scanJournal(journal string, fn func(journalUnit) bool) bool {
	f, err := os.Open(journal)
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var u journalUnit
		if err := json.Unmarshal(sc.Bytes(), &u); err != nil {
			continue // tolerate a truncated final line
		}
		if fn(u) {
			return true
		}
	}
	return false
}

// findRepoRoot walks up from the test working directory to the directory
// holding go.mod (the integration package runs from its own dir).
func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	wd, _ := os.Getwd()
	return "", fmt.Errorf("go.mod not found above %s", wd)
}
