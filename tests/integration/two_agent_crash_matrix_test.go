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
	"syscall"
	"testing"
	"time"
)

// TestTwoAgentCrashRecoveryMatrix is the ADR-002 / two-agent crash-recovery
// acceptance: the unit state machine is idempotent under the death of ANY of
// the three processes mid-copy — the source agent (mid-COPY), the dest agent
// (mid-COPY), and the controller (between journal writes). Each subtest
// copies two tables through the real two-agent binaries (coordinator data
// path, the one runnable on a plain-PG pgtest pair), kills the target process
// while the second table is mid-copy, re-runs the SAME operation-id (the
// operator's recovery action), and asserts the destination ends up exactly
// right: both tables correct, no staging left, every unit terminal.
//
// The tables are ordered so the kill happens on a deterministic unit: the
// plan sorts units by (schema, name), so `alpha` (small) is unit 1 and
// `bravo` (large) is unit 2. With --workers 1, alpha is fully copied +
// swapped first; the test then waits for bravo to reach `copying` and kills
// the target. On resume, alpha (already swapped) is KEPT (T5 keep rule) and
// bravo (left non-terminal by the crash) is RE-COPIED under a fresh snapshot
// (epoch+1) — the surviving partial staging is truncated first, so the
// re-copy replaces rather than appends.
func TestTwoAgentCrashRecoveryMatrix(t *testing.T) {
	// One shared source/dest pair + the three real binaries, built once.
	srcCl, dstCl, src, dst := startPGPairFull(t, 5731, 5732)

	const alphaRows = 500
	const bravoRows = 1_200_000 // ~130 MB: a multi-second kill window mid-copy
	mustExec(t, src, `CREATE TABLE public.alpha (id int, v text)`)
	mustExec(t, src, fmt.Sprintf(
		`INSERT INTO public.alpha SELECT g, repeat('a',40)||g FROM generate_series(1,%d) g`, alphaRows))
	mustExec(t, src, `CREATE TABLE public.bravo (id int, v text)`)
	mustExec(t, src, fmt.Sprintf(
		`INSERT INTO public.bravo SELECT g, repeat('b',100)||g FROM generate_series(1,%d) g`, bravoRows))
	if n := mustCount(t, src, `SELECT count(*) FROM public.alpha`); n != alphaRows {
		t.Fatalf("alpha seeded %d, want %d", n, alphaRows)
	}
	if n := mustCount(t, src, `SELECT count(*) FROM public.bravo`); n != bravoRows {
		t.Fatalf("bravo seeded %d, want %d", n, bravoRows)
	}
	alphaDigest := digest(t, src, "public.alpha")
	bravoDigest := digest(t, src, "public.bravo")

	bin := buildTwoAgentBinaries(t)
	runArgs := func(stateDir, opID string) []string {
		return []string{
			"run",
			"--source", srcCl.URL(), "--dest", dstCl.URL(),
			"--operation-id", opID, "--state-dir", stateDir,
			"--dbs", "srcdb",
			"--source-agent-binary", bin.srcAgent,
			"--dest-agent-binary", bin.destAgent,
			"--workers", "1",
			"--resolve-conflicts", "overwrite",
		}
	}

	// killPID SIGKILLs the process.
	killPID := func(t *testing.T, pid int) {
		t.Helper()
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatalf("kill -9 %d: %v", pid, err)
		}
	}

	// pgrepPIDs returns the pids whose command line matches the pattern.
	pgrepPIDs := func(pattern string) []int {
		out, err := exec.Command("pgrep", "-f", pattern).Output()
		if err != nil {
			return nil
		}
		var pids []int
		for _, tok := range strings.Fields(string(out)) {
			p, e := strconv.Atoi(tok)
			if e == nil {
				pids = append(pids, p)
			}
		}
		return pids
	}

	// waitUnitState polls the durable journal until `table` reaches `state`
	// (or the deadline). Returns the unit's job id ("" on timeout).
	waitUnitState := func(t *testing.T, journal, table, state string, d time.Duration) string {
		t.Helper()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			if jid := scanJournalTableState(journal, table, state); jid != "" {
				return jid
			}
			time.Sleep(100 * time.Millisecond)
		}
		return ""
	}

	// assertCleanRun verifies the post-resume destination is exactly right and
	// no gpstream staging object survives.
	assertCleanRun := func(t *testing.T) {
		t.Helper()
		if got := mustCount(t, dst, `SELECT count(*) FROM public.alpha`); got != alphaRows {
			t.Fatalf("after resume alpha count = %d, want %d", got, alphaRows)
		}
		if got := mustCount(t, dst, `SELECT count(*) FROM public.bravo`); got != bravoRows {
			t.Fatalf("after resume bravo count = %d, want %d", got, bravoRows)
		}
		if got := digest(t, dst, "public.alpha"); got != alphaDigest {
			t.Fatalf("after resume alpha digest %q != source %q", got, alphaDigest)
		}
		if got := digest(t, dst, "public.bravo"); got != bravoDigest {
			t.Fatalf("after resume bravo digest %q != source %q", got, bravoDigest)
		}
		if got := mustCount(t, dst,
			`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			 WHERE n.nspname='public' AND c.relname LIKE '_gs\_%'`); got != 0 {
			t.Fatalf("staging tables still present after resume: %d", got)
		}
	}

	// The source agent is the long-lived process launched by the controller;
	// its command line is "<srcAgentPath> --src <dsn> --serve <addr> …". The
	// controller's OWN command line also contains the srcAgent path (it is
	// passed as --source-agent-binary), so match the agent by its unique
	// "--src <conn>" argument, which the controller never carries.
	srcAgentPattern := filepath.Base(bin.srcAgent) + " --src "
	destAgentPattern := filepath.Base(bin.destAgent) + " --dest "

	scenarios := []struct {
		name  string
		opID  string
		kill  func(t *testing.T, runPID int)
		check func(t *testing.T) // post-kill invariant (before resume)
	}{
		{
			name: "kill-source-agent-mid-copy",
			opID: "crashsrc",
			kill: func(t *testing.T, runPID int) {
				pids := pgrepPIDs(srcAgentPattern)
				if len(pids) == 0 {
					t.Fatalf("source agent not found (pattern %q)", srcAgentPattern)
				}
				killPID(t, pids[0])
			},
			// FIX A: when the source agent dies, the controller's control
			// connection drops and the run must exit. There is no orphan to
			// check here (we killed the agent), but the run must not hang.
			check: func(t *testing.T) {},
		},
		{
			name: "kill-dest-agent-mid-copy",
			opID: "crashdst",
			kill: func(t *testing.T, runPID int) {
				pids := pgrepPIDs(destAgentPattern)
				if len(pids) == 0 {
					t.Fatalf("dest agent not found (pattern %q)", destAgentPattern)
				}
				killPID(t, pids[0])
			},
			check: func(t *testing.T) {},
		},
		{
			name: "kill-controller-mid-run",
			opID: "crashctrl",
			kill: func(t *testing.T, runPID int) {
				killPID(t, runPID)
			},
			// The crux of FIX A: with the controller gone, the source agent's
			// control connection drops, so the agent must EXIT (rolling back
			// its pinned snapshot) rather than linger as an orphan holding a
			// server-side REPEATABLE READ backend. Give it a moment to unwind,
			// then assert it is gone.
			check: func(t *testing.T) {
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					if len(pgrepPIDs(srcAgentPattern)) == 0 {
						return // source agent exited cleanly on control loss
					}
					time.Sleep(300 * time.Millisecond)
				}
				t.Fatalf("source agent is an ORPHAN after the controller died (still running: %v); it must exit and roll back its snapshot",
					pgrepPIDs(srcAgentPattern))
			},
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			journal := filepath.Join(stateDir, sc.opID, "journal.jsonl")

			// --- Phase 1: run until bravo is mid-copy, then kill ----------
			rctx, rcancel := context.WithCancel(context.Background())
			defer rcancel()
			cmd := exec.CommandContext(rctx, bin.gpstream, runArgs(stateDir, sc.opID)...)
			var runErr strings.Builder
			cmd.Stderr = &runErr
			if err := cmd.Start(); err != nil {
				t.Fatalf("start run: %v", err)
			}
			runPID := cmd.Process.Pid

			// Wait until alpha is swapped (unit 1 done) — proves the run is
			// progressing through the two-agent path and the keep-unit is
			// complete before we crash on bravo.
			if jid := waitUnitState(t, journal, "alpha", "swapped", 120*time.Second); jid == "" {
				rcancel()
				_ = cmd.Wait()
				t.Fatalf("alpha never reached swapped (run stuck?)\nstderr:\n%s", runErr.String())
			}
			// Wait for bravo (unit 2) to reach copying — the mid-copy moment.
			if jid := waitUnitState(t, journal, "bravo", "copying", 120*time.Second); jid == "" {
				rcancel()
				_ = cmd.Wait()
				t.Fatalf("bravo never reached copying (run stuck?)\nstderr:\n%s", runErr.String())
			}
			// A short settle so the kill lands while the COPY is actually in
			// flight (the source has dialed the dest and is streaming). Loopback
			// COPYs are fast, so keep the settle short: the journal records
			// `copying` at unit start, so 200ms is mid-stream for a 100MB+ table.
			time.Sleep(200 * time.Millisecond)

			sc.kill(t, runPID)

			// The run must exit (with an error) once the target dies.
			if err := cmd.Wait(); err == nil {
				t.Fatalf("run exited cleanly after killing %s (expected an error)", sc.name)
			}
			// Best-effort: cancel the run context so any surviving child the
			// run no longer reaps is reaped (the scenario-specific orphans,
			// when that is the point, were already asserted in sc.check).
			rcancel()

			sc.check(t)

			// --- Phase 2: resume by re-running the SAME operation-id ------
			rc := exec.Command(bin.gpstream, runArgs(stateDir, sc.opID)...)
			out, err := rc.CombinedOutput()
			if err != nil {
				t.Fatalf("resume run failed: %v\n%s", err, out)
			}

			// --- Phase 3: the destination is exactly right ----------------
			assertCleanRun(t)
			t.Logf("%s: recovered — alpha+bravo correct, no staging", sc.name)
		})
	}
}

// buildTwoAgentBinaries compiles the three real two-agent binaries (controller
// + source agent + dest agent) into a temp dir and returns their paths. The
// crash matrix drives them as subprocesses, exactly as production does.
type twoAgentBins struct {
	gpstream  string
	srcAgent  string
	destAgent string
}

func buildTwoAgentBinaries(t *testing.T) twoAgentBins {
	t.Helper()
	root, err := findRepoRoot()
	if err != nil {
		t.Fatalf("find repo root: %v", err)
	}
	dir := t.TempDir()
	b := twoAgentBins{
		gpstream:  filepath.Join(dir, "gpstream"),
		srcAgent:  filepath.Join(dir, "gpstream-source-agent"),
		destAgent: filepath.Join(dir, "gpstream-dest-agent"),
	}
	for _, pair := range []struct{ target, main string }{
		{b.gpstream, "./cmd/gpstream"},
		{b.srcAgent, "./cmd/gpstream-source-agent"},
		{b.destAgent, "./cmd/gpstream-dest-agent"},
	} {
		cmd := exec.Command("go", "build", "-o", pair.target, pair.main)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build %s: %v\n%s", pair.main, err, out)
		}
	}
	return b
}

// scanJournalTableState returns the job id of the journal record for `table`
// that is in `state` (the durable mirror is appended live during the run).
// Empty when not yet present. Tolerates a truncated final line.
func scanJournalTableState(journal, table, state string) string {
	f, err := os.Open(journal)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var u struct {
			JobID string `json:"job_id"`
			Table string `json:"table"`
			State string `json:"state"`
		}
		if err := json.Unmarshal(sc.Bytes(), &u); err != nil {
			continue
		}
		if u.Table == table && u.State == state {
			return u.JobID
		}
	}
	return ""
}
