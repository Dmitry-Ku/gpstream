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

	"github.com/gpstream/gpstream/internal/pgtest"
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
	const bravoRows = 5_000_000 // ~500 MB: a ~15-30 s copy, so the SSH kill
	// reliably lands mid-copy (at 130 MB the run can finish before the
	// kill's round trip arrives -> "run exited cleanly").
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
	// live mode: the controller is the DEPLOYED /usr/bin/gpstream on the
	// controller host (vm1) and the agents are driven over SSH (env); local
	// mode keeps the convention-resolved local build (agents sit next to the
	// controller binary).
	controllerBin := liveControllerBinary(t, bin.gpstream)
	runEnv := liveCLIEnv(t, srcCl.URL(), dstCl.URL())
	runArgs := func(stateDir, opID string) []string {
		return []string{
			"run",
			"--source", srcCl.URL(), "--dest", dstCl.URL(),
			"--operation-id", opID, "--state-dir", stateDir,
			"--dbs", srcCl.DB,
			// no agent-binary flags: the agents resolve by convention
			// (sibling of the controller binary / GPSTREAM_AGENT_BIN_DIR).
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

	// sshHostFor returns the host the given agent runs on (live mode: parsed
	// from the DSN; local: "").
	hostOf := func(dsn string) string {
		if !pgtest.LiveMode() {
			return ""
		}
		return dsnHost(dsn)
	}

	// pgrepPIDs returns the pids whose command line matches the pattern — on
	// the given agent host over SSH in live mode, locally otherwise.
	pgrepPIDs := func(host, pattern string) []int {
		var out []byte
		var err error
		if host != "" {
			user := os.Getenv("GP6_LAB_SSH_USER")
			if user == "" {
				user = "gp"
			}
			args := []string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no"}
			if key := os.Getenv("GP6_LAB_SSH_KEY"); key != "" {
				args = append(args, "-i", key)
			}
			// Bracket the first char (the classic [c] trick): pgrep -f runs
			// inside the remote `bash -c` wrapper, whose own command line
			// contains the pattern text, so pgrep would always report the
			// wrapper's pid too — and the orphan-exit check
			// (len(pgrepPIDs)==0) could never observe 0 pids, even when the
			// agent had exited cleanly (guaranteed false ORPHAN). [c] still
			// matches the agent's command line, but the wrapper's contains
			// the literal "[c]" and no longer matches.
			// Quote for the REMOTE shell: without quotes the pattern's
			// spaces word-split, so pgrep would see "--src" as an option
			// (error -> no pids). The pattern is harness-controlled (no
			// single quotes), so single quoting is safe.
			bracketed := pattern
			if r := rune(bracketed[0]); (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
				bracketed = "[" + string(r) + "]" + bracketed[1:]
			}
			quoted := "'" + strings.ReplaceAll(bracketed, "'", "'\\''") + "'"
			// sudo -n: the agents run as gpadmin (launched via `sudo -u
			// gpadmin`), so a plain pgrep as gp cannot see them.
			args = append(args, user+"@"+host, "sudo -n pgrep -f "+quoted)
			out, err = exec.Command("ssh", args...).Output()
		} else {
			out, err = exec.Command("pgrep", "-f", pattern).Output()
		}
		if err != nil {
			return nil
		}
		var pids []int
		for _, tok := range strings.Fields(string(out)) {
			if p, e := strconv.Atoi(tok); e == nil {
				pids = append(pids, p)
			}
		}
		return pids
	}

	// psRemote returns `ps -o pid,etime,stat,args` lines for processes matching
	// pattern on host (SSH in live mode, local otherwise) - orphan diagnosis:
	// a leftover from an earlier scenario has a long etime; a genuinely stuck
	// agent of this scenario has a short one.
	psRemote := func(t *testing.T, host, pattern string) string {
		t.Helper()
		if host == "" {
			out, _ := exec.Command("ps", "-eo", "pid,etime,stat,args").Output()
			var b strings.Builder
			for _, line := range strings.Split(string(out), "\n") {
				if strings.Contains(line, pattern) && !strings.Contains(line, "ps -eo") {
					b.WriteString(line + "\n")
				}
			}
			return b.String()
		}
		user := os.Getenv("GP6_LAB_SSH_USER")
		if user == "" {
			user = "gp"
		}
		args := []string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no"}
		if key := os.Getenv("GP6_LAB_SSH_KEY"); key != "" {
			args = append(args, "-i", key)
		}
		q := "'" + strings.ReplaceAll(pattern, "'", "'\\''") + "'"
		args = append(args, user+"@"+host, "sudo -n ps -eo pid,etime,stat,args | grep -F "+q+" | grep -v grep")
		out, _ := exec.Command("ssh", args...).CombinedOutput()
		return string(out)
	}

	// killPIDRemote SIGKILLs a pid on host over SSH (live mode). The agents
	// run as gpadmin (the SSH user is gp), so the kill must go through
	// passwordless sudo — a plain kill -9 as gp would EPERM. The kill
	// is best-effort: by the time the SSH round trip lands, the agent
	// may already have exited on its own (the run finished before the
	// kill arrived) — that is the intended "run ended" outcome, not a
	// failure.
	killPIDRemote := func(t *testing.T, host string, pid int) {
		t.Helper()
		user := os.Getenv("GP6_LAB_SSH_USER")
		if user == "" {
			user = "gp"
		}
		args := []string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no"}
		if key := os.Getenv("GP6_LAB_SSH_KEY"); key != "" {
			args = append(args, "-i", key)
		}
		args = append(args, user+"@"+host, "sudo -n kill -9 "+strconv.Itoa(pid))
		if out, err := exec.Command("ssh", args...).CombinedOutput(); err != nil {
			if strings.Contains(string(out), "No such process") {
						return // already gone before the kill arrived
			}
			t.Fatalf("remote kill -9 %d on %s: %v: %s", pid, host, err, out)
		}
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
	srcHost := hostOf(srcCl.URL())
	dstHost := hostOf(dstCl.URL())

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
				pids := pgrepPIDs(srcHost, srcAgentPattern)
				if len(pids) == 0 {
					t.Fatalf("source agent not found (pattern %q, host %q)", srcAgentPattern, srcHost)
				}
				if srcHost != "" {
					killPIDRemote(t, srcHost, pids[0])
				} else {
					killPID(t, pids[0])
				}
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
				pids := pgrepPIDs(dstHost, destAgentPattern)
				if len(pids) == 0 {
					t.Fatalf("dest agent not found (pattern %q, host %q)", destAgentPattern, dstHost)
				}
				if dstHost != "" {
					killPIDRemote(t, dstHost, pids[0])
				} else {
					killPID(t, pids[0])
				}
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
			// server-side REPEATABLE READ backend.
			//
			// Timing: the control plane and the data plane are decoupled.
			// Killing the controller closes the CONTROL connection, but the
			// in-flight COPY rides a separate data connection to the (still
			// alive) destination agent, so the source agent finishes that unit
			// before its control loop notices the drop and exits. On live GP6
			// a ~130 MB in-flight COPY can take well over 30 s, so the window
			// must cover "the in-flight unit finishing" + unwind. 180 s is
			// generous; if the agent is still here after that it is a real
			// hang, not a timing artifact.
			check: func(t *testing.T) {
				deadline := time.Now().Add(180 * time.Second)
				for time.Now().Before(deadline) {
					if len(pgrepPIDs(srcHost, srcAgentPattern)) == 0 {
						return // source agent exited cleanly on control loss
					}
					time.Sleep(500 * time.Millisecond)
				}
				if info := psRemote(t, srcHost, srcAgentPattern); info != "" {
				t.Logf("orphan ps on %s:\n%s", srcHost, info)
				}
				t.Fatalf("source agent is an ORPHAN after the controller died (still running: %v); it must exit and roll back its snapshot",
					pgrepPIDs(srcHost, srcAgentPattern))
			},
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			journal := filepath.Join(stateDir, sc.opID, "journal.jsonl")

			// Clean slate: no leftover agents from a previous scenario (an
			// orphaned dest agent holds the fixed control/data ports and
			// would refuse the fresh run's agent launch).
			liveKillAgents(t, srcCl.URL(), dstCl.URL())

			// --- Phase 1: run until bravo is mid-copy, then kill ----------
			rctx, rcancel := context.WithCancel(context.Background())
			defer rcancel()
			cmd := exec.CommandContext(rctx, controllerBin, runArgs(stateDir, sc.opID)...)
			if runEnv != nil {
				cmd.Env = append(os.Environ(), runEnv...)
			}
			var runErr strings.Builder
			cmd.Stderr = &runErr
			if err := cmd.Start(); err != nil {
				t.Fatalf("start run: %v", err)
			}
			runPID := cmd.Process.Pid

			// Wait until alpha is swapped (unit 1 done) — proves the run is
			// progressing through the two-agent path and the keep-unit is
			// complete before we crash on bravo. (Live: a GP6 coordinator
			// connect is 10-20s, so the window is generous.)
			if jid := waitUnitState(t, journal, "alpha", "swapped", 300*time.Second); jid == "" {
				rcancel()
				_ = cmd.Wait()
				t.Fatalf("alpha never reached swapped (run stuck?)\nstderr:\n%s", runErr.String())
			}
			// Wait for bravo (unit 2) to reach copying — the mid-copy moment.
			if jid := waitUnitState(t, journal, "bravo", "copying", 300*time.Second); jid == "" {
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
			// (the orphaned agents should have exited on control loss; a
			// best-effort pkill clears any laggard holding the fixed ports).
			liveKillAgents(t, srcCl.URL(), dstCl.URL())
			time.Sleep(2 * time.Second)
			rc := exec.Command(controllerBin, runArgs(stateDir, sc.opID)...)
			if runEnv != nil {
				rc.Env = append(os.Environ(), runEnv...)
			}
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
	// Live mode: the binaries are the DEPLOYED package on the controller
	// host (/usr/bin) — no repo / go build needed. The paths are used for
	// the pgrep patterns (base names) and, in live mode, the controller
	// itself is exec'd via liveControllerBinary.
	if pgtest.LiveMode() {
		return twoAgentBins{
			gpstream:  "/usr/bin/gpstream",
			srcAgent:  "/usr/bin/gpstream-source-agent",
			destAgent: "/usr/bin/gpstream-dest-agent",
		}
	}
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
