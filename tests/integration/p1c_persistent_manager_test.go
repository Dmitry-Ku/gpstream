//go:build gp6lab

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// optDigest returns the table's content digest or "" when the table does not
// exist (a failed unit) — the digest() helper t.Fatals on a missing table.
func optDigest(t *testing.T, q pg.StreamingQuerier, table string) string {
	t.Helper()
	dw := transfer.NewContentDigest()
	if _, err := q.ReadCopy(context.Background(), dw, `COPY (SELECT * FROM `+table+`) TO STDOUT (FORMAT binary)`); err != nil {
		return ""
	}
	d, err := dw.Sum()
	if err != nil {
		return ""
	}
	return d
}

// optCount returns the row count of a count(*) query, tolerating a
// non-existent table (returns 0): a FAILED unit legitimately has no
// destination table (the swap never ran), so mustCount (which t.Fatals)
// cannot be used to probe it.
func optCount(t *testing.T, c pg.Querier, sql string) int64 {
	t.Helper()
	rows, err := c.Query(context.Background(), sql)
	if err != nil {
		return 0 // the table does not exist (a failed unit)
	}
	if len(rows) == 0 {
		return 0
	}
	if n, ok := rows[0][0].(int64); ok {
		return n
	}
	return -1
}

// waitJournalState polls the durable journal mirror (appended live, one
// record per state change) and returns the LAST recorded state of `table`,
// or "" if it never reached `state` within d.
func waitJournalState(journalPath, table, state string, d time.Duration) string {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f, err := os.Open(journalPath); err == nil {
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 1024*1024), 1024*1024)
			var last string
			for sc.Scan() {
				var u struct {
					UnitID int64  `json:"unit_id"`
					Table  string `json:"table"`
					State  string `json:"state"`
				}
				if json.Unmarshal(sc.Bytes(), &u) == nil && u.Table == table {
					last = u.State
				}
			}
			f.Close()
			if last == state {
				return last
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ""
}

// TestPersistentManagerRun is the P1c manager-level acceptance: the Manager
// itself (plan -> journal -> runUnit -> verify -> swap) drives 12 small
// tables through the PERSISTENT coordinator data channel (one source->dest
// connection for the whole run; every unit = UnitBegin + frames + Trailer on
// the same socket) with --verify on (inline digests on both sides, compared
// by the manager). It proves:
//
//  1. a mid-run DEST AGENT DEATH is a unit-level failure (not a run abort):
//     the killed unit (and the units after it) fail, the run completes, and
//     the killed unit's partial staging survives for the resume;
//  2. a RESUME (a second Manager.Run over the same journal, fresh agents)
//     completes the failed units over a FRESH persistent channel (the
//     surviving partial staging is truncated + re-COPYd, FR-07) and keeps
//     the already-swapped units (the T5 keep rule);
//  3. the persistent channel is actually in use (the dest accepted a
//     persistent connection; the legacy path opens one per unit).
func TestPersistentManagerRun(t *testing.T) {
	const (
		srcPort = 5654
		dstPort = 5655
		tables  = 12
		rows    = 2500
		// killTargetRows: the kill-target table (p1c_t05, the 5th unit by
		// name order) is much larger so its COPY takes a few seconds — the
		// kill goroutine (50ms poll + close) then lands MID-COPY, exercising
		// the persistent channel breaking mid-unit (the source's in-flight
		// write fails) and the resume truncating the surviving partial
		// staging (FR-07). The other tables stay small (fast units).
		killTargetRows = 600_000
	)
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
	t.Cleanup(func() { src.Close() })
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { dst.Close() })

	// Seed the tables (compressible + small: fast units, a reliable kill
	// window, a quick run).
	var wantDigests []string
	var wantRows []int64
	var names []string
	for i := 1; i <= tables; i++ {
		name := fmt.Sprintf("p1c_t%02d", i)
		names = append(names, name)
		nrows := rows
		if i == 5 {
			nrows = killTargetRows // the kill target (unit 5)
		}
		mustExec(t, src, `CREATE TABLE `+name+` (id integer, customer text, note text, amount numeric)`)
		mustExec(t, src,
			`INSERT INTO `+name+` SELECT g, 'cust_' || (g % 97), 'repeatable note body for compression padding', (g % 100) * 10.5 FROM generate_series(1,`+strconv.Itoa(nrows)+`) g`)
		if n := mustCount(t, src, `SELECT count(*) FROM `+name); n != int64(nrows) {
			t.Fatalf("%s seeded %d, want %d", name, n, nrows)
		}
		wantDigests = append(wantDigests, digest(t, src, "public."+name))
		wantRows = append(wantRows, int64(nrows))
	}

	cfg := config.Default()
	cfg.WorkersPerPair = 1 // deterministic unit order (the kill targets a unit)
	cfg.ConflictAction = config.Overwrite
	cfg.Verify = config.VerifyOn // inline digests on both sides (P9-E)
	cfg.Compression = config.CompZstd
	cfg.Source = srcCl.URL()
	cfg.Dest = dstCl.URL()

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != tables {
		t.Fatalf("plan has %d units, want %d", len(p.Units), tables)
	}

	jdir := t.TempDir()
	journalPath := filepath.Join(jdir, "journal.jsonl")
	mgr := manifest.NewManager(&cfg, manifest.NewJournal(dst, journalPath))
	runner := manifest.NewInProcTwoAgentRunner(&cfg)
	mgr.SetTwoAgentRunner(runner)

	// --- Run 1: 12 units over the persistent channel; when a unit reaches
	// "copying" (mid-COPY) the dest agent is KILLED (listeners + pg conn
	// closed hard, no graceful release — the SIGKILL analog). The in-flight
	// unit fails; the remaining units fail fast (no dest); the run completes
	// (default on-unit-failure=continue).
	var acceptsAtKill atomic.Int64
	go func() {
		if waitJournalState(journalPath, "p1c_t05", "copying", 120*time.Second) == "" {
			return
		}
		acceptsAtKill.Store(runner.PersistAccepts())
		runner.KillDestAgent()
	}()

	start := time.Now()
	runErr := runWithTimeout(t, mgr, p, 240*time.Second)
	t.Logf("run 1 (dest killed mid-run): %v in %v", runErr, time.Since(start))
	if runErr == nil {
		t.Fatalf("run 1: expected a unit failure (the kill), got a clean run")
	}
	// The persistent channel WAS in use before the kill: the dest accepted a
	// persistent connection (>= 1). The legacy per-unit path would have
	// opened one connection per unit, not a single persistent one.
	if n := acceptsAtKill.Load(); n < 1 {
		t.Fatalf("dest accepted %d persistent connections before the kill, want >= 1 (the persistent channel is not in use)", n)
	}

	// The kill's blast radius is bounded: the killed unit + (at most) the
	// units after it fail; the earlier units are swapped + verified.
	var wrong []string
	for i, name := range names {
		n := optCount(t, dst, `SELECT count(*) FROM public.`+name)
		if n == wantRows[i] && optDigest(t, dst, "public."+name) == wantDigests[i] {
			continue
		}
		wrong = append(wrong, name)
	}
	if len(wrong) == 0 {
		t.Fatalf("no table affected by the kill (the kill did not land?)")
	}
	if len(wrong) > tables-3 {
		t.Fatalf("%d tables wrong after the kill, want a bounded blast radius: %v", len(wrong), wrong)
	}
	t.Logf("run 1: %d table(s) affected by the kill: %v", len(wrong), wrong)

	// The swapped units left no staging; the failed units may leave their
	// partial staging (the resume truncates it).
	if n := mustCount(t, dst,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname LIKE '_gs\\_%'`); n > int64(len(wrong)) {
		t.Fatalf("%d staging tables left after run 1, want <= %d (the failed units)", n, len(wrong))
	}

	// --- Run 2 (RESUME): a fresh Manager + fresh agents over the SAME
	// journal. The swapped units are kept (T5 keep rule); the failed units
	// are re-copied over a FRESH persistent channel (the surviving partial
	// staging is truncated + re-COPYd, FR-07).
	mgr2 := manifest.NewManager(&cfg, manifest.NewJournal(dst, journalPath))
	runner2 := manifest.NewInProcTwoAgentRunner(&cfg)
	mgr2.SetTwoAgentRunner(runner2)
	if err := runWithTimeout(t, mgr2, p, 240*time.Second); err != nil {
		t.Fatalf("run 2 (resume): %v", err)
	}
	if n := runner2.PersistAccepts(); n < 1 {
		t.Fatalf("resume: dest accepted %d persistent connections, want >= 1", n)
	}

	// ALL tables are now correct (the resume completed the failed ones).
	for i, name := range names {
		if n := mustCount(t, dst, `SELECT count(*) FROM public.`+name); n != wantRows[i] {
			t.Fatalf("after resume: %s = %d rows, want %d", name, n, wantRows[i])
		}
		if got := digest(t, dst, "public."+name); got != wantDigests[i] {
			t.Fatalf("after resume: %s digest %q != source %q", name, got, wantDigests[i])
		}
	}
	// No staging left (the resume's swap + truncate cleaned everything).
	if n := mustCount(t, dst,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname LIKE '_gs\\_%'`); n != 0 {
		t.Fatalf("%d staging tables left after the resume, want 0", n)
	}
	if st := waitJournalState(journalPath, "p1c_t05", "swapped", 5*time.Second); st != "swapped" {
		t.Fatalf("journal: p1c_t05 state = %q, want swapped", st)
	}
	t.Logf("persistent manager run: kill mid-run -> resume ok; %d tables correct, persistent accepts run1=%d run2=%d",
		tables, acceptsAtKill.Load(), runner2.PersistAccepts())
}

// TestPersistentChannelMultiTableNoKill is the clean (no-kill) variant: 12
// tables over the persistent channel with --verify on, asserting the dest
// accepted EXACTLY ONE persistent connection for the whole run (the legacy
// per-unit behavior would open 12 connections).
func TestPersistentChannelMultiTableNoKill(t *testing.T) {
	const (
		srcPort = 5656
		dstPort = 5657
		tables  = 12
		rows    = 2500
	)
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
	t.Cleanup(func() { src.Close() })
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { dst.Close() })

	var wantDigests []string
	var names []string
	for i := 1; i <= tables; i++ {
		name := fmt.Sprintf("p1c_nk%02d", i)
		names = append(names, name)
		mustExec(t, src, `CREATE TABLE `+name+` (id integer, customer text, note text, amount numeric)`)
		mustExec(t, src,
			`INSERT INTO `+name+` SELECT g, 'cust_' || (g % 97), 'repeatable note body for compression padding', (g % 100) * 10.5 FROM generate_series(1,`+strconv.Itoa(rows)+`) g`)
		wantDigests = append(wantDigests, digest(t, src, "public."+name))
	}

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	cfg.ConflictAction = config.Overwrite
	cfg.Verify = config.VerifyOn
	cfg.Compression = config.CompZstd
	cfg.Source = srcCl.URL()
	cfg.Dest = dstCl.URL()

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != tables {
		t.Fatalf("plan has %d units, want %d", len(p.Units), tables)
	}

	jdir := t.TempDir()
	mgr := manifest.NewManager(&cfg, manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl")))
	runner := manifest.NewInProcTwoAgentRunner(&cfg)
	mgr.SetTwoAgentRunner(runner)

	if err := runWithTimeout(t, mgr, p, 180*time.Second); err != nil {
		t.Fatalf("manager.Run (persistent): %v", err)
	}

	// THE assertion: ONE persistent connection carried ALL 12 units (the
	// legacy per-unit path would open 12 connections).
	if n := runner.PersistAccepts(); n != 1 {
		t.Fatalf("dest accepted %d persistent connections for %d units, want exactly 1 (one connection for the run)", n, tables)
	}

	for i, name := range names {
		if n := mustCount(t, dst, `SELECT count(*) FROM public.`+name); n != rows {
			t.Fatalf("%s = %d rows, want %d", name, n, rows)
		}
		if got := digest(t, dst, "public."+name); got != wantDigests[i] {
			t.Fatalf("%s digest %q != source %q", name, got, wantDigests[i])
		}
	}
	if n := mustCount(t, dst,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname LIKE '_gs\\_%'`); n != 0 {
		t.Fatalf("%d staging tables left after the run, want 0", n)
	}
	t.Logf("persistent channel: %d tables over 1 connection, verify on, all digests match", tables)
}
