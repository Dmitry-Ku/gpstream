package main

// F-09 coverage (item 11), second pass: the remaining cheap cmd/gpstream
// branches — cmdSelftest (OK path), cmdMan (man page found), cmdCleanup
// --mode drop with an unreachable --dest (connect-fail path + the TLS/dest
// flag parsing), cmdStatus with a valid runstats.json (live table: pair,
// progress, throughput, by_status, per-table) plus jobmeta.json (snap-epoch
// line), the parseConfig flag branches (--no-redistribute, --skew-threshold,
// --segment-balance-threshold, --index-workers, --index-parallel,
// --no-indexes, --no-constraints, --index-priority), and the config-file
// security-warning loop (0644 file with a password in the DSN).
import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/monitor"
)

// TestCmdSelftest runs `gpstream selftest` end to end. The selftest is
// environment-independent (it exercises the in-process fake plane), so the
// OK path must print "selftest: OK" and return nil.
func TestCmdSelftest(t *testing.T) {
	if err := run([]string{"selftest"}); err != nil {
		t.Fatalf("selftest: %v", err)
	}
}

// TestCmdManFound runs `gpstream man` from the repo root, where
// ./man/gpstream.8 exists — the found branch prints the page.
func TestCmdManFound(t *testing.T) {
	if err := run([]string{"man"}); err != nil {
		t.Fatalf("man: %v", err)
	}
}

// TestCleanupModeDropConnectFail runs `cleanup --mode drop --dest <unreachable>
// --yes` and verifies the connect-fail path: the flags are parsed (dest +
// TLS flags), the pool cannot reach the destination, and the error is
// surfaced as "connect dest".
func TestCleanupModeDropConnectFail(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-x", []*manifest.Unit{unit(1, manifest.StatePending, false)})
	err := run([]string{
		"cleanup",
		"--operation-id", "op-x",
		"--state-dir", dir,
		"--mode", "drop",
		"--dest", "host=127.0.0.1 port=1 user=gpstream password=secret dbname=gpstream sslmode=disable",
		"--ssl-cert", "/nonexistent/cert.pem",
		"--ssl-key", "/nonexistent/key.pem",
		"--ssl-ca", "/nonexistent/ca.pem",
		"--ssl-min-tls", "1.3",
		"--yes",
	})
	if err == nil {
		t.Fatal("cleanup --mode drop with unreachable dest: want error")
	}
	if !strings.Contains(err.Error(), "connect dest") {
		t.Errorf("want 'connect dest' in error, got: %v", err)
	}
	// The state dir must still exist (nothing was removed before the
	// connect failed).
	if _, err := os.Stat(filepath.Join(dir, "op-x", "journal.jsonl")); err != nil {
		t.Errorf("journal must survive a failed drop connect: %v", err)
	}
}

// TestStatusLiveTable runs `status` with a journal that has units AND a
// valid runstats.json (pair, progress, throughput, by_status, per-table)
// plus a jobmeta.json — exercising the snap-epoch line and the whole F-04
// live table.
func TestStatusLiveTable(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-live2", []*manifest.Unit{
		unit(1, manifest.StateCopying, false),
		unit(2, manifest.StateSwapped, true),
	})
	opdir := filepath.Join(dir, "op-live2")
	// jobmeta.json: the snap-epoch metadata (T5).
	meta := manifest.JobMeta{JobID: "op-live2", SnapEpoch: 3, SnapID: "snap-3", CopyMode: "direct", ResumeCons: "strict"}
	if b, err := json.Marshal(meta); err == nil {
		if err := os.WriteFile(filepath.Join(opdir, "jobmeta.json"), b, 0o644); err != nil {
			t.Fatalf("write jobmeta: %v", err)
		}
	}
	// runstats.json: the F-04 live snapshot.
	rs := monitor.RunStats{
		UpdatedAt:   "2026-09-30T00:00:00Z",
		StartedAt:   "2026-09-30T00:00:00Z",
		ElapsedSec:  90,
		Pair:        "s1→d1",
		Bytes:       1 << 30,
		Decoded:     2 << 30,
		Rows:        100000,
		Throughput:  50_000_000,
		ETASeconds:  3600 + 2400,
		ActiveFlows: 2,
		ByStatus:    map[string]int{"copying": 1, "swapped": 1},
		PerTable: []monitor.TableRow{
			{Table: "public.t1", Rows: 90000, Wire: 1 << 30, Logical: 2 << 30, State: "copying"},
			{Table: "public.t2", Rows: 10000, Wire: 5 << 20, Logical: 9 << 20, State: "swapped"},
		},
	}
	b, err := json.Marshal(rs)
	if err != nil {
		t.Fatalf("marshal runstats: %v", err)
	}
	if err := os.WriteFile(filepath.Join(opdir, "runstats.json"), b, 0o644); err != nil {
		t.Fatalf("write runstats: %v", err)
	}
	if err := run([]string{"status", "--state-dir", dir, "--operation-id", "op-live2"}); err != nil {
		t.Fatalf("status: %v", err)
	}
}

// TestParseConfigFlagsB covers the remaining parseConfig flag branches:
// redistribution, index and constraint flags.
func TestParseConfigFlagsB(t *testing.T) {
	c, err := parseConfig([]string{
		"--no-redistribute",
		"--skew-threshold", "0.2",
		"--segment-balance-threshold", "0.3",
		"--index-workers", "7",
		"--index-parallel", "9",
		"--no-indexes",
		"--no-constraints",
		"--index-priority", "background",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if !c.NoRedistribute {
		t.Error("--no-redistribute: NoRedistribute = false")
	}
	// --skew-threshold is parsed AFTER --no-redistribute (which sets -1), so
	// the later flag wins: 0.2.
	if c.SkewThreshold != 0.2 {
		t.Errorf("SkewThreshold = %v, want 0.2 (later flag wins)", c.SkewThreshold)
	}
	if c.SegmentBalanceThreshold != 0.3 {
		t.Errorf("SegmentBalanceThreshold = %v, want 0.3", c.SegmentBalanceThreshold)
	}
	if c.IndexWorkers != 9 { // --index-parallel (9) is parsed after --index-workers (7)
		t.Errorf("IndexWorkers = %d, want 9 (later flag wins)", c.IndexWorkers)
	}
	if !c.NoIndexes || !c.NoConstraints {
		t.Error("NoIndexes/NoConstraints not set")
	}
	if c.IndexPriority != "background" {
		t.Errorf("IndexPriority = %q, want background", c.IndexPriority)
	}
}

// TestParseConfigSecurityWarning writes a 0644 config file whose source DSN
// carries a password, then parses it — the security-warning loop (F-13)
// must emit to stderr without failing the parse.
func TestParseConfigSecurityWarning(t *testing.T) {
	dir := t.TempDir()
	pf := filepath.Join(dir, "cfg.toml")
	content := "source = \"host=h user=u password=pw dbname=d sslmode=disable\"\n"
	if err := os.WriteFile(pf, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	// 0644: group/other readable → the mode warning fires.
	if err := os.Chmod(pf, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	c, err := parseConfig([]string{"--config", pf})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if !c.SourceFromFile {
		t.Error("SourceFromFile = false for a file-sourced DSN")
	}
	if len(c.SecurityWarnings()) == 0 {
		t.Error("expected at least one security warning (0644 + password DSN)")
	}
}
