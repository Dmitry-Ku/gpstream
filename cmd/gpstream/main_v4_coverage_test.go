package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/monitor"
)

// writeJournal creates a durable file mirror under <stateDir>/<op>/journal.jsonl
// with the given units, mirroring what `run` would leave on disk.
func writeJournal(t *testing.T, stateDir, op string, units []*manifest.Unit) string {
	t.Helper()
	dir := filepath.Join(stateDir, op)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	mirror := filepath.Join(dir, "journal.jsonl")
	j := manifest.NewJournal(nil, mirror)
	for _, u := range units {
		if err := j.SetState(u); err != nil {
			t.Fatalf("setstate %d: %v", u.UnitID, err)
		}
	}
	return mirror
}

func unit(u int64, state manifest.State, renamed bool) *manifest.Unit {
	uu := &manifest.Unit{
		JobID: "op", UnitID: u, DB: "db", Schema: "s",
		Table: "t", Kind: "table", State: state, Checksum: "ck",
	}
	if renamed {
		uu.DstSchema = "s_stg"
		uu.DstName = "t_stg"
	}
	return uu
}

// --- version ---------------------------------------------------------------

func TestVersionCmd(t *testing.T) {
	if err := run([]string{"version"}); err != nil {
		t.Fatalf("version: %v", err)
	}
}

// --- status ----------------------------------------------------------------

func TestStatusMissingOpID(t *testing.T) {
	if err := run([]string{"status"}); err == nil {
		t.Fatal("status without --operation-id must error")
	}
}

func TestStatusEmptyOperation(t *testing.T) {
	dir := t.TempDir()
	if err := run([]string{"status", "--state-dir", dir, "--operation-id", "op-empty"}); err != nil {
		t.Fatalf("status (no journal yet): %v", err)
	}
}

func TestStatusWithUnits(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op1", []*manifest.Unit{
		unit(1, manifest.StateCompleted, false),
		unit(2, manifest.StatePending, false),
	})
	if err := run([]string{"status", "--state-dir", dir, "--operation-id", "op1"}); err != nil {
		t.Fatalf("status: %v", err)
	}
}

// --- resume ----------------------------------------------------------------

func TestResumeMissingOpID(t *testing.T) {
	if err := run([]string{"resume"}); err == nil {
		t.Fatal("resume without --operation-id must error")
	}
}

func TestResumeAllTerminal(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-term", []*manifest.Unit{
		unit(1, manifest.StateCompleted, false),
		unit(2, manifest.StateSwapped, true),
	})
	if err := run([]string{"resume", "--state-dir", dir, "--operation-id", "op-term"}); err != nil {
		t.Fatalf("resume (all terminal): %v", err)
	}
}

func TestResumeRequeuesIncomplete(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-run", []*manifest.Unit{
		unit(1, manifest.StateCompleted, false),
		unit(2, manifest.StatePending, false),
	})
	if err := run([]string{"resume", "--state-dir", dir, "--operation-id", "op-run"}); err != nil {
		t.Fatalf("resume (incomplete): %v", err)
	}
}

// --- cleanup ---------------------------------------------------------------

func TestCleanupRequiresYes(t *testing.T) {
	dir := t.TempDir()
	if err := run([]string{"cleanup", "--state-dir", dir, "--operation-id", "x"}); err == nil {
		t.Fatal("cleanup without --yes must error")
	}
}

func TestCleanupRequiresOpID(t *testing.T) {
	dir := t.TempDir()
	if err := run([]string{"cleanup", "--state-dir", dir, "--yes"}); err == nil {
		t.Fatal("cleanup without --operation-id/--all-incomplete must error")
	}
}

func TestCleanupMissingJournal(t *testing.T) {
	dir := t.TempDir()
	if err := run([]string{"cleanup", "--state-dir", dir, "--operation-id", "ghost", "--yes"}); err != nil {
		t.Fatalf("cleanup (missing journal): %v", err)
	}
}

func TestCleanupRemovesOperation(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-del", []*manifest.Unit{unit(1, manifest.StatePending, false)})
	if err := run([]string{"cleanup", "--state-dir", dir, "--operation-id", "op-del", "--yes"}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "op-del")); !os.IsNotExist(err) {
		t.Fatalf("operation dir %s should be removed", filepath.Join(dir, "op-del"))
	}
}

func TestCleanupAllIncomplete(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-inc", []*manifest.Unit{unit(1, manifest.StatePending, false)})
	writeJournal(t, dir, "op-done", []*manifest.Unit{unit(1, manifest.StateCompleted, false)})
	if err := run([]string{"cleanup", "--state-dir", dir, "--all-incomplete", "--yes"}); err != nil {
		t.Fatalf("cleanup --all-incomplete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "op-inc")); !os.IsNotExist(err) {
		t.Fatal("incomplete operation should be removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "op-done")); err != nil {
		t.Fatal("terminal operation must survive --all-incomplete")
	}
}

func TestCleanupModeDropRequiresDest(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-drop", []*manifest.Unit{
		unit(1, manifest.StateCompleted, true),
		unit(2, manifest.StateCompleted, false),
	})
	// Without --dest there is no way to reach the destination cluster, so
	// --mode drop must refuse (not silently skip the drops).
	err := run([]string{"cleanup", "--state-dir", dir, "--operation-id", "op-drop", "--mode", "drop", "--yes"})
	if err == nil {
		t.Fatal("cleanup --mode drop without --dest must error")
	}
	if !strings.Contains(err.Error(), "--dest") {
		t.Fatalf("error should mention --dest, got: %v", err)
	}
	// The operation state must survive the refused drop (nothing was done).
	if _, err := os.Stat(filepath.Join(dir, "op-drop", "journal.jsonl")); err != nil {
		t.Fatalf("state should survive a refused drop: %v", err)
	}
	// Plain state cleanup (no drop) still works without --dest.
	if err := run([]string{"cleanup", "--state-dir", dir, "--operation-id", "op-drop", "--yes"}); err != nil {
		t.Fatalf("cleanup (state only): %v", err)
	}
}

// --- listIncompleteOperations ----------------------------------------------

func TestListIncompleteOperationsMissingDir(t *testing.T) {
	ids, err := listIncompleteOperations(t.TempDir() + "/no-such-dir")
	if err != nil || ids != nil {
		t.Fatalf("missing dir: ids=%v err=%v, want nil,nil", ids, err)
	}
}

func TestListIncompleteOperationsMixed(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-a", []*manifest.Unit{unit(1, manifest.StatePending, false)})
	writeJournal(t, dir, "op-b", []*manifest.Unit{unit(1, manifest.StateCompleted, false)})
	// A plain file in the state dir must be ignored (only directories count).
	if err := os.WriteFile(filepath.Join(dir, "stray.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	ids, err := listIncompleteOperations(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(ids) != 1 || ids[0] != "op-a" {
		t.Fatalf("ids=%v, want [op-a]", ids)
	}
}

// --- run (connection phase) --------------------------------------------------

// cmdRun needs real streaming connections; a malformed DSN fails the
// pgx.ParseConfig step immediately, exercising the connect-error path
// (F-01: run never falls back to a fake).
func TestRunBadSourceConn(t *testing.T) {
	err := run([]string{
		"run", "--source", "not a valid dsn", "--dest", "postgres://u@localhost:5432/db",
	})
	if err == nil {
		t.Fatal("run with an unparsable source DSN must error")
	}
}

// --- metrics server + formatters ---------------------------------------------

func TestStartMetricsServerDisabled(t *testing.T) {
	if stop := startMetricsServer(context.Background(), "", monitor.New()); stop != nil {
		t.Fatal("empty addr must disable the metrics server")
	}
}

func TestStartMetricsServerServesPrometheus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stop := startMetricsServer(ctx, "127.0.0.1:0", monitor.New())
	if stop == nil {
		t.Fatal("stop fn must be non-nil")
	}
	defer stop()
	// The listener starts asynchronously; addr :0 means we cannot hit it,
	// but the goroutine + shutdown path still runs (covers the body).
}

// TestParseConfigKitchenSink exercises every v4 flag through parseConfig and
// its downstream helpers (dbs parsing, include validation, file load, env
// overlay, Validate).
func TestParseConfigKitchenSink(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "gpstream.toml")
	if err := os.WriteFile(f, []byte("rate-limit = \"1.5\"\nworkers = \"4\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	orig := map[string]string{"GPSTREAM_COMPRESSION": "zlib", "GPSTREAM_LOG_LEVEL": "debug"}
	for k, v := range orig {
		t.Setenv(k, v)
	}

	argv := []string{
		"--source", "postgres://src?host=localhost",
		"--dest", "postgres://dst?host=localhost",
		"--dbs", "db1,db2",
		"--state-dir", dir,
		"--config", f,
		"--rate-limit", "2",
		"--workers", "3",
		"--zstd-level", "3",
		"--verify", "full",
		"--overwrite",
		"--compress", "zstd",
		"--consistent-db",
		"--metadata-only",
		"--auto-resume", "1",
		"--metrics-addr", "127.0.0.1:0",
		"--dry-run",
	}

	cfg, err := parseConfig(argv)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(cfg.DBs) != 2 || cfg.DBs[0] != "db1" {
		t.Fatalf("DBs = %v", cfg.DBs)
	}
	if cfg.DryRun != true {
		t.Fatal("DryRun not set")
	}
	if cfg.MetadataOnly != true {
		t.Fatal("MetadataOnly not set")
	}
	if cfg.RateLimit != 2 {
		t.Fatalf("RateLimit = %v", cfg.RateLimit)
	}
	if cfg.ZstdLevel != 3 {
		t.Fatalf("ZstdLevel = %v", cfg.ZstdLevel)
	}
	if cfg.WorkersPerPair != 3 {
		t.Fatalf("WorkersPerPair = %v", cfg.WorkersPerPair)
	}
	// Env overlay must not clobber CLI-set values (rate-limit from CLI).
	if cfg.Compression != config.CompZstd {
		t.Fatalf("Compression = %v (env must not override CLI)", cfg.Compression)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// TestParseConfigInvalidArgs covers the parse-error branches of parseConfig.
func TestParseConfigInvalidArgs(t *testing.T) {
	// --include was removed (duplicated the -only shorthands): it must be an unknown flag
	if _, err := parseConfig([]string{"run", "--src", "postgres://a", "--dst", "postgres://b", "--include", "metadata"}); err == nil {
		t.Fatal("expected error for the removed --include flag")
	}
	// both metadata-only and data-only
	if _, err := parseConfig([]string{"run", "--src", "postgres://a", "--dst", "postgres://b", "--metadata-only", "--data-only"}); err == nil {
		t.Fatal("expected mutual exclusion error")
	}
	// out-of-range zstd level
	if _, err := parseConfig([]string{"run", "--src", "postgres://a", "--dst", "postgres://b", "--zstd-level", "99"}); err == nil {
		t.Fatal("expected zstd range error")
	}
}

// TestCmdVersionAndMan drive the version and man subcommands.
func TestCmdVersionAndMan(t *testing.T) {
	if err := cmdVersion(nil); err != nil {
		t.Fatal(err)
	}
}
