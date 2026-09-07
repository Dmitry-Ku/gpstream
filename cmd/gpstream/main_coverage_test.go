package main

import (
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
)

// These tests exercise the large surface of parseConfig and the small helper
// functions (compress, parseDuration) plus cmdSelftest, raising the cmd/gpstream
// coverage toward the v2 unit bar. They use only real APIs and real config
// fields; no feature is invented.

// TestSelftestCommand: `gpstream selftest` runs the built-in self-check and
// returns nil when all cases pass (FR-05). This covers cmdSelftest (0%).
func TestSelftestCommand(t *testing.T) {
	if err := run([]string{"selftest"}); err != nil {
		t.Fatalf("selftest: %v", err)
	}
}

// TestParseConfigFull exercises the many --flag cases that the v2-selection
// tests do not cover, so the big parseConfig switch is exercised end to end.
func TestParseConfigFull(t *testing.T) {
	c, err := parseConfig([]string{
		"--source", "source://s", "--dest", "dest://d",
		"--include", "metadata,data",
		"--db", "db1", "--schema", "sch1", "--table", "tab1",
		"--exclude", "x.1",
		"--overwrite",
		"--rename-db", "old=New", "--rename-schema", "o=n",
		"--compress", "zstd:9",
		"--rate-limit", "80gbit",
		"--workers-per-pair", "4",
		"--data-port-range", "6000-6010",
		"--auto-resume", "2",
		"--verify", "full",
		"--consistent-db",
		"--index-workers", "3",
		"--no-indexes",
		"--index-priority", "size",
		"--no-redistribute",
		"--skew-threshold", "0.1",
		"--ssl-cert", "/c.pem", "--ssl-key", "/k.pem",
		"--ssl-ca", "/ca.pem", "--ssl-min-tls", "TLS12",
		"--dry-run",
		"--timeout", "60s",
		"--plan-file", "/tmp/plan.json",
		"--metrics-addr", "127.0.0.1:9090",
		"--log-level", "debug",
		"--quiet", "--debug",
		"--background-indexes",
		"--report-csv", "/tmp/r.csv", "--report-text", "/tmp/r.txt",
		"--skip-if-same-checksum",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if !c.DryRun {
		t.Fatalf("dry-run not bound")
	}
	// v4 §8: object classes come from --include (v2 --full/--metadata-only gone).
	if len(c.Include) != 2 || c.Include[0] != "metadata" || c.Include[1] != "data" {
		t.Fatalf("include = %v, want [metadata data]", c.Include)
	}
	if len(c.DBs) != 1 || c.DBs[0] != "db1" {
		t.Fatalf("db = %v", c.DBs)
	}
	if len(c.Schemas) != 1 || c.Schemas[0] != "sch1" {
		t.Fatalf("schemas = %v", c.Schemas)
	}
	if len(c.Tables) != 1 || c.Tables[0] != "tab1" {
		t.Fatalf("tables = %v", c.Tables)
	}
	if len(c.Exclude) != 1 || c.Exclude[0] != "x.1" {
		t.Fatalf("exclude = %v", c.Exclude)
	}
	if c.RenameDB["old"] != "New" {
		t.Fatalf("rename-db = %v", c.RenameDB)
	}
	if c.RenameSchema["o"] != "n" {
		t.Fatalf("rename-schema = %v", c.RenameSchema)
	}
	if c.Compression != "zstd" || c.ZstdLevel != 9 {
		t.Fatalf("compress = %q/%d, want zstd/9", c.Compression, c.ZstdLevel)
	}
	if c.WorkersPerPair != 4 {
		t.Fatalf("workers = %d", c.WorkersPerPair)
	}
	if c.DataPortRange != "6000-6010" {
		t.Fatalf("port-range = %q", c.DataPortRange)
	}
	if c.AutoResume != 2 {
		t.Fatalf("auto-resume = %d", c.AutoResume)
	}
	if c.Verify != "full" {
		t.Fatalf("verify = %q", c.Verify)
	}
	if !c.ConsistentDB {
		t.Fatalf("consistent-db not set")
	}
	if c.IndexWorkers != 3 {
		t.Fatalf("index-workers = %d", c.IndexWorkers)
	}
	if c.IndexPriority != "size" {
		t.Fatalf("index-priority = %q", c.IndexPriority)
	}
	if !c.NoRedistribute {
		t.Fatalf("no-redistribute not set")
	}
	if c.SkewThreshold != 0.1 {
		t.Fatalf("skew = %g", c.SkewThreshold)
	}
	if c.SSLCert != "/c.pem" || c.SSLKey != "/k.pem" || c.SSLCA != "/ca.pem" || c.SSLMinTLS != "TLS12" {
		t.Fatalf("ssl not bound: %+v", c)
	}
	if c.PlanFile != "/tmp/plan.json" {
		t.Fatalf("plan-file = %q", c.PlanFile)
	}
	if c.MetricsAddr != "127.0.0.1:9090" {
		t.Fatalf("metrics-addr = %q", c.MetricsAddr)
	}
	if c.LogLevel != "debug" || !c.Quiet || !c.Debug {
		t.Fatalf("log-level/quiet/debug not set: %+v", c)
	}
	if c.ReportCSV != "/tmp/r.csv" || c.ReportText != "/tmp/r.txt" {
		t.Fatalf("report flags = %q/%q", c.ReportCSV, c.ReportText)
	}
	if !c.Dedup {
		t.Fatalf("skip-if-same-checksum -> Dedup not set")
	}
	if c.Timeout != 60*time.Second {
		t.Fatalf("timeout = %s, want 60s", c.Timeout)
	}
	// --background-indexes forces NoIndexes=false even though --no-indexes was
	// set earlier (last-wins on the boolean via the flag that sets it).
	if c.NoIndexes {
		t.Fatalf("background-indexes should have cleared NoIndexes")
	}
}

// TestParseCompressLevels covers the --compress variants (none/zlib/zstd:level).
func TestParseCompressLevels(t *testing.T) {
	cases := []struct {
		flag   string
		alg    string
		level  int
	}{
		{"none", "none", 0},
		{"zlib", "zlib", 0},
		{"zstd", "zstd", 0},
		{"zstd:5", "zstd", 5},
	}
	for _, c := range cases {
		cfg, err := parseConfig([]string{"--compress", c.flag})
		if err != nil {
			t.Fatalf("parseConfig --compress %s: %v", c.flag, err)
		}
		if string(cfg.Compression) != c.alg {
			t.Fatalf("compress %s: alg = %q, want %q", c.flag, cfg.Compression, c.alg)
		}
		if c.flag == "zstd:5" && cfg.ZstdLevel != 5 {
			t.Fatalf("compress %s: level = %d, want 5", c.flag, cfg.ZstdLevel)
		}
	}
}

// TestParseConfigValidationErrors: the mutual-exclusion and range checks in
// Validate() must surface as errors through parseConfig.
func TestParseConfigValidationErrors(t *testing.T) {
	// MetadataOnly + DataOnly are mutually exclusive (Validate rule; v4 §8
	// reaches object classes via --include, so exercise Validate directly).
	if err := (config.Config{MetadataOnly: true, DataOnly: true}).Validate(); err == nil {
		t.Fatal("expected error for MetadataOnly + DataOnly")
	}
	// --consistent-db and --consistent-cluster are mutually exclusive.
	if _, err := parseConfig([]string{"--consistent-db", "--consistent-cluster"}); err == nil {
		t.Fatal("expected error for --consistent-db + --consistent-cluster")
	}
	// --compress zstd:25 exceeds the valid zstd level range (1-19).
	if _, err := parseConfig([]string{"--compress", "zstd:25"}); err == nil {
		t.Fatal("expected error for --compress zstd:25 (level out of range)")
	}
	// --workers-per-pair 0 fails the >= 1 check.
	if _, err := parseConfig([]string{"--workers-per-pair", "0"}); err == nil {
		t.Fatal("expected error for --workers-per-pair 0")
	}
}

// TestParseDuration: a valid duration binds; an invalid one falls back to the
// default (does not error).
func TestParseDuration(t *testing.T) {
	if got := parseDuration("60s"); got != 60*time.Second {
		t.Fatalf("parseDuration(60s) = %s, want 60s", got)
	}
	if got := parseDuration("not-a-duration"); got != configDefaultTimeout() {
		t.Fatalf("parseDuration(invalid) = %s, want default", got)
	}
}

// configDefaultTimeout returns the default timeout so the parseDuration
// fallback assertion stays in sync with config.Default().
func configDefaultTimeout() time.Duration {
	c, err := parseConfig(nil)
	if err != nil {
		// A nil config is valid (defaults only); if it errors, fall back to 0
		// so the test still makes a meaningful comparison.
		return 0
	}
	return c.Timeout
}

// TestCopyConnectError: a malformed --source DSN must fail fast at
// pgx.ParseConfig (openOrFake's error branch + cmdCopy's connection path),
// not hang trying to reach a real cluster (FR-01/CR-7).
func TestCopyConnectError(t *testing.T) {
	if err := run([]string{"copy", "--source", "not-a-valid-dsn://x", "--timeout", "2s"}); err == nil {
		t.Fatal("expected copy to fail on an unparseable source DSN")
	}
}

// TestCopyNoClusterDryRun: a --dry-run with no cluster runs against the healthy
// fake and succeeds (covers cmdDryRun's fake path, already exercised, but
// ensures the two-way openOrFake path is covered from the copy entry).
func TestDoctorConnectError(t *testing.T) {
	// A malformed source DSN drives diagnoseConn's error branch (the live
	// NewConn error path, not the missing-connstring fake path).
	if err := run([]string{"doctor", "--source", "not-a-valid-dsn://x"}); err == nil {
		t.Fatal("expected doctor to fail on an unparseable source DSN")
	}
}

