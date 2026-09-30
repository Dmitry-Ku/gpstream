package main

import (
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/monitor"
)

// TestRunNoArgs: an empty invocation must be a clear error.
func TestRunNoArgs(t *testing.T) {
	err := run(nil)
	if err == nil {
		t.Fatal("expected an error for no subcommand")
	}
	if !strings.Contains(err.Error(), "no subcommand") {
		t.Fatalf("got %v", err)
	}
}

// TestRunUnknown: an unknown subcommand must error.
func TestRunUnknown(t *testing.T) {
	err := run([]string{"nope"})
	if err == nil {
		t.Fatal("expected an error for an unknown subcommand")
	}
	if !strings.Contains(err.Error(), "unknown subcommand") {
		t.Fatalf("got %v", err)
	}
}

// TestRunHelp: the help alias is a no-op success.
func TestRunHelp(t *testing.T) {
	for _, a := range [][]string{{"-h"}, {"--help"}, {"help"}} {
		if err := run(a); err != nil {
			t.Fatalf("help %v: %v", a, err)
		}
	}
}

// TestHumanBytes covers the unit ladder (B → KiB → MiB → GiB → …).
func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1 << 20, "1.0 MB"},
		{1 << 30, "1.0 GB"},
		{1 << 40, "1.0 TB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.n); got != c.want {
			t.Fatalf("humanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

// TestHumanDuration covers the second/minute/hour branches.
func TestHumanDuration(t *testing.T) {
	cases := []struct {
		sec  int
		want string
	}{
		{0, "0s"},
		{-1, "0s"},
		{30, "30s"},
		{60, "1m"},
		{120, "2m"},
		{3599, "59m"},
		{3600, "60m"}, // 3600s is NOT > 3600s, so it stays in the minute branch
		{3660, "1h1m"},
	}
	for _, c := range cases {
		if got := humanDuration(c.sec); got != c.want {
			t.Fatalf("humanDuration(%d) = %q, want %q", c.sec, got, c.want)
		}
	}
}

// TestAtoiAtf cover the parse-with-default helpers.
func TestAtoiAtf(t *testing.T) {
	if atoi("42") != 42 {
		t.Fatal("atoi")
	}
	if atoi("bogus") != 0 {
		t.Fatal("atoi default")
	}
	if atof("3.5") != 3.5 {
		t.Fatal("atof")
	}
	if atof("bogus") != 0 {
		t.Fatal("atof default")
	}
}

// TestSplitKV covers the src=dst split (incl. no '=' and multiple '=').
func TestSplitKV(t *testing.T) {
	k, v := splitKV("a=b")
	if k != "a" || v != "b" {
		t.Fatalf("splitKV(a=b) = %q,%q", k, v)
	}
	k, v = splitKV("x=a=b") // split on the FIRST '='
	if k != "x" || v != "a=b" {
		t.Fatalf("splitKV(x=a=b) = %q,%q", k, v)
	}
	k, v = splitKV("noeq")
	if k != "noeq" || v != "" {
		t.Fatalf("splitKV(noeq) = %q,%q", k, v)
	}
}

// TestSetRename covers the nil-map and override cases.
func TestSetRename(t *testing.T) {
	m := setRename(nil, "a", "b")
	if m["a"] != "b" {
		t.Fatalf("setRename nil = %v", m)
	}
	m = setRename(m, "c", "d")
	if m["c"] != "d" || m["a"] != "b" {
		t.Fatalf("setRename accumulate = %v", m)
	}
}

// TestDryRunNoCluster exercises the dry-run command end-to-end against the
// built-in healthy fake (no live cluster required). It must succeed and print
// the plan summary.
func TestDryRunNoCluster(t *testing.T) {
	if err := run([]string{"dry-run"}); err != nil {
		t.Fatalf("dry-run: %v", err)
	}
}

// TestV2SubcommandGone: v4 §8 dropped the v2 `plan` subcommand (planning is now
// `gpstream dry-run`). Invoking `plan` must be an unknown-subcommand error, not
// a silent success.
func TestV2SubcommandGone(t *testing.T) {
	for _, sub := range []string{"plan", "copy", "metrics", "report", "logs",
		"indexes", "cancel", "rollback", "package-info", "upgrade-check",
		"security-check", "compatibility-check"} {
		err := run([]string{sub})
		if err == nil {
			t.Fatalf("%q: expected unknown-subcommand error, got nil", sub)
		}
		if !strings.Contains(err.Error(), "unknown subcommand") {
			t.Fatalf("%q: got %v", sub, err)
		}
	}
}

// TestStatusNeedsID: status without --operation-id must error.
func TestStatusNeedsID(t *testing.T) {
	if err := run([]string{"status"}); err == nil {
		t.Fatal("expected an error without --operation-id")
	}
}

// TestCleanupNeedsYes: cleanup without --yes must error (destructive guard).
func TestCleanupNeedsYes(t *testing.T) {
	if err := run([]string{"cleanup", "--operation-id", "none"}); err == nil {
		t.Fatal("expected an error without --yes")
	}
}

// TestManNoFile: with no man file present it prints the built-in summary.
func TestManNoFile(t *testing.T) {
	if err := run([]string{"man"}); err != nil {
		t.Fatalf("man: %v", err)
	}
}

// TestCopyNoCluster: copy must fail cleanly (no live cluster) rather than
// fabricate a result (FR-01/CR-7). An empty connstring cannot connect.
func TestCopyNoCluster(t *testing.T) {
	if err := run([]string{"copy"}); err == nil {
		t.Fatal("expected copy to fail without a reachable cluster")
	}
}

// TestDoctorNoCluster: CR-7 — the doctor must NOT report healthy for an
// unconfigured cluster. Before the fix it returned a healthy fake and passed;
// now a missing connstring is a reachability FAIL, so the command must error.
func TestDoctorNoCluster(t *testing.T) {
	if err := run([]string{"doctor"}); err == nil {
		t.Fatal("expected doctor to fail (report FAIL) without a reachable cluster, got nil")
	}
}

// ---- v2 commands (§4.2, §4.4, §4.5, §4.7, §4.12, §4.13, §4.16) ----

// TestResumeNoID: resume without --operation-id must error.
func TestResumeNoID(t *testing.T) {
	if err := run([]string{"resume"}); err == nil {
		t.Fatal("expected resume to error without --operation-id")
	}
}

// TestResumeEmptyJournal: resume against an empty (unpopulated) journal is a
// clean no-op (all units terminal → nothing to resume).
func TestResumeEmptyJournal(t *testing.T) {
	if err := run([]string{"resume", "--operation-id", "none", "--state-dir", "/tmp/gpstream-test-state"}); err != nil {
		t.Fatalf("resume empty journal: %v", err)
	}
}

// TestCancelNeedsYes: cancel is destructive; without --yes it must error.
func TestCancelNeedsYes(t *testing.T) {
	if err := run([]string{"cancel", "--operation-id", "none"}); err == nil {
		t.Fatal("expected cancel to error without --yes")
	}
}

// TestRollbackNeedsYes: rollback is destructive; without --yes it must error.
func TestRollbackNeedsYes(t *testing.T) {
	if err := run([]string{"rollback", "--operation-id", "none"}); err == nil {
		t.Fatal("expected rollback to error without --yes")
	}
}

// TestLogsNoID: logs without --operation-id must error.
func TestLogsNoID(t *testing.T) {
	if err := run([]string{"logs"}); err == nil {
		t.Fatal("expected logs to error without --operation-id")
	}
}

// TestMetricsEndpoint: v4 dropped the `metrics` subcommand; metrics are served
// from the `--metrics-addr` endpoint (F-04). The Prometheus text is exposed by
// monitor.Metrics — assert the wire format directly instead of a dead subcommand.
func TestMetricsEndpoint(t *testing.T) {
	m := monitor.New()
	body := m.Prometheus()
	if !strings.Contains(body, "gpstream_") {
		t.Fatalf("PrometheusText() missing gpstream_ series: %.200s", body)
	}
}

// TestSecurityCheckNoCluster: CR-7/v2 §4.12 — security-check must FAIL (not pass)
// for an unconfigured cluster, mirroring doctor's no-mask guarantee.
func TestSecurityCheckNoCluster(t *testing.T) {
	if err := run([]string{"security-check"}); err == nil {
		t.Fatal("expected security-check to fail without a reachable cluster")
	}
}

// TestIndexesNoID: indexes without --operation-id must error.
func TestIndexesNoID(t *testing.T) {
	if err := run([]string{"indexes"}); err == nil {
		t.Fatal("expected indexes to error without --operation-id")
	}
}

// TestHelpFlag: --help/-h in a subcommand's flags renders usage (no error),
// while any other unknown flag is rejected.
func TestHelpFlag(t *testing.T) {
	if err := run([]string{"run", "--help"}); err != nil {
		t.Fatalf("run --help: %v", err)
	}
	if err := run([]string{"dry-run", "-h"}); err != nil {
		t.Fatalf("dry-run -h: %v", err)
	}
	if err := run([]string{"doctor", "--source", "s", "--help"}); err != nil {
		t.Fatalf("doctor --help: %v", err)
	}
	if err := run([]string{"run", "--bogus-flag"}); err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

// TestParseConfigDiagnostics: the surviving dry-run diagnostic flag binds.
func TestParseConfigDiagnostics(t *testing.T) {
	c, err := parseConfig([]string{"--locale-check"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if !c.LocaleCheck {
		t.Fatalf("locale-check not bound: %+v", c)
	}
}

// TestParseConfigSelection: --dbs/--rate-limit units/--map-table bind into
// the config.
func TestParseConfigSelection(t *testing.T) {
	c, err := parseConfig([]string{
		"--dbs", "analytics",
		"--rate-limit", "80gbit",
		"--map-table", "public.t1=schema1.t2",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(c.DBs) != 1 || c.DBs[0] != "analytics" {
		t.Fatalf("databases = %v, want [analytics]", c.DBs)
	}
	if c.RateLimit != 80 {
		t.Fatalf("rate-limit 80gbit = %g, want 80 (Gbps)", c.RateLimit)
	}
	if len(c.RenameTable) != 1 || c.RenameTable["public.t1"] != "schema1.t2" {
		t.Fatalf("map-table = %v", c.RenameTable)
	}
}

// TestParseConfigRateLimitUnits: --rate-limit accepts bps/mbit/gbit units
// (v2 §4.13, §26 uses --rate-limit 80gbit).
func TestParseConfigRateLimitUnits(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"80gbit", 80},
		{"50mbit", 0.05},
		{"1000", 1000},
		{"0", 0},
	}
	for _, tc := range cases {
		c, err := parseConfig([]string{"--rate-limit", tc.in})
		if err != nil {
			t.Fatalf("parseConfig(--rate-limit %s): %v", tc.in, err)
		}
		if c.RateLimit != tc.want {
			t.Fatalf("rate-limit %q = %g, want %g", tc.in, c.RateLimit, tc.want)
		}
	}
}
