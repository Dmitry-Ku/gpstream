package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
)

// TestPureHelpers covers the small flag/ formatting helpers that the §5 CLI
// contract and the status/dry-run renderers rely on.

func TestAtoiAtof(t *testing.T) {
	if atoi("42") != 42 || atoi("1x") != 0 || atoi("") != 0 {
		t.Errorf("atoi wrong")
	}
	if atof("1.5") != 1.5 || atof("nope") != 0 {
		t.Errorf("atof wrong")
	}
}

func TestParseBytes(t *testing.T) {
	cases := map[string]int64{
		"":     0,
		"1024": 1024,
		"512M": 512 << 20,
		"1G":   1 << 30,
		"2T":   2 << 40,
		"1k":   1 << 10,
		"xx":   0,
	}
	for in, want := range cases {
		if got := parseBytes(in); got != want {
			t.Errorf("parseBytes(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestSplitKVSetRename(t *testing.T) {
	k, v := splitKV("old=new")
	if k != "old" || v != "new" {
		t.Errorf("splitKV = %q/%q", k, v)
	}
	k, v = splitKV("only")
	if k != "only" || v != "" {
		t.Errorf("splitKV(no =) = %q/%q", k, v)
	}
	m := setRename(nil, "a", "b")
	if m["a"] != "b" {
		t.Errorf("setRename nil map: %v", m)
	}
	m = setRename(m, "c", "d")
	if m["a"] != "b" || m["c"] != "d" {
		t.Errorf("setRename existing: %v", m)
	}
}

func TestParseRateLimit(t *testing.T) {
	cases := map[string]float64{
		"":        0,
		"50":      50,
		"80gbit":  80,
		"80gbps":  80,
		"80gb":    80,
		"500mbit": 0.5,
		"500mb":   0.5,
		"junk":    0,
	}
	for in, want := range cases {
		if got := parseRateLimit(in); got != want {
			t.Errorf("parseRateLimit(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestHumanBytesU(t *testing.T) {
	if got := humanBytes(500); got != "500 B" {
		t.Errorf("humanBytes(500) = %q", got)
	}
	if got := humanBytes(2 << 30); got != "2.0 GB" {
		t.Errorf("humanBytes(2G) = %q", got)
	}
	if got := humanBytes(3 << 20); got != "3.0 MB" {
		t.Errorf("humanBytes(3M) = %q", got)
	}
	if got := humanBytesU(1 << 40); got != "1.0 TB" {
		t.Errorf("humanBytesU(1T) = %q", got)
	}
}

func TestOrDashHumanDuration(t *testing.T) {
	if orDash("") != "-" || orDash("x") != "x" {
		t.Errorf("orDash wrong")
	}
	if humanDurationSec(0) != "-" {
		t.Errorf("humanDurationSec(0)")
	}
	if got := humanDurationSec(90061); got != "1d 1h 1m" {
		t.Errorf("humanDurationSec(90061) = %q", got)
	}
	if got := humanDuration(0); got != "0s" {
		t.Errorf("humanDuration(0) = %q", got)
	}
	if got := humanDuration(120); got != "2m" {
		t.Errorf("humanDuration(120) = %q", got)
	}
	if got := humanDuration(3725); got != "1h2m" {
		t.Errorf("humanDuration(3725) = %q", got)
	}
}

func TestLoadRunStats(t *testing.T) {
	dir := t.TempDir()
	// absent -> nil, nil
	rs, err := loadRunStats(filepath.Join(dir, "op", "journal.jsonl"))
	if err != nil || rs != nil {
		t.Errorf("absent: rs=%v err=%v", rs, err)
	}
	// valid
	opDir := filepath.Join(dir, "op")
	if err := os.MkdirAll(opDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(opDir, "runstats.json"), []byte(`{"pair":"a","bytes":100,"rows":5}`), 0o644); err != nil {
		t.Fatal(err)
	}
	rs, err = loadRunStats(filepath.Join(opDir, "journal.jsonl"))
	if err != nil || rs == nil || rs.Bytes != 100 || rs.Rows != 5 || rs.Pair != "a" {
		t.Errorf("valid: rs=%+v err=%v", rs, err)
	}
	// corrupt -> error
	if err := os.WriteFile(filepath.Join(opDir, "runstats.json"), []byte(`{bad json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRunStats(filepath.Join(opDir, "journal.jsonl")); err == nil {
		t.Error("corrupt: want error")
	}
}

func TestPlanNotes(t *testing.T) {
	// Unresolved route (plain PG / no segments): coordinator-only note.
	p := &plan.Plan{}
	if got := routeSummaryLine(p); got != "not resolvable (plain-PostgreSQL or no segments) — coordinator path only" {
		t.Errorf("routeSummaryLine unresolved = %q", got)
	}

	// Resolved route with mixed units.
	p = &plan.Plan{
		Route: plan.RouteSummary{
			Resolved:           true,
			SrcSegments:        4,
			DstSegments:        8,
			EligiblePairs:      2,
			GPVersionEqual:     true,
			PossibleDirect1to1: false,
		},
		Units: []plan.Table{
			{RouteMode: "direct_1to1"},
			{RouteMode: "direct_1to1"},
			{RouteMode: "router_full"},
			{RouteMode: "coordinator"},
		},
	}
	if got := routeSummaryLine(p); got != "src=4 dst=8 segs, same-major, 2 eligible pair(s), router_full (S!=D); units: 2 direct_1to1 / 1 router_full" {
		t.Errorf("routeSummaryLine resolved = %q", got)
	}

	// Mixed-major + direct-eligible variant.
	p.Route.GPVersionEqual = false
	p.Route.PossibleDirect1to1 = true
	if got := routeSummaryLine(p); got != "src=4 dst=8 segs, mixed-major, 2 eligible pair(s), direct_1to1 possible; units: 2 direct_1to1 / 1 router_full" {
		t.Errorf("routeSummaryLine mixed = %q", got)
	}

	// rateLimitNote / overheadNote / backgroundIndexNote.
	if got := rateLimitNote(config.Config{}); got != "no ceiling (full bandwidth)" {
		t.Errorf("rateLimitNote none = %q", got)
	}
	if got := rateLimitNote(config.Config{RateLimit: 80}); got != "80 Gbps ceiling (adaptive per host-pair)" {
		t.Errorf("rateLimitNote 80 = %q", got)
	}
	if got := overheadNote(config.Config{}); got != "off" {
		t.Errorf("overheadNote off = %q", got)
	}
	if got := overheadNote(config.Config{Compression: config.CompZstd, ZstdLevel: 3}); got != "zstd level 3" {
		t.Errorf("overheadNote zstd = %q", got)
	}
	if got := overheadNote(config.Config{Compression: config.CompZlib, SSLCert: "/c.pem"}); got != "zlib + TLS" {
		t.Errorf("overheadNote zlib+tls = %q", got)
	}
	if got := backgroundIndexNote(config.Config{NoIndexes: true}, p); got != 0 {
		t.Errorf("backgroundIndexNote no-indexes = %d", got)
	}
	if got := backgroundIndexNote(config.Config{}, p); got != 4 {
		t.Errorf("backgroundIndexNote = %d", got)
	}

	// confidenceNote: partial -> low, hashed -> high, else medium.
	if got := confidenceNote(&plan.Plan{Partial: true}); got != "low (partial plan; timed out)" {
		t.Errorf("confidenceNote partial = %q", got)
	}
	if got := confidenceNote(&plan.Plan{Hash: "abc"}); got != "high (deterministic plan, both clusters reachable)" {
		t.Errorf("confidenceNote hashed = %q", got)
	}
	if got := confidenceNote(&plan.Plan{}); got != "medium" {
		t.Errorf("confidenceNote plain = %q", got)
	}
}

// TestCmdVersionMan runs the trivial subcommands.
func TestCmdVersionMan(t *testing.T) {
	if err := run([]string{"version"}); err != nil {
		t.Errorf("version: %v", err)
	}
	if err := run([]string{"man"}); err != nil {
		t.Errorf("man: %v", err)
	}
	if err := run([]string{"help"}); err != nil {
		t.Errorf("help: %v", err)
	}
	if err := run(nil); err == nil {
		t.Error("no subcommand: want error")
	}
	if err := run([]string{"bogus"}); err == nil {
		t.Error("unknown subcommand: want error")
	}
}

// TestCmdDryRunFake: with no source/dest connstrings the dry-run falls back to
// the healthy fake (openOrFake) and renders the full plan summary — covering
// cmdDryRun, printPlanSummary and the note helpers end-to-end.
func TestCmdDryRunFake(t *testing.T) {
	planFile := filepath.Join(t.TempDir(), "plan.json")
	if err := run([]string{"dry-run", "--plan-file", planFile}); err != nil {
		t.Fatalf("dry-run (fake): %v", err)
	}
	if _, err := os.Stat(planFile); err != nil {
		t.Errorf("plan-file not written: %v", err)
	}

	// --map-db / --map-schema flag parsing (splitKV/setRename in parseConfig).
	if err := run([]string{"dry-run", "--map-db", "sales=finance", "--map-schema", "old=new"}); err != nil {
		t.Errorf("dry-run renames: %v", err)
	}
}

// TestCmdDoctorUnreachable: doctor against an unconfigured/unreachable cluster
// must emit FAIL findings (CR-7) and return an error.
func TestCmdDoctorUnreachable(t *testing.T) {
	if err := run([]string{"doctor"}); err == nil {
		t.Error("doctor unconfigured: want error (CR-7: no silent PASS)")
	}
	// An unreachable DSN (closed port, no listener) takes the connect-failure
	// path of diagnoseConn.
	err := run([]string{"doctor", "--source", "postgres://nouser:nowhere@127.0.0.1:1/db?sslmode=disable&connect_timeout=2"})
	if err == nil {
		t.Error("doctor unreachable: want error")
	}
}

// TestCmdRunConnectFails: run requires real pools; an unreachable source must
// fail at the connect step, before any copy.
func TestCmdRunConnectFails(t *testing.T) {
	err := run([]string{"run",
		"--source", "postgres://nouser:nowhere@127.0.0.1:1/db?sslmode=disable&connect_timeout=2",
		"--dest", "postgres://nouser:nowhere@127.0.0.1:1/db?sslmode=disable&connect_timeout=2",
	})
	if err == nil {
		t.Error("run unreachable: want error")
	}
}

// TestCmdStatusWithJournal: status against a seeded operation (journal.jsonl +
// runstats.json) renders the unit table and the FR-04 live table.
func TestCmdStatusWithJournal(t *testing.T) {
	dir := t.TempDir()
	opDir := filepath.Join(dir, "op1")
	if err := os.MkdirAll(opDir, 0o755); err != nil {
		t.Fatal(err)
	}
	journal := `{"unit_id":1,"db":"sales","schema":"public","table":"orders","kind":"table","state":"completed","row_count":100,"bytes":1073741824,"attempts":1,"epoch":2}
{"unit_id":2,"db":"sales","schema":"public","table":"audit","kind":"table","state":"copying","row_count":5,"bytes":100,"attempts":1,"epoch":2}
`
	if err := os.WriteFile(filepath.Join(opDir, "journal.jsonl"), []byte(journal), 0o644); err != nil {
		t.Fatal(err)
	}
	runstats := `{"pair":"a->b","decoded":2048,"bytes":4096,"rows":105,"throughput_bytes":1024,"eta_seconds":90061,"active_flows":3,"by_status":{"completed":1,"copying":1},"per_table":[{"Table":"public.orders","Rows":100,"Wire":1073741824,"Logical":1073741824,"State":"completed"}]}`
	if err := os.WriteFile(filepath.Join(opDir, "runstats.json"), []byte(runstats), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"status", "--operation-id", "op1", "--state-dir", dir}); err != nil {
		t.Errorf("status with journal: %v", err)
	}
}

// TestCmdStatusCleanupErrors exercises the argument-validation and
// journal-absent paths of status and cleanup.
func TestCmdStatusCleanupErrors(t *testing.T) {
	dir := t.TempDir()
	// status: no id at all
	if err := run([]string{"status"}); err == nil {
		t.Error("status: want error (no id)")
	}
	// status: journal absent -> empty journal, "units: none recorded", no error.
	if err := run([]string{"status", "--operation-id", "op1", "--state-dir", dir}); err != nil {
		t.Errorf("status absent journal: %v", err)
	}
	// cleanup: destructive without --yes
	if err := run([]string{"cleanup", "--operation-id", "op1", "--state-dir", dir}); err == nil {
		t.Error("cleanup without --yes: want error")
	}
	// cleanup: --yes but nothing to clean
	if err := run([]string{"cleanup", "--yes"}); err == nil {
		t.Error("cleanup --yes without id: want error")
	}
	// cleanup: drop mode without --dest
	if err := run([]string{"cleanup", "--yes", "--mode", "drop", "--operation-id", "op1", "--state-dir", dir}); err == nil {
		t.Error("cleanup drop without --dest: want error")
	}
	// cleanup --all-incomplete on an empty state dir: no targets, no error.
	if err := run([]string{"cleanup", "--yes", "--all-incomplete", "--state-dir", dir}); err != nil {
		t.Errorf("cleanup --all-incomplete empty: %v", err)
	}
}
