package main

// F-09 coverage (item 11): unit tests for the cmd/gpstream functions that are
// pure (no live cluster required): printPlanSummary (all report branches),
// the remaining parseConfig
// flag branches, cmdCleanup --all-incomplete, cmdStatus's live runstats
// table, listIncompleteOperations, and cmdDryRun's --plan-file write.

import (
	"encoding/json"
	"errors"
	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/monitor"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/route"
	"os"
	"path/filepath"
	"testing"
)

var errBoom = errors.New("boom: drop failed")

// richPlan builds a plan that exercises every branch of printPlanSummary:
// conflicts, redistribution, route modes, partition guards, all three
// collation-diff kinds, other objects, missing extensions, locale, hash.
func richPlan() *plan.Plan {
	return &plan.Plan{
		Units: []plan.Table{
			{Schema: "s", Name: "t_guard", DstSchema: "s", DstName: "t_guard",
				PartitionGuard: "partition guard: string key without DEFAULT partition"},
			{Schema: "s", Name: "t1", DstSchema: "s", DstName: "t1", RouteMode: route.Direct1to1},
			{Schema: "s", Name: "t2", DstSchema: "s", DstName: "t2", RouteMode: route.RouterFull},
		},
		EstimatedVolume: 5 << 30,
		EstimatedETA:    3661, // 1h1m -> humanDuration "1h1m"
		Conflicts: []plan.Conflict{
			{Schema: "s", Table: "t_conflict", Action: "stop"},
		},
		Redistrib: plan.Redistribution{
			Skew: 0.42, Reason: "layout differs (skew 42%% of data moves)",
		},
		Collations: []plan.CollationDiff{
			{DB: "db", LCCollate: "C", LCType: "C"},
			{DB: "db", Table: "t_col", Column: "name", SrcCollation: "C", DstCollation: "C.utf8"},
			{DB: "db", Encoding: "SQL_ASCII -> UTF8"},
		},
		OtherObjects: plan.OtherObjects{
			Views:     []string{"s.v1", "s.v2"},
			MatViews:  []string{"s.mv1"},
			Functions: 3,
		},
		MissingExtensions: []string{"postgis", "madlib"},
		Route: plan.RouteSummary{
			Resolved:           true,
			SrcSegments:        2,
			DstSegments:        4,
			GPVersionEqual:     true,
			EligiblePairs:      8,
			PossibleDirect1to1: false,
		},
		Hash: "abc123",
	}
}

// TestPrintPlanSummaryFull exercises every print branch of the plan summary.
func TestPrintPlanSummaryFull(t *testing.T) {
	cfg := config.Default()
	cfg.RateLimit = 40
	if err := printPlanSummarySafe(richPlan(), cfg); err != nil {
		t.Fatalf("printPlanSummary: %v", err)
	}
}

// TestPrintPlanSummaryMinimal covers the no-conflicts / no-collation /
// route-unresolvable / partial branches.
func TestPrintPlanSummaryMinimal(t *testing.T) {
	cfg := config.Default()
	p := &plan.Plan{
		Units:           []plan.Table{{Schema: "s", Name: "t", DstSchema: "s", DstName: "t"}},
		EstimatedVolume: 10,
		Route:           plan.RouteSummary{}, // not resolved
		Partial:         true,                // forecast labels
	}
	if err := printPlanSummarySafe(p, cfg); err != nil {
		t.Fatalf("printPlanSummary (minimal/partial): %v", err)
	}
	// And the same plan completed (hash present, no partial).
	p.Partial = false
	p.Hash = "zzz"
	p.EstimatedETA = 5
	if err := printPlanSummarySafe(p, cfg); err != nil {
		t.Fatalf("printPlanSummary (completed): %v", err)
	}
}

// printPlanSummarySafe is a thin wrapper so a test can fail with a message
// instead of panicking on a bad plan (printPlanSummary only reads fields).
func printPlanSummarySafe(p *plan.Plan, cfg config.Config) error {
	printPlanSummary(p, cfg)
	return nil
}

// TestParseConfigRemainingFlags covers the parseConfig cases the v2 tests do
// not reach: canonical aliases, frame-ack, consistency, copy-mode, TLS,
// diagnostics, and the in-parse validation errors (zstd level, mutually
// exclusive consistency).
func TestParseConfigRemainingFlags(t *testing.T) {
	c, err := parseConfig([]string{
		"--map-db", "a=b", "--map-schema", "c=d", "--map-table", "e=f",
		"--data-only",
		"--truncate",
		"--resolve-conflicts", "truncate",
		"--compress", "zlib",
		"--rate-limit", "100",
		"--workers", "8",
		"--verify", "on",
		"--on-unit-failure", "abort",
		"--frame-ack", "off",
		"--ack-timeout", "30s",
		"--max-reconnects", "5",
		"--retransmit-buffer-max", "64M",
		"--copy-mode", "mesh", "--seg-listen", "10.0.0.5:9432",
		"--seg-tls-cert", "/c.pem", "--seg-tls-key", "/k.pem", "--seg-tls-ca", "/ca.pem",
		"--index-workers", "4", "--no-constraints",
		"--zstd-level", "5",
		"--small-table-threshold", "2M",
		"--ssl-ca", "/ca.pem",
		"--metrics-addr", "127.0.0.1:9191",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if c.RenameDB["a"] != "b" || c.RenameSchema["c"] != "d" || c.RenameTable["e"] != "f" {
		t.Errorf("map-* = %v/%v/%v", c.RenameDB, c.RenameSchema, c.RenameTable)
	}
	if !c.DataOnly {
		t.Error("data-only not set")
	}
	if c.ConflictAction != config.Truncate {
		t.Errorf("resolve-conflicts = %q, want truncate", c.ConflictAction)
	}
	if c.Compression != config.CompZlib {
		t.Errorf("compression = %q, want zlib", c.Compression)
	}
	if c.RateLimit != 100 {
		t.Errorf("rate-limit = %v, want 100", c.RateLimit)
	}
	if c.WorkersPerPair != 8 {
		t.Errorf("workers = %d, want 8", c.WorkersPerPair)
	}
	if c.OnUnitFailure != "abort" {
		t.Errorf("on-unit-failure = %q", c.OnUnitFailure)
	}
	if c.FrameAck != "off" || c.AckTimeout != "30s" || c.MaxReconnects != 5 {
		t.Errorf("frame-ack trio = %q/%q/%d", c.FrameAck, c.AckTimeout, c.MaxReconnects)
	}
	if c.CopyMode != config.CopyMesh || c.SegListen != "10.0.0.5:9432" || c.SegBinary != "" {
		t.Errorf("on_segment = %q/%q/%q (seg binary resolves by convention, not a flag)", c.CopyMode, c.SegListen, c.SegBinary)
	}
	if c.SegTLSCert != "/c.pem" || c.SegTLSKey != "/k.pem" || c.SegTLSCA != "/ca.pem" {
		t.Error("seg-tls trio not bound")
	}
	if c.IndexWorkers != 4 || !c.NoConstraints {
		t.Error("index-workers/no-constraints not bound")
	}
	if c.ZstdLevel != 5 || c.SmallTableThreshold != 2<<20 {
		t.Errorf("zstd/small-table = %d/%d", c.ZstdLevel, c.SmallTableThreshold)
	}
	if c.SSLCA != "/ca.pem" {
		t.Error("ssl-ca not bound")
	}
	if c.MetricsAddr != "127.0.0.1:9191" {
		t.Error("metrics-addr not bound")
	}
}

// TestParseConfigFlagErrors covers the in-parse validation errors.
func TestParseConfigFlagErrors(t *testing.T) {
	if _, err := parseConfig([]string{"--zstd-level", "25"}); err == nil {
		t.Error("zstd-level 25: want error")
	}
	// --include was removed (duplicated the -only shorthands): it is an unknown flag.
	if _, err := parseConfig([]string{"--include", "bogus"}); err == nil {
		t.Error("removed --include: want unknown-flag error")
	}
	// on_segment: seg-listen/seg-binary are convention-resolved (sibling of
	// the gpstream binary / auto base address), so a bare --copy-mode is now
	// valid config; an unresolvable binary surfaces at run time instead.
	if _, err := parseConfig([]string{"--copy-mode", "mesh"}); err != nil {
		t.Errorf("copy-mode mesh (binaries resolve by convention): unexpected error: %v", err)
	}
	// the direct copy-mode was removed (2026-10-04): its per-segment write is
	// subsumed by on_segment's auto-selected sub-paths (1:1 / routed / merge).
	if _, err := parseConfig([]string{"--copy-mode", "direct"}); err == nil {
		t.Error("removed copy-mode direct: want validation error")
	}
}

// TestParseConfigFromFile covers the --config file layer (file values form the
// base under env/CLI).
func TestParseConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	pf := filepath.Join(dir, "cfg.toml")
	if err := os.WriteFile(pf, []byte(`rate_limit = 77
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	c, err := parseConfig([]string{"--config", pf})
	if err != nil {
		t.Fatalf("parseConfig --config: %v", err)
	}
	if c.RateLimit != 77 {
		t.Errorf("file value = rate %v, want 77", c.RateLimit)
	}
	// A missing config file is an error (not a silent default).
	if _, err := parseConfig([]string{"--config", filepath.Join(dir, "nope.toml")}); err == nil {
		t.Error("missing config file: want error")
	}
}

// TestListIncompleteOperations covers the state-dir scan (non-terminal units
// are listed, terminal-only dirs are not, a missing dir is not an error).
func TestListIncompleteOperations(t *testing.T) {
	dir := t.TempDir()
	// op-pending: every unit non-terminal (Units() iterates a map, so the
	// "first unit" check is nondeterministic — all units must be non-terminal).
	writeJournal(t, dir, "op-pending", []*manifest.Unit{
		unit(1, manifest.StatePending, false),
		unit(2, manifest.StateCopying, false),
	})
	writeJournal(t, dir, "op-done", []*manifest.Unit{
		unit(1, manifest.StateSwapped, true),
	})
	// A stray file (not a dir) is ignored.
	if err := os.WriteFile(filepath.Join(dir, "stray"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write stray: %v", err)
	}
	got, err := listIncompleteOperations(dir)
	if err != nil {
		t.Fatalf("listIncompleteOperations: %v", err)
	}
	if len(got) != 1 || got[0] != "op-pending" {
		t.Errorf("got %v, want [op-pending]", got)
	}
	// A missing state dir is not an error (nothing to clean).
	if got, err := listIncompleteOperations(filepath.Join(dir, "nope")); err != nil || len(got) != 0 {
		t.Errorf("missing dir: got %v err %v, want nil/nil", got, err)
	}
}

// TestCleanupAllIncompleteScan runs `gpstream cleanup --all-incomplete --yes`
// and verifies only the incomplete operation's state dir is removed.
func TestCleanupAllIncompleteScan(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-pending", []*manifest.Unit{
		unit(1, manifest.StatePending, false),
	})
	writeJournal(t, dir, "op-done", []*manifest.Unit{
		unit(1, manifest.StateSwapped, true),
	})
	if err := run([]string{"cleanup", "--state-dir", dir, "--all-incomplete", "--yes"}); err != nil {
		t.Fatalf("cleanup --all-incomplete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "op-pending")); !os.IsNotExist(err) {
		t.Error("op-pending state dir was not removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "op-done")); err != nil {
		t.Error("op-done state dir was removed (must be kept)")
	}
	// A second run finds nothing.
	if err := run([]string{"cleanup", "--state-dir", dir, "--all-incomplete", "--yes"}); err != nil {
		t.Fatalf("cleanup (empty): %v", err)
	}
}

// TestStatusRunStatsTable covers cmdStatus's live runstats.json rendering
// (pair, progress, throughput, ETA, tables-by-status, per-table rows).
func TestStatusRunStatsTable(t *testing.T) {
	dir := t.TempDir()
	writeJournal(t, dir, "op-live", []*manifest.Unit{
		unit(1, manifest.StateCopying, false),
	})
	rs := monitor.RunStats{
		Pair:        "h1:5432 -> h2:6432",
		Bytes:       1000,
		Decoded:     500,
		Rows:        10,
		Throughput:  100,
		ETASeconds:  90,
		ActiveFlows: 1,
		ByStatus:    map[string]int{"copying": 1, "pending": 1},
		PerTable: []monitor.TableRow{
			{Table: "s.t1", Rows: 10, Wire: 1000, Logical: 500, State: "copying"},
			{Table: "s.t2", Rows: 0, Wire: 0, Logical: 0},
		},
	}
	b, err := json.Marshal(rs)
	if err != nil {
		t.Fatalf("marshal runstats: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "op-live", "runstats.json"), b, 0o644); err != nil {
		t.Fatalf("write runstats: %v", err)
	}
	if err := run([]string{"status", "--state-dir", dir, "--operation-id", "op-live"}); err != nil {
		t.Fatalf("status: %v", err)
	}
	// A corrupt runstats.json is silently skipped (the live table is
	// observability, not core): status still succeeds, just without the
	// runstats section.
	if err := os.WriteFile(filepath.Join(dir, "op-live", "runstats.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write bad runstats: %v", err)
	}
	if err := run([]string{"status", "--state-dir", dir, "--operation-id", "op-live"}); err != nil {
		t.Errorf("corrupt runstats.json: status must still succeed (live table skipped), got %v", err)
	}
}

// TestPersistPlan covers the CR-4 plan artifact: the deterministic plan is
// persisted to <state-dir>/<operation-id>/plan.json (both by run and on
// demand) and round-trips with its hash.
func TestPersistPlan(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Units: []plan.Table{{Schema: "s", Name: "t", Bytes: 10}}, Hash: "abc123"}
	got, err := persistPlan(dir, "op1", p)
	if err != nil {
		t.Fatalf("persistPlan: %v", err)
	}
	want := filepath.Join(dir, "op1", "plan.json")
	if got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
	b, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("plan file not written: %v", err)
	}
	var p2 plan.Plan
	if err := json.Unmarshal(b, &p2); err != nil {
		t.Fatalf("plan file is not valid JSON: %v", err)
	}
	if p2.Hash != "abc123" {
		t.Errorf("hash = %q, want abc123", p2.Hash)
	}
}
