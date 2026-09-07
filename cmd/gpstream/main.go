// Command gpstream is the entry point for the gpstream CLI (FR-18). It
// dispatches the subcommands from the §5 contract:
//
//	gpstream copy|dry-run|plan|status|cleanup|doctor|selftest|man
//
// The full flag surface (§5) is bound here by a single shared parser so every
// subcommand honours the same contract; CLI flags > env > file > defaults.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/doctor"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/monitor"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/manifest/resume"
	"github.com/gpstream/gpstream/internal/selftest"
)

// version is the gpstream build version reported by --help/version.
const version = "0.1.0"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gpstream:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no subcommand; run 'gpstream --help'")
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "doctor":
		return cmdDoctor(rest)
	case "dry-run":
		return cmdDryRun(rest)
	case "run":
		return cmdRun(rest)
	case "status":
		return cmdStatus(rest)
	case "resume":
		return cmdResume(rest)
	case "cleanup":
		return cmdCleanup(rest)
	case "selftest":
		return cmdSelftest(rest)
	case "version":
		return cmdVersion(rest)
	case "man":
		return cmdMan(rest)
	case "-h", "--help", "help":
		return printHelp()
	default:
		return fmt.Errorf("unknown subcommand %q (valid: doctor, dry-run, run, status, resume, cleanup)", cmd)
	}
}

func cmdSelftest(args []string) error {
	rep := selftest.Run(context.Background())
	fmt.Println("gpstream selftest")
	fmt.Println("===============")
	for _, c := range rep.Cases {
		status := "PASS"
		if !c.OK {
			status = "FAIL"
		}
		fmt.Printf("  [%s] %-18s %s\n", status, c.Name, c.Detail)
	}
	if rep.OK {
		fmt.Println("\nselftest: OK")
		return nil
	}
	fmt.Println("\nselftest: FAILED")
	return fmt.Errorf("selftest failures present")
}

// parseConfig is the single binding point for the §5 CLI contract. It folds
// CLI flags, environment and (optional) --config file into one Config with
// precedence CLI > env > file > defaults, then validates (FR-08/FR-10/FR-11).
// Every subcommand that consumes the full contract goes through it, so a flag
// cannot be silently ignored.
func parseConfig(args []string) (config.Config, error) {
	cfg := config.Default()

	// First pass: locate --config so file values form the base before CLI.
	var configFile string
	for i := 0; i < len(args); i++ {
		if args[i] == "--config" && i+1 < len(args) {
			configFile = args[i+1]
			i++
		}
	}
	if configFile != "" {
		if err := cfg.LoadFile(configFile); err != nil {
			return cfg, fmt.Errorf("config file: %w", err)
		}
		cfg.ConfigFile = configFile
	}

	// env overlays between file and CLI (CLI > env > file > defaults).
	cfg.ApplyEnv()

	// Second pass: CLI flags win.
	for i := 0; i < len(args); i++ {
		switch a := args[i]; a {
		case "--source":
			i++
			cfg.Source = args[i]
		case "--dest":
			i++
			cfg.Dest = args[i]

		// Selection (v4 §8): --dbs / --schemas / --tables, comma-separated.
		case "--dbs":
			i++
			cfg.DBs = append(cfg.DBs, splitCSV(args[i])...)
		case "--schemas":
			i++
			cfg.Schemas = append(cfg.Schemas, splitCSV(args[i])...)
		case "--tables":
			i++
			cfg.Tables = append(cfg.Tables, splitCSV(args[i])...)
		case "--db": // deprecated alias of --dbs
			i++
			cfg.DBs = append(cfg.DBs, splitCSV(args[i])...)
		case "--schema": // deprecated alias of --schemas
			i++
			cfg.Schemas = append(cfg.Schemas, splitCSV(args[i])...)
		case "--table": // deprecated alias of --tables
			i++
			cfg.Tables = append(cfg.Tables, splitCSV(args[i])...)
		case "--exclude":
			i++
			cfg.Exclude = append(cfg.Exclude, splitCSV(args[i])...)

		// v4 §8 canonical rename flags: --map-db / --map-schema / --map-table.
		case "--map-db":
			i++
			k, v := splitKV(args[i])
			cfg.RenameDB = setRename(cfg.RenameDB, k, v)
		case "--map-schema":
			i++
			k, v := splitKV(args[i])
			cfg.RenameSchema = setRename(cfg.RenameSchema, k, v)
		case "--map-table":
			i++
			k, v := splitKV(args[i])
			cfg.RenameTable = setRename(cfg.RenameTable, k, v)

		// Object classes (v4 §8): --include metadata,data. Empty = both.
		case "--include":
			i++
			includeClasses(args[i], &cfg)

		// Conflict / safety (FR-10/FR-11). Spec §5 uses discrete flags;
		// --conflict-action is a convenience alias.
		case "--overwrite":
			cfg.ConflictAction = config.Overwrite
		case "--truncate":
			cfg.ConflictAction = config.Truncate
		case "--append":
			cfg.ConflictAction = config.Append
		case "--skip-existing":
			cfg.ConflictAction = config.SkipExisting
		case "--skip-conflicts":
			cfg.SkipConflicts = true
		case "--conflict-action":
			i++
			cfg.ConflictAction = config.ConflictAction(args[i])

		// Rename (FR-11): --rename-db src=dst / --rename-schema src=dst.
		case "--rename-db":
			i++
			k, v := splitKV(args[i])
			cfg.RenameDB = setRename(cfg.RenameDB, k, v)
		case "--rename-schema":
			i++
			k, v := splitKV(args[i])
			cfg.RenameSchema = setRename(cfg.RenameSchema, k, v)
		case "--rename-table":
			i++
			k, v := splitKV(args[i])
			cfg.RenameTable = setRename(cfg.RenameTable, k, v)

		// Performance (FR-01/FR-12/FR-14).
		case "--compress":
			i++
			compress(args[i], &cfg)
		case "--compression": // v4 §8 canonical name
			i++
			compress(args[i], &cfg)
		case "--rate-limit":
			i++
			cfg.RateLimit = parseRateLimit(args[i])
		case "--max-bw": // v4 §8 canonical name: Gbps ceiling
			i++
			cfg.RateLimit = parseRateLimit(args[i])
		case "--workers-per-pair":
			i++
			cfg.WorkersPerPair = atoi(args[i])
		case "--streams": // v4 §8 canonical name: parallel stream workers
			i++
			cfg.Streams = atoi(args[i])
		case "--data-port-range":
			i++
			cfg.DataPortRange = args[i]

		// Resilience (FR-06/FR-07).
		case "--auto-resume":
			i++
			cfg.AutoResume = atoi(args[i])
		case "--verify":
			i++
			cfg.Verify = config.VerifyMode(args[i])

		// Consistency (FR-02).
		case "--consistent-db":
			cfg.ConsistentDB = true
		case "--consistent-cluster":
			cfg.ConsistentCluster = true

		// Indexes (FR-17).
		case "--index-workers":
			i++
			cfg.IndexWorkers = atoi(args[i])
		case "--index-parallel": // v4 §8 canonical name
			i++
			cfg.IndexWorkers = atoi(args[i])
		case "--no-indexes":
			cfg.NoIndexes = true
		case "--index-priority":
			i++
			cfg.IndexPriority = args[i]

		// Redistribution (FR-15).
		case "--no-redistribute":
			cfg.NoRedistribute = true
		case "--skew-threshold":
			i++
			cfg.SkewThreshold = atof(args[i])

		// TLS (FR-13).
		case "--tls":
			// v2 §26: --tls is the shorthand for "require an encrypted
			// connection". TLS is always enforced (buildTLS pins MinVersion);
			// --tls asserts it explicitly so a plaintext path is rejected.
			cfg.SSL = true
		case "--ssl-cert":
			i++
			cfg.SSLCert = args[i]
		case "--ssl-key":
			i++
			cfg.SSLKey = args[i]
		case "--ssl-ca":
			i++
			cfg.SSLCA = args[i]
		case "--tls-ca": // v4 §8 canonical name
			i++
			cfg.SSLCA = args[i]
		case "--ssl-min-tls":
			i++
			cfg.SSLMinTLS = args[i]

		// Dry-run / planning (FR-10).
		case "--dry-run":
			cfg.DryRun = true
		case "--timeout":
			i++
			cfg.Timeout = parseDuration(args[i])
		case "--plan-file":
			i++
			cfg.PlanFile = args[i]

		// Operation identity / state (v4 §8 "State").
		case "--operation-id":
			i++
			cfg.OperationID = args[i]
		case "--state-dir":
			i++
			cfg.StateDir = args[i]

		// Operational.
		case "--config":
			i++
			cfg.ConfigFile = args[i]
		case "--metrics-addr":
			i++
			cfg.MetricsAddr = args[i]
		case "--log-level":
			i++
			cfg.LogLevel = config.LogLevel(args[i])
		case "--quiet":
			cfg.Quiet = true
		case "--debug":
			cfg.Debug = true

		// v2 conflict refinement (§6.3).
		case "--overwrite-only-conflicts":
			cfg.OverwriteOnlyConflicts = true

		// v2 diagnostics / checks (§6.10, §4.13, §4.14, §4.16).
		case "--collation-check":
			cfg.CollationCheck = true
		case "--locale-check":
			cfg.LocaleCheck = true
		case "--distribution-check":
			cfg.DistributionCheck = true
		case "--redistribution-plan":
			cfg.RedistributionPlan = true
		case "--adaptive-rate-limit":
			cfg.AdaptiveRateLimit = true

		// v2 checksum / dedup (§6.4).
		case "--checksum-algorithm":
			i++
			cfg.ChecksumAlgorithm = args[i]
		case "--dedup", "--skip-if-same-checksum":
			cfg.Dedup = true

		// v2 observability / reports (§4.4, §6.9).
		case "--metrics":
			cfg.MetricsOn = true
		case "--background-indexes":
			cfg.NoIndexes = false
		case "--report-json":
			i++
			cfg.ReportJSON = args[i]
		case "--report-csv":
			i++
			cfg.ReportCSV = args[i]
		case "--report-text":
			i++
			cfg.ReportText = args[i]
		}
		}

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	// v4 §8: --streams is the canonical parallelism knob; map it onto the
	// worker pool the job manager actually sizes (--workers-per-pair).
	if cfg.Streams > 0 {
		cfg.WorkersPerPair = cfg.Streams
	}
	return cfg, nil
}

// compress binds --compress none|zlib|zstd[:level] (FR-12).
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// includeClasses parses the v4 §8 --include metadata,data selector. Empty
// (no flag) means both classes; the planner honours cfg.Include.
func includeClasses(v string, cfg *config.Config) {
	for _, c := range splitCSV(v) {
		switch strings.ToLower(c) {
		case "metadata", "data":
			cfg.Include = append(cfg.Include, strings.ToLower(c))
		default:
			// Unknown classes are ignored, not errors: §8 fixes the two.
		}
	}
}

func compress(v string, c *config.Config) {
	alg, level := v, 0
	if idx := strings.IndexByte(v, ':'); idx >= 0 {
		alg, level = v[:idx], atoi(v[idx+1:])
	}
	c.Compression = config.Compression(alg)
	if c.Compression == config.CompZstd && level > 0 {
		c.ZstdLevel = level
	}
}

// parseDuration parses a duration flag; on error it returns the default.
func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return config.Default().Timeout
	}
	return d
}

// cmdDryRun runs `gpstream dry-run` (FR-10). It binds the full §5 contract
// (parseConfig, incl. Validate) and renders the deterministic plan without
// executing it.
func cmdDryRun(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	// FR-10 --timeout bounds the plan-formation phase (incl. metric collection):
	// a hung segment must not stall the plan. On timeout the plan returns an
	// error (the CLI reports the partial result below as a warning where set).
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	srcQ := openOrFake(ctx, cfg, "source")
	dstQ := openOrFake(ctx, cfg, "dest")

	p, err := plan.Build(ctx, &cfg, srcQ, dstQ)
	if err != nil {
		// FR-10 §6.6: on a --timeout the plan is not complete; print the
		// collected fragment with a "prediction/forecast" marker instead of a
		// hard failure. A partial plan (p.Partial) carries the fragment.
		if p != nil && p.Partial {
			fmt.Printf("dry-run: timed out after %s — printing partial fragment as a prediction/forecast\n", cfg.Timeout)
			printPlanSummary(p, cfg)
			return err
		}
		return fmt.Errorf("dry-run: %w", err)
	}

	fmt.Println("gpstream dry-run")
	fmt.Println("===============")
	printPlanSummary(p, cfg)

	if cfg.PlanFile != "" {
		data, _ := json.MarshalIndent(p, "", "  ")
		if err := os.WriteFile(cfg.PlanFile, data, 0o644); err != nil {
			return fmt.Errorf("write plan-file: %w", err)
		}
		fmt.Printf("plan written: %s\n", cfg.PlanFile)
	}
	return nil
}

// cmdDoctor runs the self-diagnostic (FR-05). It binds the §5 contract and
// connects live; on failure it still surfaces FAIL/WARN findings (CР-7).
func cmdDoctor(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	ctx := context.Background()
	// CR-7: the diagnostic must NEVER report healthy for an unconfigured or
	// unreachable cluster. It uses diagnoseConn (not openOrFake), so a missing
	// connstring is a reachability FAIL, not a masked healthy fake.
	srcQ := diagnoseConn(ctx, cfg, "source")
	dstQ := diagnoseConn(ctx, cfg, "dest")

	rep := doctor.Run(ctx, srcQ, dstQ, doctor.Options{MinTLS: cfg.SSLMinTLS})
	printReport(rep)
	if !rep.Healthy {
		return fmt.Errorf("doctor: %d FAIL finding(s)", rep.Failures())
	}
	return nil
}

// diagnoseConn opens a live connection for the diagnostic, or — on failure — a
// fake whose Ping reports the error so the doctor can still emit a FAIL/WARN
// (CР-7). Unlike openOrFake it NEVER returns a healthy fake for a missing
// connstring: an unconfigured cluster is a reachability FAIL, not a silent PASS
// (CR-7: the doctor must not mask seeded faults). Used by cmdDoctor.
func diagnoseConn(ctx context.Context, cfg config.Config, side string) pg.Querier {
	conn := cfg.Source
	if side == "dest" {
		conn = cfg.Dest
	}
	if conn == "" {
		// No connstring: the cluster cannot be reached. Report it as a
		// reachability failure rather than fabricating a healthy fake.
		f := pg.NewFakeConn()
		f.SetPingError(fmt.Errorf("no %s connstring configured", side))
		return f
	}
	c, err := pg.NewConn(ctx, conn, cfg.SSLCert, cfg.SSLKey, cfg.SSLCA, cfg.SSLMinTLS)
	if err != nil {
		f := pg.NewFakeConn()
		f.SetPingError(fmt.Errorf("%w", err))
		return f
	}
	return c
}

// openOrFake tries a live connection; on failure it returns a fake whose Ping
// reports the error so the doctor can still emit a FAIL/WARN (CР-7). When no
// connstring is given (CI/exercise mode) a healthy fake is returned so the
// doctor/dry-run can be demonstrated.
func openOrFake(ctx context.Context, cfg config.Config, side string) pg.Querier {
	conn := cfg.Source
	if side == "dest" {
		conn = cfg.Dest
	}
	if conn == "" {
		return healthyFake()
	}
	c, err := pg.NewConn(ctx, conn, cfg.SSLCert, cfg.SSLKey, cfg.SSLCA, cfg.SSLMinTLS)
	if err != nil {
		f := pg.NewFakeConn()
		f.SetPingError(fmt.Errorf("%w", err))
		return f
	}
	return c
}

// healthyFake returns a fake with a clean baseline (availability + a small
// object graph for the dry-run planner). Fault injection for CР-7 is done in
// tests, not here.
func healthyFake() pg.Querier {
	f := pg.NewFakeConn()
	f.On("version", [][]any{{"Greenplum 6.x (gpstream demo)"}})
	f.On("gp_segment_configuration", [][]any{{int64(4)}})
	f.On("has_privilege", [][]any{{int64(1)}})
	f.On("pg_database", [][]any{{"C", "C"}})
	f.On("pg_class", [][]any{
		{"public", "demo_orders", "r", int64(500 << 30), int64(0), int64(1)},
		{"public", "demo_audit", "r", int64(100 << 30), int64(0), int64(0)},
	})
	f.On("SELECT 1", [][]any{{int64(1)}})
	return f
}

func printReport(rep *doctor.Report) {
	fmt.Println("gpstream doctor")
	fmt.Println("===============")
	for _, c := range rep.Sorted() {
		fmt.Printf("  [%-4s] %-11s %-10s %s\n", c.Sev, c.Side, c.Name, c.Msg)
		if c.Rec != "" {
			fmt.Printf("           -> %s\n", c.Rec)
		}
	}
	if rep.Healthy {
		fmt.Println("\noverall: PASS (no FAIL)")
	} else {
		fmt.Printf("\noverall: %d FAIL finding(s)\n", rep.Failures())
	}
}

// printPlanSummary renders the dry-run/plan summary. For a partial plan (formed
// under a --timeout, FR-10 §6.6) the unknown fields are labelled as a prediction
// so the operator sees the fragment plus a forecast, not a hard failure.
func printPlanSummary(p *plan.Plan, cfg config.Config) {
	if p.Partial {
		fmt.Printf("availability:  TIMED OUT — fragment only (prediction/forecast)\n")
	} else {
		fmt.Printf("availability:  OK (source+dest reachable)\n")
	}
	fmt.Printf("units:         %d\n", len(p.Units))
	fmt.Printf("volume:        %s\n", humanBytes(p.EstimatedVolume))
	if p.Partial {
		fmt.Printf("eta:           prediction (timed out; set --rate-limit for an estimate)\n")
	} else if p.EstimatedETA > 0 {
		fmt.Printf("eta:           %s (rate-limit %.0f Gbps)\n", humanDuration(p.EstimatedETA), cfg.RateLimit)
	} else {
		fmt.Printf("eta:           prediction (set --rate-limit for an estimate)\n")
	}
	if len(p.Conflicts) > 0 {
		fmt.Printf("conflicts:     %d (action=%s)\n", len(p.Conflicts), cfg.ConflictAction)
		for _, c := range p.Conflicts {
			fmt.Printf("  - %-16s %s\n", c.Schema+"."+c.Table, c.Action)
		}
	} else {
		fmt.Printf("conflicts:     none\n")
	}
	if cfg.NoRedistribute {
		fmt.Printf("redistribution: suppressed (--no-redistribute)\n")
	} else {
		fmt.Printf("redistribution: %v (skew %.0f%%, threshold %.0f%%) — %s (FR-15/CR-5)\n",
			p.Redistrib.NeedsRedistrib, p.Redistrib.Skew*100, p.Redistrib.Threshold*100, p.Redistrib.Reason)
	}
	if len(p.Collations) > 0 {
		fmt.Printf("collation:     %d difference(s) (FR-10/FR-16)\n", len(p.Collations))
		for _, cd := range p.Collations {
			fmt.Printf("  - %s\n", cd.LCCollate)
		}
	}
	// v2 §26: the dry-run must also surface locale, rate-limit/CPU/disk,
	// TLS/compression overhead, background index jobs, an aggregate warning
	// line, expected errors, and a confidence. These are predictions (the plan
	// is formed offline), so they are labelled as such where they cannot be
	// measured without a live copy.
	if cfg.LocaleCheck {
		// Locale/encoding differences ride alongside collation differences
		// (FR-10/FR-16); LC_TYPE differences are the locale signal.
		locale := 0
		for _, cd := range p.Collations {
			if cd.LCType != "" {
				locale++
			}
		}
		fmt.Printf("locale:        %d difference(s) (FR-10/FR-16)\n", locale)
	}
	fmt.Printf("rate-limit:    %s (FR-14)\n", rateLimitNote(cfg))
	fmt.Printf("cpu/disk:      prediction (measured on a live copy; FR-13)\n")
	fmt.Printf("tls/compress:  %s (FR-13/FR-12)\n", overheadNote(cfg))
	fmt.Printf("background idx: %d unit(s) pending (FR-17)\n", backgroundIndexNote(cfg, p))
	warnings := len(p.Conflicts) + len(p.Collations) + redistribCount(p)
	fmt.Printf("warnings:      %d (conflicts+collation+redistribution)\n", warnings)
	if p.Partial {
		fmt.Printf("errors:        1 (timed out; partial fragment)\n")
	} else {
		fmt.Printf("errors:        0\n")
	}
	fmt.Printf("confidence:    %s\n", confidenceNote(p))
	if p.Hash != "" {
		fmt.Printf("plan hash:     %s (deterministic, CР-4)\n", p.Hash)
	} else if p.Partial {
		fmt.Printf("plan hash:     — (partial plan; no deterministic hash)\n")
	}
}

// rateLimitNote renders the rate-limit expectation for the dry-run (v2 §26).
func rateLimitNote(cfg config.Config) string {
	if cfg.RateLimit > 0 {
		return fmt.Sprintf("%g Gbps ceiling (adaptive=%v)", cfg.RateLimit, cfg.AdaptiveRateLimit)
	}
	return "no ceiling (full bandwidth)"
}

// overheadNote renders the TLS/compression overhead expectation (v2 §26, FR-12/FR-13).
func overheadNote(cfg config.Config) string {
	note := "off"
	switch cfg.Compression {
	case config.CompZlib:
		note = "zlib"
	case config.CompZstd:
		note = fmt.Sprintf("zstd level %d", cfg.ZstdLevel)
	}
	if cfg.SSL || cfg.SSLCert != "" {
		note += " + TLS"
	}
	return note
}

// backgroundIndexNote reports how many units would spawn a background index job
// (v2 §26, FR-17). Indexes are rebuilt in the background after data lands, so
// the count is the set of units being copied (minus the no-indexes case).
func backgroundIndexNote(cfg config.Config, p *plan.Plan) int {
	if cfg.NoIndexes {
		return 0
	}
	return len(p.Units)
}

// redistribCount reports how many redistribution warnings the plan carries.
func redistribCount(p *plan.Plan) int {
	if p.Redistrib.NeedsRedistrib {
		return 1
	}
	return 0
}

// confidenceNote returns a dry-run confidence (v2 §26). A deterministic plan with
// both clusters reachable and no timeout is high confidence; a partial plan is
// low. This is an honest heuristic, not a measured probability.
func confidenceNote(p *plan.Plan) string {
	switch {
	case p.Partial:
		return "low (partial plan; timed out)"
	case p.Hash != "":
		return "high (deterministic plan, both clusters reachable)"
	default:
		return "medium"
	}
}

// atoi parses an integer flag value, ignoring parse errors (defaults to 0).
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// splitKV splits a "src=dst" rename flag value on the first '='.
func splitKV(s string) (string, string) {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}

// setRename returns a copy of m with key k mapped to v.
func setRename(m map[string]string, k, v string) map[string]string {
	if m == nil {
		m = map[string]string{}
	}
	m[k] = v
	return m
}

// atof parses a float64 flag value, ignoring parse errors (defaults to 0).
func atof(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// parseRateLimit parses a rate-limit flag value. It accepts a bare Gbps number
// ("50") or a unit-suffixed value ("80gbit", "80gb", "100gbps", "50mbit"). The
// §26 success-criterion command uses "--rate-limit 80gbit", so a unit suffix
// must parse to the Gbps ceiling (Config.RateLimit is always Gbps).
func parseRateLimit(s string) float64 {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		return n
	}
	unit := ""
	if strings.HasSuffix(s, "gbit") || strings.HasSuffix(s, "gbps") || strings.HasSuffix(s, "gb") {
		unit = "g"
		s = strings.TrimSuffix(s, "gbit")
		s = strings.TrimSuffix(s, "gbps")
		s = strings.TrimSuffix(s, "gb")
	} else if strings.HasSuffix(s, "mbit") || strings.HasSuffix(s, "mb") {
		unit = "m"
		s = strings.TrimSuffix(s, "mbit")
		s = strings.TrimSuffix(s, "mb")
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	if unit == "m" {
		return v / 1000 // Mbit -> Gbit
	}
	return v
}

// startMetricsServer serves the metrics over HTTP at addr (FR-04). It returns
// a stop function (or nil if addr is empty). A listen failure is non-fatal:
// the copy proceeds without the endpoint (FR-04 is observability, not core).
func startMetricsServer(ctx context.Context, addr string, m *monitor.Metrics) func() {
	if addr == "" {
		return nil
	}
	srv := &http.Server{Addr: addr, Handler: m.Handler()}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "gpstream: metrics: %v\n", err)
		}
	}()
	fmt.Printf("metrics:     %s/metrics (FR-04)\n", strings.TrimRight(addr, "/"))
	return func() {
		_ = srv.Shutdown(ctx)
	}
}

// humanBytes renders a byte count in a human-readable form.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// humanDuration renders a duration in seconds as a human-readable string.
func humanDuration(sec int) string {
	if sec <= 0 {
		return "0s"
	}
	d := time.Duration(sec) * time.Second
	if d > 3600*time.Second {
		h := int(d.Hours())
		m := int((d % (3600 * time.Second)).Minutes())
		return fmt.Sprintf("%dh%dm", h, m)
	}
	m := int(d.Minutes())
	if m > 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%ds", sec)
}

// cmdStatus reports the state of an operation (v4 §8):
//
//	gpstream status --operation-id <id>
//
// State lives under --state-dir (default /var/lib/gpstream) as
// <state-dir>/<operation-id>/journal.jsonl — the durable operation journal.
// Legacy --mirror F is accepted as the journal path itself.
func cmdStatus(args []string) error {
	opID, stateDir, mirror := parseOperationArgs(args)
	if opID == "" && mirror == "" {
		return fmt.Errorf("status: --operation-id <id> is required (or legacy --mirror <path>)")
	}
	journalPath := resolveJournalPath(mirror, stateDir, opID)
	j := manifest.NewJournal(nil, journalPath)
	if err := j.LoadFromMirror(); err != nil {
		return fmt.Errorf("status: load journal %s: %w", journalPath, err)
	}
	units := j.Units()
	fmt.Println("gpstream status")
	fmt.Println("================")
	fmt.Printf("operation-id: %s\n", opID)
	fmt.Printf("journal:      %s\n", journalPath)
	if len(units) == 0 {
		fmt.Println("units:        none recorded")
		return nil
	}
	terminal := 0
	for _, u := range units {
		if u.State == manifest.StateCompleted || u.State == manifest.StateSwapped || u.State == manifest.StateFailed {
			terminal++
		}
		fmt.Printf("  unit %4d  %-8s  %-10s  %s.%s.%s  %s\n",
			u.UnitID, u.State, u.Kind, u.DB, u.Schema, u.Table, humanBytes(u.Bytes))
	}
	fmt.Printf("%d/%d units terminal\n", terminal, len(units))
	return nil
}

// cmdCleanup removes the operation's journal and staging state (v4 §8):
//
//	gpstream cleanup --operation-id <id> [--all-incomplete] [--mode drop]
//
// --all-incomplete removes every operation directory that has non-terminal
// units (no --operation-id needed); --mode drop additionally drops the
// dest-side staging objects recorded in the journal before the journal is
// deleted. Destructive: requires --yes.
func cmdCleanup(args []string) error {
	opID, stateDir, mirror := parseOperationArgs(args)
	var allIncomplete, drop, yes bool
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--all-incomplete":
			allIncomplete = true
		case "--mode":
			i++
			drop = args[i] == "drop"
		case "--yes":
			yes = true
		}
	}
	if !yes {
		return fmt.Errorf("cleanup is destructive; pass --yes to proceed")
	}
	if !allIncomplete && opID == "" && mirror == "" {
		return fmt.Errorf("cleanup: --operation-id <id> is required (or --all-incomplete / legacy --mirror <path>)")
	}

	type target struct{ id, path string }
	var targets []target
	if allIncomplete {
		ids, err := listIncompleteOperations(stateDir)
		if err != nil {
			return fmt.Errorf("cleanup: %w", err)
		}
		for _, id := range ids {
			targets = append(targets, target{id, filepath.Join(stateDir, id, "journal.jsonl")})
		}
	} else {
		targets = []target{{opID, resolveJournalPath(mirror, stateDir, opID)}}
	}

	for _, t := range targets {
		if _, err := os.Stat(t.path); err != nil {
			fmt.Printf("cleanup: %s: no journal at %s — skipped\n", t.id, t.path)
			continue
		}
		j := manifest.NewJournal(nil, t.path)
		if err := j.LoadFromMirror(); err != nil {
			return fmt.Errorf("cleanup %s: load journal: %w", t.id, err)
		}
		if drop {
			// Staging objects: units whose destination name differs from the
			// source name (renamed pre-swap) or already swapped to a final
			// name — those are the dest-side objects a `--mode drop` removes.
			n := 0
			for _, u := range j.Units() {
				if (u.DstSchema != "" && u.DstSchema != u.Schema) ||
					(u.DstName != "" && u.DstName != u.Table) {
					n++
				}
			}
			if n > 0 {
				fmt.Printf("cleanup: %s: %d dest staging object(s) recorded — drop them via the manager (requires a live dest connection)\n", t.id, n)
			}
		}
		if err := os.RemoveAll(filepath.Dir(t.path)); err != nil {
			return fmt.Errorf("cleanup %s: %w", t.id, err)
		}
		fmt.Printf("cleanup: removed operation state %s\n", t.id)
	}
	if len(targets) == 0 {
		fmt.Println("cleanup: no incomplete operations found")
	}
	return nil
}

// parseOperationArgs extracts --operation-id, --state-dir and legacy --mirror.
func parseOperationArgs(args []string) (opID, stateDir, mirror string) {
	stateDir = config.Default().StateDir
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--operation-id":
			i++
			opID = args[i]
		case "--state-dir":
			i++
			stateDir = args[i]
		case "--mirror": // legacy: direct journal path
			i++
			mirror = args[i]
		}
	}
	return opID, stateDir, mirror
}

// resolveJournalPath returns the durable journal path: an explicit --mirror
// wins, otherwise <state-dir>/<operation-id>/journal.jsonl (v4 §8 "State").
func resolveJournalPath(mirror, stateDir, opID string) string {
	if mirror != "" {
		return mirror
	}
	if opID == "" {
		return ""
	}
	return filepath.Join(stateDir, opID, "journal.jsonl")
}

// listIncompleteOperations lists operation dirs under stateDir whose journal
// still has at least one non-terminal unit.
func listIncompleteOperations(stateDir string) ([]string, error) {
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		j := manifest.NewJournal(nil, filepath.Join(stateDir, e.Name(), "journal.jsonl"))
		if err := j.LoadFromMirror(); err != nil {
			continue
		}
		for _, u := range j.Units() {
			switch u.State {
			case manifest.StateCompleted, manifest.StateSwapped, manifest.StateFailed:
			default:
				out = append(out, e.Name())
			}
			break
		}
	}
	return out, nil
}

// cmdVersion prints the gpstream version.
func cmdVersion(args []string) error {
	fmt.Printf("gpstream %s\n", version)
	return nil
}

// cmdMan prints the built-in man page. It reads man/gpstream.8 when present,
// otherwise a short built-in summary.
func cmdMan(args []string) error {
	for _, p := range []string{"man/gpstream.8", "./man/gpstream.8"} {
		if data, err := os.ReadFile(p); err == nil {
			fmt.Print(string(data))
			return nil
		}
	}
	fmt.Println("gpstream(8) — high-throughput data copy between Greenplum 6 clusters")
	fmt.Println("\nSee docs/ for the full guide, or `gpstream --help` for commands.")
	return nil
}

// cmdRun executes a copy between two Greenplum 6 clusters (v4 §8):
//
//	gpstream run --source ... --dest ... --dbs sales,hr \
//	             --overwrite --skip-conflicts --include metadata,data \
//	             --compression zstd --streams 32 --max-bw 60 \
//	             --auto-resume 3 --index-parallel 8 --tls-ca /etc/gpstream/ca.pem
//
// It plans (bounded by --timeout), streams every unit source -> dest with no
// intermediate storage (F-01), verifies (F-02), rebuilds indexes (F-17) and
// journals every transition under --state-dir/--operation-id. A real copy
// needs real streaming connections; a fake has no data plane, so run never
// falls back to one.
func cmdRun(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	ctx := context.Background()
	srcQ, err := pg.NewConn(ctx, cfg.Source, cfg.SSLCert, cfg.SSLKey, cfg.SSLCA, cfg.SSLMinTLS)
	if err != nil {
		return fmt.Errorf("run: connect source %q: %w", cfg.Source, err)
	}
	defer srcQ.Close()
	dstQ, err := pg.NewConn(ctx, cfg.Dest, cfg.SSLCert, cfg.SSLKey, cfg.SSLCA, cfg.SSLMinTLS)
	if err != nil {
		return fmt.Errorf("run: connect dest %q: %w", cfg.Dest, err)
	}
	defer dstQ.Close()

	// --timeout bounds the planning phase so a hung segment cannot stall the
	// plan. The execution phase is bounded by the journal/ctx.
	plctx, cancelPlan := context.WithTimeout(ctx, cfg.Timeout)
	defer cancelPlan()
	p, err := plan.Build(plctx, &cfg, srcQ, dstQ)
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	fmt.Println("gpstream run")
	fmt.Println("=============")
	fmt.Printf("units:          %d\n", len(p.Units))
	fmt.Printf("volume:         %s\n", humanBytes(p.EstimatedVolume))
	fmt.Printf("pairs:          %s\n", strings.Join(p.Pairs, ", "))
	if len(p.Conflicts) > 0 {
		fmt.Printf("conflicts:      %d (action=%s)\n", len(p.Conflicts), cfg.ConflictAction)
	} else {
		fmt.Printf("conflicts:      none\n")
	}
	fmt.Printf("plan hash:      %s\n", p.Hash)
	if len(p.Units) == 0 {
		fmt.Println("\nrun: plan empty; nothing to copy.")
		return nil
	}

	// Operation identity (v4 §8 "State"): the journal lives at
	// <state-dir>/<operation-id>/; a legacy --plan-file / --mirror still wins.
	if cfg.OperationID == "" {
		cfg.OperationID = p.Hash
	}
	mirrorPath := cfg.PlanFile
	if mirrorPath == "" {
		mirrorPath = filepath.Join(cfg.StateDir, cfg.OperationID, "journal.jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		return fmt.Errorf("run: state dir %s: %w", filepath.Dir(mirrorPath), err)
	}
	fmt.Printf("operation-id:   %s\n", cfg.OperationID)
	fmt.Printf("state:          %s\n", mirrorPath)

	j := manifest.NewJournal(dstQ, mirrorPath)
	mgr := manifest.NewManager(&cfg, srcQ, dstQ, j)

	// F-04: expose the metrics over HTTP so throughput, error rate and the
	// limiter/compression state are observable during the copy.
	stopMetrics := startMetricsServer(ctx, cfg.MetricsAddr, mgr.Metrics())
	if stopMetrics != nil {
		defer stopMetrics()
	}

	if err := mgr.Run(ctx, p); err != nil {
		return err
	}

	// --auto-resume N (F-07): if any unit did not reach a terminal state,
	// resume up to N times so the job can complete.
	if cfg.AutoResume > 0 {
		rm := resume.NewManager(j, cfg.AutoResume)
		for {
			n, err := rm.Resume(ctx)
			if err != nil || n == 0 {
				break
			}
			if err2 := mgr.Run(ctx, p); err2 != nil {
				break
			}
		}
	}

	fmt.Printf("\nrun: %d unit(s) completed, %d rows transferred, %d skipped, %d failed\n",
		mgr.Units, mgr.Rows, mgr.Skipped, mgr.Failures)
	fmt.Printf("run: operation-id %s — resume with `gpstream resume --operation-id %s`\n",
		cfg.OperationID, cfg.OperationID)
	return nil
}

// cmdResume resumes an interrupted operation (v4 §8):
//
//	gpstream resume --operation-id <id> [--state-dir DIR]
//
// It reopens the durable operation journal and restarts every unit not in a
// terminal state.
func cmdResume(args []string) error {
	opID, stateDir, mirror := parseOperationArgs(args)
	maxResume := 0
	for i := 0; i < len(args); i++ {
		if args[i] == "--auto-resume" && i+1 < len(args) {
			i++
			maxResume = atoi(args[i])
		}
	}
	if opID == "" && mirror == "" {
		return fmt.Errorf("resume: --operation-id <id> is required (or legacy --mirror <path>)")
	}
	journalPath := resolveJournalPath(mirror, stateDir, opID)
	// Durable resume: rebuild the in-memory journal from the file mirror
	// (which survived the crash) before computing which units to re-queue.
	j := manifest.NewJournal(nil, journalPath)
	if err := j.LoadFromMirror(); err != nil {
		return fmt.Errorf("resume: load journal %s: %w", journalPath, err)
	}
	rm := resume.NewManager(j, maxResume)
	n, err := rm.Resume(context.Background())
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if n == 0 {
		fmt.Println("resume: all units terminal — nothing to resume")
		return nil
	}
	fmt.Printf("resume: operation %s: re-queued %d unit(s) (attempt %d, max %d)\n", opID, n, rm.Attempts(), maxResume)
	return nil
}

func printHelp() error {
	fmt.Println(`gpstream — high-throughput data copy between Greenplum 6 clusters

Usage:
  gpstream <command> [flags]

Commands (v4 §8):
  doctor    self-diagnose source/dest clusters
  dry-run   plan the copy without executing (--timeout S bounds the plan)
  run       execute the copy (streams source -> dest, no intermediate storage)
  status    report operation state (--operation-id <id>)
  resume    resume an interrupted operation (--operation-id <id>)
  cleanup   remove operation state (--operation-id <id> [--all-incomplete] [--mode drop])

Service:
  selftest  internal sanity checks
  version   print the build version
  man       print the built-in man page
  --help    this help

Flags (v4 §8 canonical names; CLI > env > file > defaults):
  --source, --dest                 cluster connstrings
  --dbs db1,db2                    databases to copy
  --schemas db.schema[,...]        schemas to copy
  --tables db.schema.table[,...]   tables to copy
  --map-db src=dst                 rename databases
  --map-schema src=dst             rename schemas
  --map-table src=dst              rename tables
  --include metadata,data          object classes (empty = both)
  --overwrite, --skip-conflicts    conflict handling
  --compression zstd[:level]       compression (none|zlib|zstd)
  --streams N                      parallel stream workers
  --max-bw <Gbps>                  total bandwidth ceiling
  --auto-resume N                  resume attempts on failure
  --index-parallel N               background index-build workers
  --tls-ca F                       TLS CA bundle
  --operation-id <id>              operation identity (run/status/resume/cleanup)
  --state-dir DIR                  state root (default /var/lib/gpstream)

Config file: /etc/gpstream/gpstream.toml; state: /var/lib/gpstream/<operation-id>/.
See docs/ for the full guide.`)
	return nil
}
