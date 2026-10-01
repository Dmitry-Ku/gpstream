// Command gpstream is the entry point for the gpstream CLI (FR-18). It
// dispatches the subcommands from the §5 contract:
//
//	gpstream run|dry-run|status|resume|cleanup|doctor|selftest|man
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
	"github.com/gpstream/gpstream/internal/manifest/resume"
	"github.com/gpstream/gpstream/internal/monitor"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/selftest"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// version is the gpstream build version reported by --help/version.
// Overridable at build time: go build -ldflags "-X main.version=<ver>"
// (the Makefile, scripts/package.sh and the release workflow set it from
// the git tag).
var version = "0.1.0"

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
	case "bench":
		return cmdBench(rest)
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
	case "-h", "--help", "help":
		return printHelp()
	default:
		return fmt.Errorf("unknown subcommand %q (valid: doctor, dry-run, run, bench, status, resume, cleanup, selftest, version, help)", cmd)
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
// errHelpRequested is a sentinel returned by parseConfig when the user passes
// --help/-h among a subcommand's flags; the caller renders printHelp() instead
// of surfacing a parse error.
var errHelpRequested = errors.New("help requested")

func parseConfig(args []string) (config.Config, error) {
	cfg := config.Default()

	// --help/-h anywhere in the subcommand's flags: render usage (the caller
	// checks errHelpRequested) instead of treating it as an unknown flag.
	for _, a := range args {
		if a == "--help" || a == "-h" {
			return cfg, errHelpRequested
		}
	}

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
			cfg.SourceFromFile = false // the CLI wins: no file provenance
		case "--dest":
			i++
			cfg.Dest = args[i]
			cfg.DestFromFile = false // the CLI wins: no file provenance

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

		// v4 F-11: --metadata-only (structure without data) / --data-only
		// (into existing structure). Mutual exclusion is enforced in Validate.
		case "--metadata-only":
			cfg.MetadataOnly = true
		case "--data-only":
			cfg.DataOnly = true

		// Conflict / safety (FR-10/FR-11). The DEFAULT is strict: conflicts
		// STOP the run (FR-10 §8). --resolve-conflicts selects the policy;
		// --overwrite / --truncate are the two common explicit opt-ins.
		case "--overwrite":
			cfg.ConflictAction = config.Overwrite
		case "--truncate":
			cfg.ConflictAction = config.Truncate
		case "--resolve-conflicts":
			i++
			cfg.ConflictAction = config.ConflictAction(args[i])

		// Performance (FR-01/FR-12/FR-14).
		case "--compress":
			i++
			compress(args[i], &cfg)
		case "--rate-limit":
			i++
			cfg.RateLimit = parseRateLimit(args[i])
		case "--workers":
			i++
			cfg.WorkersPerPair = atoi(args[i])

		// Resilience (FR-06/FR-07).
		case "--auto-resume":
			i++
			cfg.AutoResume = atoi(args[i])
		case "--verify":
			i++
			cfg.Verify = config.VerifyMode(args[i])
		case "--on-unit-failure":
			// spec F-06: continue (default) = a failed unit is recorded and
			// the run keeps going; abort = stop at the first failure.
			i++
			cfg.OnUnitFailure = args[i]

		// Frame-level ACK / RST-resume (DESIGN §4.6, Tier-1, R2).
		case "--frame-ack":
			i++
			cfg.FrameAck = args[i]
		case "--ack-timeout":
			i++
			cfg.AckTimeout = args[i]
		case "--max-reconnects":
			i++
			cfg.MaxReconnects = atoi(args[i])
		case "--retransmit-buffer-max":
			i++
			cfg.RetransmitBufferMax = parseBytes(args[i])

		// Consistency (FR-02).
		case "--consistent-db":
			cfg.RequestConsistentDB() // explicit: conflicts with the other scopes
		case "--consistent-cluster":
			cfg.ConsistentCluster = true
		case "--consistent-tables":
			cfg.ConsistentTables = true

		// Transport (T-B, DESIGN §1.3). --copy-mode selects the source data
		// path; on_segment needs the segreceiver binary path (on the segment
		// hosts) and the controller listen address the segreceivers dial.
		case "--copy-mode":
			i++
			cfg.CopyMode = config.CopyMode(args[i])
		case "--seg-listen":
			i++
			cfg.SegListen = args[i]
		case "--seg-binary":
			i++
			cfg.SegBinary = args[i]
		// Data-plane TLS for the on_segment hop (F-13). Separate from
		// --ssl-* (which configures the PG connections): the segment stream
		// can be encrypted even when the PG connections are not.
		case "--seg-tls-cert":
			i++
			cfg.SegTLSCert = args[i]
		case "--seg-tls-key":
			i++
			cfg.SegTLSKey = args[i]
		case "--seg-tls-ca":
			i++
			cfg.SegTLSCA = args[i]

		// Indexes (FR-17).
		case "--index-workers":
			i++
			cfg.IndexWorkers = atoi(args[i])
		case "--no-indexes":
			cfg.NoIndexes = true
		case "--no-constraints":
			cfg.NoConstraints = true
		case "--index-priority":
			i++
			cfg.IndexPriority = args[i]

		// Compression level (F-12): --zstd-level 1-19.
		case "--zstd-level":
			i++
			n := atoi(args[i])
			if n < 1 || n > 19 {
				return cfg, fmt.Errorf("zstd level must be 1-19, got %d", n)
			}
			cfg.ZstdLevel = n

		// Small-table auto-disable threshold (F-12): --small-table-threshold <bytes>
		// (suffixes K/M/G allowed; 0 disables the auto-disable). Tables smaller
		// than this are copied with compression OFF (compressor overhead).
		case "--small-table-threshold":
			i++
			cfg.SmallTableThreshold = parseBytes(args[i])
			if cfg.SmallTableThreshold < 0 {
				return cfg, fmt.Errorf("small-table-threshold must be >= 0, got %d", cfg.SmallTableThreshold)
			}

		// TLS (FR-13).
		case "--ssl-cert":
			i++
			cfg.SSLCert = args[i]
		case "--ssl-key":
			i++
			cfg.SSLKey = args[i]
		case "--ssl-ca":
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
			cfg.MarkTimeoutExplicit(parseDuration(args[i]))
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

		default:
			return cfg, fmt.Errorf("unknown flag %q (see `gpstream --help`)", a)
		}
	}

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}

	// F-13/F-08: surface config-file security notes (password in a plaintext
	// DSN, config file wider than 0600) before any data moves. Advisory only.
	for _, w := range cfg.SecurityWarnings() {
		fmt.Fprintf(os.Stderr, "gpstream: [config] %s\n", w)
	}
	for _, w := range cfg.AdvisoryWarnings() {
		fmt.Fprintf(os.Stderr, "gpstream: [config] %s\n", w)
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
		if errors.Is(err, errHelpRequested) {
			return printHelp()
		}
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
// connects live; on failure it still surfaces FAIL/WARN findings (CR-7).
func cmdDoctor(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		if errors.Is(err, errHelpRequested) {
			return printHelp()
		}
		return err
	}
	ctx := context.Background()
	// CR-7: the diagnostic must NEVER report healthy for an unconfigured or
	// unreachable cluster. It uses diagnoseConn (not openOrFake), so a missing
	// connstring is a reachability FAIL, not a masked healthy fake.
	srcQ := diagnoseConn(ctx, cfg, "source")
	dstQ := diagnoseConn(ctx, cfg, "dest")

	rep := doctor.Run(ctx, srcQ, dstQ, doctor.Options{MinTLS: cfg.SSLMinTLS, SegmentBinary: cfg.SegBinary})
	printReport(rep)
	if !rep.Healthy {
		return fmt.Errorf("doctor: %d FAIL finding(s)", rep.Failures())
	}
	return nil
}

// diagnoseConn opens a live connection for the diagnostic, or — on failure — a
// fake whose Ping reports the error so the doctor can still emit a FAIL/WARN
// (CR-7). Unlike openOrFake it NEVER returns a healthy fake for a missing
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
// reports the error so the doctor can still emit a FAIL/WARN (CR-7). When no
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
// object graph for the dry-run planner). Fault injection for CR-7 is done in
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
	fmt.Printf("redistribution: skew %.0f%% — %s (FR-15/CR-5)\n",
		p.Redistrib.Skew*100, p.Redistrib.Reason)
	fmt.Printf("route:         %s (T4b)\n", routeSummaryLine(p))
	// Fix 1: partitioned parents with a collation-unsafe combination (string
	// key + no DEFAULT partition + destination collation differs) cannot be
	// copied without losing rows. Surface them as explicit warnings so the
	// operator adds a DEFAULT partition (or aligns collations) BEFORE
	// running; the run itself fails these units early with the same text.
	guarded := 0
	for _, t := range p.Units {
		if t.PartitionGuard != "" {
			guarded++
			fmt.Printf("  ! %s\n", t.PartitionGuard)
		}
	}
	if guarded > 0 {
		fmt.Printf("partition guard: %d unit(s) would FAIL at run time (add a DEFAULT partition or align collations)\n", guarded)
	}
	if len(p.Collations) > 0 {
		fmt.Printf("collation:     %d difference(s) (FR-10/FR-16)\n", len(p.Collations))
		for _, cd := range p.Collations {
			switch {
			case cd.IsColumn():
				fmt.Printf("  - %s.%s: %q (source) vs %q (destination) (column, FR-16)\n",
					cd.Table, cd.Column, cd.SrcCollation, cd.DstCollation)
			case cd.IsEncoding():
				fmt.Printf("  - %s: encoding %s (DATA DESTRUCTIVE if you proceed — strings will be reinterpreted)\n",
					cd.DB, cd.Encoding)
			default:
				fmt.Printf("  - %s\n", cd.LCCollate)
			}
		}
	}
	// F-11 metadata phase: non-table objects (views, materialized views,
	// functions) in scope are reported — the data copy does not carry them by
	// itself, so the operator sees what is detected (F-11 "optional —
	// views/functions").
	if oo := p.OtherObjects; len(oo.Views) > 0 || len(oo.MatViews) > 0 || oo.Functions > 0 {
		fmt.Printf("other objects: %d view(s), %d materialized view(s), %d function(s) detected (F-11 metadata) — not carried by the data copy\n",
			len(oo.Views), len(oo.MatViews), oo.Functions)
		for _, v := range oo.Views {
			fmt.Printf("  - view %s\n", v)
		}
		for _, v := range oo.MatViews {
			fmt.Printf("  - matview %s\n", v)
		}
	}
	// F-11 metadata phase: dependent extensions (MADlib/PostGIS/orafce, ...)
	// used by the selected tables' columns but missing on the destination —
	// their column DDL will FAIL at run time until installed in the receiver.
	if len(p.MissingExtensions) > 0 {
		fmt.Printf("extensions:    %d source extension(s) used by copied columns are MISSING in the destination: %s\n",
			len(p.MissingExtensions), strings.Join(p.MissingExtensions, ", "))
		fmt.Printf("  ! install BEFORE running: CREATE EXTENSION <name> (F-11 metadata phase)\n")
	}
	// v2 §26: the dry-run must also surface locale, rate-limit/CPU/disk,
	// TLS/compression overhead, background index jobs, an aggregate warning
	// line, expected errors, and a confidence. These are predictions (the plan
	// is formed offline), so they are labelled as such where they cannot be
	// measured without a live copy.
	fmt.Printf("rate-limit:    %s (FR-14)\n", rateLimitNote(cfg))
	fmt.Printf("cpu/disk:      prediction (measured on a live copy; FR-13)\n")
	fmt.Printf("tls/compress:  %s (FR-13/FR-12)\n", overheadNote(cfg))
	fmt.Printf("background idx: %d unit(s) pending (FR-17)\n", backgroundIndexNote(cfg, p))
	warnings := len(p.Conflicts) + len(p.Collations)
	fmt.Printf("warnings:      %d (conflicts+collation)\n", warnings)
	if p.Partial {
		fmt.Printf("errors:        1 (timed out; partial fragment)\n")
	} else {
		fmt.Printf("errors:        0\n")
	}
	fmt.Printf("confidence:    %s\n", confidenceNote(p))
	if p.Hash != "" {
		fmt.Printf("plan hash:     %s (deterministic, CR-4)\n", p.Hash)
	} else if p.Partial {
		fmt.Printf("plan hash:     — (partial plan; no deterministic hash)\n")
	}
}

// routeSummaryLine renders the T4b route-mode summary for the dry-run report:
// the (S,D) segment counts, how many (s,d) pairs the layout admits, and the
// per-unit mode mix (direct_1to1 vs router_full). When the route is not
// resolvable (plain PostgreSQL) it degrades to a clear note instead of a number.
func routeSummaryLine(p *plan.Plan) string {
	if !p.Route.Resolved {
		return "not resolvable (plain-PostgreSQL or no segments) — coordinator path only"
	}
	direct, router := 0, 0
	for _, u := range p.Units {
		switch u.RouteMode {
		case "direct_1to1":
			direct++
		case "router_full":
			router++
		}
	}
	ver := "same-major"
	if !p.Route.GPVersionEqual {
		ver = "mixed-major"
	}
	d1 := "router_full (S!=D)"
	if p.Route.PossibleDirect1to1 {
		d1 = "direct_1to1 possible"
	}
	return fmt.Sprintf("src=%d dst=%d segs, %s, %d eligible pair(s), %s; units: %d direct_1to1 / %d router_full",
		p.Route.SrcSegments, p.Route.DstSegments, ver, p.Route.EligiblePairs, d1, direct, router)
}

// rateLimitNote renders the rate-limit expectation for the dry-run. The
// limiter is adaptive (FR-14): it lowers the per-pair rate on a detected
// receiver bottleneck and recovers when it stabilises.
func rateLimitNote(cfg config.Config) string {
	if cfg.RateLimit > 0 {
		return fmt.Sprintf("%g Gbps ceiling (adaptive per host-pair)", cfg.RateLimit)
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
	if cfg.SSLCert != "" {
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

// parseBytes parses a size like "512M", "1G", "1024" (suffixes K/M/G/T,
// case-insensitive, 1024-based). Used by --retransmit-buffer-max (§4.6).
func parseBytes(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	mult := int64(1)
	switch strings.ToLower(string(s[len(s)-1])) {
	case "k":
		mult = 1 << 10
		s = s[:len(s)-1]
	case "m":
		mult = 1 << 20
		s = s[:len(s)-1]
	case "g":
		mult = 1 << 30
		s = s[:len(s)-1]
	case "t":
		mult = 1 << 40
		s = s[:len(s)-1]
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n * mult
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

// humanBytesU is humanBytes for unsigned sizes (the runstats counters).
func humanBytesU(n uint64) string {
	return humanBytes(int64(n))
}

// orDash renders an empty string as "-" for the status output.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// humanDurationSec renders a second count as a compact human duration.
func humanDurationSec(sec int) string {
	if sec <= 0 {
		return "-"
	}
	d := time.Duration(sec) * time.Second
	return fmt.Sprintf("%dd %dh %dm", int(d.Hours())/24, int(d.Hours())%24, int(d.Minutes())%60)
}

// loadRunStats reads the operation's runstats.json (FR-04 live table), sitting
// next to the journal mirror. It returns nil (no error) when the file is
// absent — a finished operation with no final snapshot, or an operation that
// predates runstats.json.
func loadRunStats(journalPath string) (*monitor.RunStats, error) {
	dir := filepath.Dir(journalPath)
	b, err := os.ReadFile(filepath.Join(dir, "runstats.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var rs monitor.RunStats
	if err := json.Unmarshal(b, &rs); err != nil {
		return nil, err
	}
	return &rs, nil
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
func cmdStatus(args []string) error {
	opID, stateDir := parseOperationArgs(args)
	if opID == "" {
		return fmt.Errorf("status: --operation-id <id> is required")
	}
	journalPath := resolveJournalPath(stateDir, opID)
	j := manifest.NewJournal(nil, journalPath)
	if err := j.LoadFromMirror(); err != nil {
		return fmt.Errorf("status: load journal %s: %w", journalPath, err)
	}
	units := j.Units()
	fmt.Println("gpstream status")
	fmt.Println("================")
	fmt.Printf("operation-id: %s\n", opID)
	fmt.Printf("journal:      %s\n", journalPath)
	// T5 (DESIGN §2.2): the snapshot epoch metadata (gpstream_jobs / the
	// jobmeta.json mirror) — which generation of the source snapshot the job
	// is on, the transport, and the resume re-read mode.
	if meta, err := j.LoadMeta(opID); err == nil && meta != nil {
		cm := meta.CopyMode
		if cm == "" {
			cm = "coordinator"
		}
		fmt.Printf("snap-epoch:   %d (copy-mode=%s)\n", meta.SnapEpoch, cm)
	}
	if len(units) == 0 {
		fmt.Println("units:        none recorded")
		return nil
	}
	terminal := 0
	for _, u := range units {
		switch u.State {
		case manifest.StateCompleted, manifest.StateSwapped, manifest.StateFailed, manifest.StateSkipped:
			terminal++
		}
		fmt.Printf("  unit %4d  %-8s  %-10s  %s.%s.%s  %s  epoch=%d\n",
			u.UnitID, u.State, u.Kind, u.DB, u.Schema, u.Table, humanBytes(u.Bytes), u.Epoch)
	}
	// F-04: live table (throughput, ETA, active flows, per-table rows/bytes)
	// from the run's runstats.json, present while the copy is running (and on
	// the final snapshot after it stops).
	if rs, err := loadRunStats(journalPath); err == nil && rs != nil {
		fmt.Println("live:           (runstats.json)")
		fmt.Printf("  pair:         %s\n", orDash(rs.Pair))
		fmt.Printf("  progress:     %s / %s  (%d rows)\n",
			humanBytesU(rs.Decoded), humanBytesU(rs.Bytes), rs.Rows)
		fmt.Printf("  throughput:   %s/s  eta: %s  active-flows: %d\n",
			humanBytesU(uint64(rs.Throughput)), humanDurationSec(rs.ETASeconds), rs.ActiveFlows)
		if len(rs.ByStatus) > 0 {
			parts := make([]string, 0, len(rs.ByStatus))
			for _, st := range []string{"pending", "copying", "verifying", "completed", "swapped", "failed", "skipped"} {
				if n, ok := rs.ByStatus[st]; ok {
					parts = append(parts, fmt.Sprintf("%s=%d", st, n))
				}
			}
			fmt.Printf("  tables:       %s\n", strings.Join(parts, " "))
		}
		if len(rs.PerTable) > 0 {
			fmt.Println("  per-table:    rows        wire      logical  state")
			for _, r := range rs.PerTable {
				fmt.Printf("    %-24s %10d  %9s  %9s  %s\n",
					r.Table, r.Rows, humanBytesU(r.Wire), humanBytesU(r.Logical), orDash(r.State))
			}
		}
	}
	fmt.Printf("%d/%d units terminal\n", terminal, len(units))
	return nil
}

// cmdCleanup removes the operation's journal and staging state (v4 §8):
//
//	gpstream cleanup --operation-id <id> [--all-incomplete] [--mode drop] [--dest <connstring>]
//
// --all-incomplete removes every operation directory that has non-terminal
// units (no --operation-id needed); --mode drop ADDITIONALLY drops the
// destination-side objects the operation created (staging tables, renamed-in
// or gpstream-created final tables, renamed-in schemas — derived
// conservatively from the journal; see manifest.CleanupDestObjects) BEFORE
// the journal is deleted, restoring the destination to its pre-operation
// state (FR-02 acceptance: kill -9 ⇒ cleanup leaves the receiver as it was).
// --mode drop requires --dest (the destination connstring; --ssl-cert/key/ca
// are honoured). Destructive: requires --yes.
func cmdCleanup(args []string) error {
	opID, stateDir := parseOperationArgs(args)
	var (
		allIncomplete, drop, yes        bool
		destStr, sslCert, sslKey, sslCA string
		minTLS                          = "1.2"
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--all-incomplete":
			allIncomplete = true
		case "--mode":
			i++
			drop = args[i] == "drop"
		case "--yes":
			yes = true
		case "--dest":
			i++
			destStr = args[i]
		case "--ssl-cert":
			i++
			sslCert = args[i]
		case "--ssl-key":
			i++
			sslKey = args[i]
		case "--ssl-ca":
			i++
			sslCA = args[i]
		case "--ssl-min-tls":
			i++
			minTLS = args[i]
		}
	}
	if !yes {
		return fmt.Errorf("cleanup is destructive; pass --yes to proceed")
	}
	if !allIncomplete && opID == "" {
		return fmt.Errorf("cleanup: --operation-id <id> is required (or --all-incomplete)")
	}
	if drop && destStr == "" {
		return fmt.Errorf("cleanup --mode drop requires --dest <connstring> to reach the destination cluster")
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
		targets = append(targets, target{opID, resolveJournalPath(stateDir, opID)})
	}

	var dropAgent *dest.Agent
	var closeDest func()
	if drop {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		q, err := pg.NewPool(ctx, destStr, sslCert, sslKey, sslCA, minTLS, 1)
		cancel()
		if err != nil {
			return fmt.Errorf("cleanup --mode drop: connect dest: %w", err)
		}
		dropAgent = dest.New(q)
		closeDest = q.Close
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
		// F-13: audit the destructive cleanup BEFORE the state dir is
		// removed (the audit.log lives in it) — log first, then delete.
		audit := manifest.NewAuditLog(j.AuditPath())
		mode := "state"
		if drop {
			mode = "drop"
		}
		dropped, err := dropDestObjects(context.Background(), dropAgent, t.id, j, audit)
		if err != nil {
			audit.Close()
			if closeDest != nil {
				closeDest()
			}
			return fmt.Errorf("cleanup %s: drop dest objects: %w", t.id, err)
		}
		audit.Log("cleanup", "operation-id", t.id, "mode", mode,
			"units", len(j.Units()), "dropped", dropped)
		if err := os.RemoveAll(filepath.Dir(t.path)); err != nil {
			return fmt.Errorf("cleanup %s: %w", t.id, err)
		}
		audit.Close()
		if drop {
			fmt.Printf("cleanup: %s: dropped %d destination object(s)\n", t.id, dropped)
		}
		fmt.Printf("cleanup: removed operation state %s\n", t.id)
	}
	if closeDest != nil {
		closeDest()
	}
	if len(targets) == 0 {
		fmt.Println("cleanup: no incomplete operations found")
	}
	return nil
}

// dropDestObjects removes the journal-derived destination objects of one
// operation (FR-02) and records each removal in the audit log. Tables and
// staging are dropped first, schemas last (DropSchemaIfEmpty is a no-op while
// any object remains, so a failed table drop protects the schema).
func dropDestObjects(ctx context.Context, a *dest.Agent, opID string, j *manifest.Journal, audit *manifest.AuditLog) (int, error) {
	if a == nil {
		return 0, nil
	}
	targets := manifest.CleanupDestObjects(j)
	dropped := 0
	var errs []string
	for _, t := range targets {
		if t.Kind == "schema" {
			continue // schemas go last
		}
		var err error
		if t.Kind == "staging" {
			err = a.DropStaging(ctx, t.Schema, t.Name)
		} else {
			err = a.DropTable(ctx, t.Schema, t.Name)
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s %s.%s: %v", t.Kind, t.Schema, t.Name, err))
			audit.Log("cleanup_drop_failed", "operation-id", opID, "kind", t.Kind,
				"schema", t.Schema, "object", t.Name, "error", err.Error())
			continue
		}
		audit.Log("cleanup_drop", "operation-id", opID, "kind", t.Kind,
			"schema", t.Schema, "object", t.Name, "reason", t.Reason)
		fmt.Printf("cleanup: dropped %s %s.%s\n", t.Kind, t.Schema, t.Name)
		dropped++
	}
	for _, t := range targets {
		if t.Kind != "schema" {
			continue
		}
		if err := a.DropSchemaIfEmpty(ctx, t.Schema); err != nil {
			errs = append(errs, fmt.Sprintf("schema %s: %v", t.Schema, err))
			audit.Log("cleanup_drop_failed", "operation-id", opID, "kind", "schema",
				"schema", t.Schema, "error", err.Error())
			continue
		}
		audit.Log("cleanup_drop", "operation-id", opID, "kind", "schema",
			"schema", t.Schema, "reason", t.Reason)
		dropped++
	}
	if len(errs) > 0 {
		return dropped, fmt.Errorf("%d object(s) not dropped: %s", len(errs), strings.Join(errs, "; "))
	}
	return dropped, nil
}

// parseOperationArgs extracts --operation-id and --state-dir.
func parseOperationArgs(args []string) (opID, stateDir string) {
	stateDir = config.Default().StateDir
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--operation-id":
			i++
			opID = args[i]
		case "--state-dir":
			i++
			stateDir = args[i]
		}
	}
	return opID, stateDir
}

// resolveJournalPath returns the durable journal path:
// <state-dir>/<operation-id>/journal.jsonl (v4 §8 "State").
func resolveJournalPath(stateDir, opID string) string {
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
			case manifest.StateCompleted, manifest.StateSwapped, manifest.StateFailed, manifest.StateSkipped:
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

// cmdRun executes a copy between two Greenplum 6 clusters (v4 §8):
//
//	gpstream run --source ... --dest ... --dbs sales,hr \
//	             --overwrite \
//	             --compress zstd --workers 32 --rate-limit 60 \
//	             --auto-resume 3 --index-workers 8 --ssl-ca /etc/gpstream/ca.pem
//
// It plans (bounded by --timeout), streams every unit source -> dest with no
// intermediate storage (F-01), verifies (F-02), rebuilds indexes (F-17) and
// journals every transition under --state-dir/--operation-id. A real copy
// needs real streaming connections; a fake has no data plane, so run never
// falls back to one.
func cmdRun(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		if errors.Is(err, errHelpRequested) {
			return printHelp()
		}
		return err
	}

	ctx := context.Background()
	// The data path runs WorkersPerPair parallel stream workers (FR-03); a
	// single pgx.Conn is not safe for concurrent statements (a second
	// in-flight COPY → "conn busy"). Use a pgxpool-backed Pool for both
	// clusters so each worker/journal operation takes its own connection.
	// Size the pool for the worker pool plus the journal and the (optional)
	// FR-02 snapshot pin.
	maxConns := cfg.WorkersPerPair + 4
	srcQ, err := pg.NewPool(ctx, cfg.Source, cfg.SSLCert, cfg.SSLKey, cfg.SSLCA, cfg.SSLMinTLS, maxConns)
	if err != nil {
		return fmt.Errorf("run: connect source %q: %w", cfg.Source, err)
	}
	defer srcQ.Close()
	dstQ, err := pg.NewPool(ctx, cfg.Dest, cfg.SSLCert, cfg.SSLKey, cfg.SSLCA, cfg.SSLMinTLS, maxConns)
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

	// Operation identity (v4 §8 "State"): the journal ALWAYS lives at
	// <state-dir>/<operation-id>/journal.jsonl so status/resume/cleanup
	// (which take only --operation-id/--state-dir) find it. --plan-file is a
	// separate concern: it persists the plan JSON (same as dry-run), not the
	// journal.
	if cfg.OperationID == "" {
		cfg.OperationID = p.Hash
	}
	journalPath := filepath.Join(cfg.StateDir, cfg.OperationID, "journal.jsonl")
	if err := os.MkdirAll(filepath.Dir(journalPath), 0o755); err != nil {
		return fmt.Errorf("run: state dir %s: %w", filepath.Dir(journalPath), err)
	}
	if cfg.PlanFile != "" {
		data, _ := json.MarshalIndent(p, "", "  ")
		if err := os.WriteFile(cfg.PlanFile, data, 0o644); err != nil {
			return fmt.Errorf("run: write plan-file: %w", err)
		}
		fmt.Printf("plan written:   %s\n", cfg.PlanFile)
	}
	fmt.Printf("operation-id:   %s\n", cfg.OperationID)
	fmt.Printf("state:          %s\n", journalPath)

	j := manifest.NewJournal(dstQ, journalPath)

	// F-10 §8: persist the conflict diagnosis to conflicts.jsonl (alongside
	// the journal) so the operator can audit what would collide — and with
	// which action — without parsing stdout. Written before the copy starts;
	// the in-memory plan still carries the list for the run itself.
	if err := j.WriteConflicts(p.Conflicts); err != nil {
		fmt.Printf("warn: write conflicts.jsonl: %v\n", err)
	} else if cp := j.ConflictsPath(); cp != "" {
		fmt.Printf("conflicts log:  %s (%d)\n", cp, len(p.Conflicts))
	}

	// F-13: audit log of actions — record the run and its policy in
	// <state>/audit.log (0600, alongside the journal).
	audit := manifest.NewAuditLog(j.AuditPath())
	defer audit.Close()
	copyMode := string(cfg.CopyMode)
	if copyMode == "" {
		copyMode = string(config.CopyCoordinator) // the implicit default
	}
	onFailure := cfg.OnUnitFailure
	if onFailure == "" {
		onFailure = "continue" // the implicit default (spec F-06)
	}
	audit.Log("run", "operation-id", cfg.OperationID, "units", len(p.Units),
		"copy-mode", copyMode, "on-unit-failure", onFailure,
		"compression", string(cfg.Compression), "consistent-db", cfg.ConsistentDB,
		"consistent-cluster", cfg.ConsistentCluster,
		"consistent-tables", cfg.ConsistentTables)

	mgr := manifest.NewManager(&cfg, srcQ, dstQ, j)

	// F-04: expose the metrics over HTTP so throughput, error rate and the
	// limiter/compression state are observable during the copy.
	stopMetrics := startMetricsServer(ctx, cfg.MetricsAddr, mgr.Metrics())
	if stopMetrics != nil {
		defer stopMetrics()
	}

	runErr := mgr.Run(ctx, p)

	// --auto-resume N (F-07): if any unit did not reach a terminal state,
	// resume up to N times so the job can complete.
	if cfg.AutoResume > 0 && runErr == nil {
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
	// F-13: audit the run's outcome (stats + error, if any).
	audit.Log("run-finished", "operation-id", cfg.OperationID, "units", mgr.Units,
		"rows", mgr.Rows, "skipped", mgr.Skipped, "failed", mgr.Failures,
		"error", runErr)
	if runErr != nil {
		return runErr
	}

	fmt.Printf("\nrun: %d unit(s) completed, %d rows transferred, %d skipped, %d failed\n",
		mgr.Units, mgr.Rows, mgr.Skipped, mgr.Failures)
	// F-16: the detected collation differences (DB-level LC_COLLATE/LC_CTYPE
	// and column-level attcollation) are re-surfaced in the FINAL report —
	// the spec requires the warning in doctor/dry-run AND the final report.
	// An admin reviews and accepts the risk (string sort/index semantics may
	// differ) rather than gpstream silently deciding.
	if len(p.Collations) > 0 {
		fmt.Printf("collation:    %d difference(s) detected (FR-10/FR-16) — review before relying on string sort/index semantics:\n", len(p.Collations))
		for _, cd := range p.Collations {
			switch {
			case cd.IsColumn():
				fmt.Printf("  - %s.%s: %q (source) vs %q (destination) [column]\n",
					cd.Table, cd.Column, cd.SrcCollation, cd.DstCollation)
			case cd.IsEncoding():
				fmt.Printf("  - db %s: encoding %s [DATA DESTRUCTIVE]\n", cd.DB, cd.Encoding)
			default:
				fmt.Printf("  - db %s: %s (LC_COLLATE), LC_CTYPE %s\n", cd.DB, cd.LCCollate, cd.LCType)
			}
		}
	}
	// F-15: the live segment-fill balance of the copied tables (max/avg over
	// the destination's gp_segment_id) with the warnings for any table that
	// exceeded the F-15 segment-fill gate (fixed 1.5). Empty on a non-Greenplum
	// destination (no segments to measure).
	if bal := dest.RenderBalance(mgr.Balances); bal != "" {
		fmt.Println("segment balance (F-15, live, per destination segment):")
		fmt.Println(bal)
	}
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
	opID, stateDir := parseOperationArgs(args)
	maxResume := 0
	for i := 0; i < len(args); i++ {
		if args[i] == "--auto-resume" && i+1 < len(args) {
			i++
			maxResume = atoi(args[i])
		}
	}
	if opID == "" {
		return fmt.Errorf("resume: --operation-id <id> is required")
	}
	journalPath := resolveJournalPath(stateDir, opID)
	// Durable resume: rebuild the in-memory journal from the file mirror
	// (which survived the crash) before computing which units to re-queue.
	j := manifest.NewJournal(nil, journalPath)
	if err := j.LoadFromMirror(); err != nil {
		return fmt.Errorf("resume: load journal %s: %w", journalPath, err)
	}
	// F-13: audit the resume action (the operator restarted an operation).
	audit := manifest.NewAuditLog(j.AuditPath())
	defer audit.Close()
	rm := resume.NewManager(j, maxResume)
	n, err := rm.Resume(context.Background())
	if err != nil {
		audit.Log("resume", "operation-id", opID, "error", err.Error())
		return fmt.Errorf("resume: %w", err)
	}
	if n == 0 {
		audit.Log("resume", "operation-id", opID, "requeued", 0)
		fmt.Println("resume: all units terminal — nothing to resume")
		return nil
	}
	audit.Log("resume", "operation-id", opID, "requeued", n, "attempt", rm.Attempts())
	fmt.Printf("resume: operation %s: re-queued %d unit(s) (attempt %d, max %d)\n", opID, n, rm.Attempts(), maxResume)
	return nil
}

func printHelp() error {
	fmt.Println(`gpstream — high-throughput logical data copy between Greenplum 6 clusters

Usage:
  gpstream <command> [flags]

Commands:
  doctor    self-diagnose source/dest clusters
  dry-run   plan the copy without executing
  run       execute the copy (streams source -> dest, no intermediate storage)
  bench     measure the input stream: stream the selected source tables
            through the real data path (read -> frame -> compress) and
            discard the payload (no destination; per-table + total rates)
  status    report operation state (--operation-id <id>)
  resume    resume an interrupted operation (--operation-id <id>)
  cleanup   remove operation state + dest objects (--operation-id <id>
            [--all-incomplete] [--mode drop --dest <conn>] [--yes])

Service:
  selftest  internal sanity checks
  version   print the build version
  help      this help (also: --help / -h)

Flags (CLI > env > file > defaults):

Selection:
  --source, --dest connstr           source/dest cluster connstrings
  --dbs csv                          databases to copy
  --schemas csv                      schemas to copy (db.schema)
  --tables csv                       tables to copy (db.schema.table)
  --exclude csv                      exclude a database/schema/table
  --map-db src=dst                   rename a database at the dest
  --map-schema src=dst               rename a schema at the dest
  --map-table src=dst                rename a table at the dest
  --metadata-only                    structure only (DDL + sequences, no data)
  --data-only                        data only, into the existing structure

Conflicts:
  --resolve-conflicts stop|skip|overwrite|truncate   policy (default stop)
  --overwrite, --truncate          shorthand opt-ins that set the policy

Performance:
  --compress none|zlib|zstd[:1-19]  stream compression (default zstd level 3)
  --zstd-level N                   zstd level 1-19 (default 3)
  --rate-limit rate                total bandwidth ceiling (bps/mbit/gbit)
  --workers N                      parallel workers per host-pair (default 2)
  --small-table-threshold size     tables smaller than this copy with compression
                                   off (default 1MiB; 0 disables)
  --index-workers N                background index-build workers (default 2)
  --index-priority size|fifo       index build order (default size: largest first)
  --no-indexes                     skip index creation entirely
  --no-constraints                 skip the FK/CHECK constraint-restore pass

Transport / resilience:
  --copy-mode coordinator|on_segment|direct   data path (default coordinator)
  --seg-listen HOST:PORT           controller listen addr for the segreceivers
  --seg-binary PATH                gpstream-segreceiver path on the SOURCE segment hosts (package or copied file; required with --copy-mode on_segment)
  --seg-tls-cert F                 TLS serving cert for the segment-direct hop
  --seg-tls-key F                  TLS serving key for --seg-tls-cert
  --seg-tls-ca F                   trusted CA; also enables mTLS for the hop
  --frame-ack on|off               frame-level ACK + RST-resume (default on)
  --ack-timeout 15s|30s|adaptive   ACK timeout (default 30s)
  --max-reconnects N               reconnect budget (default 20)
  --retransmit-buffer-max size     in-flight window per stream (default 64MiB)

Integrity / consistency:
  --verify full|sample|off         verification depth (default sample)
  --consistent-db                  snapshot scope: whole DB (default)
  --consistent-cluster             snapshot scope: whole cluster
  --consistent-tables              snapshot scope: per-table
  --auto-resume N                  auto-resume attempts after the run (default 0)
  --on-unit-failure continue|abort on a failed unit (default continue)

Planning / operation:
  --dry-run                        plan the copy without executing (alias of
                                   the dry-run command)
  --timeout S                      run/dry-run: bound plan formation (default
                                   120s); bench: the benchmark wall-time limit
                                   (default unlimited)
  --plan-file F                    persist the plan JSON (dry-run and run)
  --operation-id id                operation identity (run/status/resume/cleanup)
  --state-dir dir                  state root (default /var/lib/gpstream)
  --metrics-addr addr              serve Prometheus /metrics during run
  --config file                    config file (TOML or YAML)

TLS:
  --ssl-cert, --ssl-key            mTLS client cert/key
  --ssl-ca F                       TLS CA bundle
  --ssl-min-tls V                  minimum TLS version (default 1.2)

Config file: /etc/gpstream/gpstream.toml; state: /var/lib/gpstream/<operation-id>/.
See docs/ for the full guide.`)
	return nil
}
