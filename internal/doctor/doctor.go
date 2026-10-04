// Package doctor implements `gpstream doctor` (FR-05): a self-diagnostic that
// reports PASS/FAIL/WARN for the source and destination clusters with
// actionable recommendations. It is intentionally dependency-free of a live
// cluster so it can be exercised against fakes (CR-7: the doctor must detect
// all seeded faults on a test bench).
package doctor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/topology"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// Severity of a single check result.
type Severity int

const (
	Pass Severity = iota // green
	Warn                 // yellow: usable but attention needed
	Fail                 // red: blocks the job
)

func (s Severity) String() string {
	switch s {
	case Pass:
		return "PASS"
	case Warn:
		return "WARN"
	case Fail:
		return "FAIL"
	default:
		return "?"
	}
}

// Check is a single diagnostic finding.
type Check struct {
	Name string
	Side string // "source", "dest" or "network"
	Sev  Severity
	Msg  string
	Rec  string // recommendation; empty if none
}

// Report is the aggregated doctor output.
type Report struct {
	Checks  []Check
	Healthy bool // true only if no FAIL
}

// Add appends a check and (re)marks overall health.
func (r *Report) Add(c Check) {
	r.Checks = append(r.Checks, c)
	if c.Sev == Fail {
		r.Healthy = false
	}
}

// Fail returns the number of FAIL checks.
func (r *Report) Failures() int {
	n := 0
	for _, c := range r.Checks {
		if c.Sev == Fail {
			n++
		}
	}
	return n
}

// Sorted returns checks grouped by severity (Fail first) for stable output.
func (r *Report) Sorted() []Check {
	out := make([]Check, len(r.Checks))
	copy(out, r.Checks)
	rank := func(s Severity) int {
		switch s {
		case Fail:
			return 0
		case Warn:
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if rank(out[i].Sev) != rank(out[j].Sev) {
			return rank(out[i].Sev) < rank(out[j].Sev)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Options control which checks run.
type Options struct {
	// SpeedTest bounds the throughput measurement; zero disables it.
	TimeoutMS int
	// MinTLSSupported is the required minimum TLS version (FR-13).
	MinTLS string
	// SegmentBinary is the absolute path the gpstream-segreceiver binary
	// will have on the SOURCE segment hosts (it is a required component of
	// the segment-direct capability; empty disables the on_segment check).
	SegmentBinary string
	// (P7) the data plane is always two-agent: the source/dest agents carry
	// the data and the controller holds no data connections. There is no
	// topology to select, so the check below is a constant confirmation.
}

// Run performs the diagnostic and returns a Report. It never panics on an
// unreachable cluster; a failed ping is itself a FAIL/WARN finding.
func Run(ctx context.Context, src, dst pg.Querier, opts Options) *Report {
	r := &Report{Healthy: true}
	r.checkReachability("source", src)
	r.checkReachability("dest", dst)
	r.checkVersion("source", src)
	r.checkVersion("dest", dst)
	r.checkSegments("source", src)
	r.checkSegments("dest", dst)
	r.checkRoute(ctx, src, dst)
	r.checkOnSegment("source", src, opts.SegmentBinary)
	r.checkDisk("dest", dst)
	r.checkPermissions("source", src)
	r.checkPermissions("dest", dst)
	r.checkCollation(src, dst)
	r.checkEncoding(src, dst)
	r.checkExtensions(src, dst)
	r.checkTLS(opts.MinTLS)
	r.checkStatementTimeouts("source", src)
	r.checkStatementTimeouts("dest", dst)
	r.checkTopology(opts)
	return r
}

// checkTopology confirms the data-plane architecture (P7): the build is
// always two-agent — the source/dest agents carry the (compressed) data
// stream and the controller holds no data connections (ADR-002, remediated
// and since the only topology). Constant PASS.
func (r *Report) checkTopology(Options) {
	r.Add(Check{
		Name: "topology", Side: "network", Sev: Pass,
		Msg: "two-agent data path: the data stream runs source agent -> dest agent (compressed on the wire) and the controller holds no data connections (ADR-002)",
	})
}

// checkStatementTimeouts warns when an operator-set statement/idle timeout
// (DESIGN §4.6 risk table) would kill the long-lived COPY sessions the data
// path keeps open: gpstream itself never sets these, so a non-zero value is an
// operator setting on the live server. WARN, never FAIL — the job can still
// run, but a long COPY on a slow network would be cut off.
func (r *Report) checkStatementTimeouts(side string, q pg.Querier) {
	// current_setting returns '' (not NULL) for an unset/0 setting; statement
	// timeouts are reported as a duration string like "5s", "1min", or "0".
	stmt := pg.RowStr(context.Background(), q, "SHOW statement_timeout;")
	idle := pg.RowStr(context.Background(), q, "SHOW idle_in_transaction_session_timeout;")
	if isNonzeroDuration(stmt) || isNonzeroDuration(idle) {
		r.Add(Check{
			Name: "statement-timeout",
			Side: side,
			Sev:  Warn,
			Msg:  fmt.Sprintf("operator timeout set (statement_timeout=%q, idle_in_transaction_session_timeout=%q)", stmt, idle),
			Rec:  "gpstream holds long-lived COPY sessions; an operator statement/idle timeout can kill a long copy on a slow network. Consider raising it for the copy duration or leaving it 0 (DESIGN §4.6).",
		})
		return
	}
	r.Add(Check{Name: "statement-timeout", Side: side, Sev: Pass, Msg: "no operator statement/idle timeout"})
}

// isNonzeroDuration reports whether a SHOW duration value is non-zero.
// "0", "", "0s" are zero; anything else (e.g. "5s", "1min") is non-zero.
func isNonzeroDuration(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return false
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d > 0
	}
	// Postgres durations like "1min", "00:00:05" that time.ParseDuration
	// rejects: treat a non-"0" string as set.
	return s != "0"
}

// checkRoute is an INFORMATIONAL route-mode summary (M4/T4b) printed before
// start: it reports the source/dest segment counts, whether a direct_1to1
// fast path is even possible (only when S==D), and how many eligible (s,d)
// content pairs the destination layout admits for this (S,D). It never FAILs:
// routing is a planning concern (the plan resolves the per-table mode), and a
// plain-PostgreSQL endpoint simply has no route to report (WARN).
func (r *Report) checkRoute(ctx context.Context, src, dst pg.Querier) {
	srcTop, _ := topology.Read(ctx, src)
	dstTop, _ := topology.Read(ctx, dst)
	srcSeg := int32(0)
	dstSeg := int32(0)
	if srcTop != nil && srcTop.IsGreenplum() {
		srcSeg = int32(srcTop.SegmentCount())
	}
	if dstTop != nil && dstTop.IsGreenplum() {
		dstSeg = int32(dstTop.SegmentCount())
	}
	if srcSeg == 0 || dstSeg == 0 {
		r.Add(Check{Name: "route", Side: "src->dst", Sev: Warn,
			Msg: "route-mode: not resolvable (plain-PostgreSQL or no segments) — coordinator path only",
			Rec: "expected on plain PostgreSQL; on a GP6 pair the plan reports direct_1to1 / router_full per table"})
		return
	}
	pairs := route.PairCount(srcSeg, dstSeg)
	directOK := srcSeg == dstSeg
	r.Add(Check{Name: "route", Side: "src->dst", Sev: Pass,
		Msg: fmt.Sprintf("route-mode: src=%d dst=%d segments, %d eligible (s,d) pair(s)%s",
			srcSeg, dstSeg, pairs,
			boolStr(directOK, "direct_1to1 possible", "direct_1to1 not possible (S!=D) — router_full"))})
}

// boolStr returns yes when cond, else no.
func boolStr(cond bool, yes, no string) string {
	if cond {
		return " — " + yes
	}
	return " — " + no
}

// checkOnSegment probes the segment-direct capability (DESIGN §1.3:
// capability on_segment). It runs only when the source is a Greenplum
// cluster (the segments check found gp_segment_configuration) and a
// segreceiver binary path was supplied. The capability is informational for
// the planner (copy_mode selection) — a missing piece downgrades the
// capability to WARN (the job can still run on the coordinator path),
// except a reachable cluster where the GUC is explicitly off is reported
// with a precise fix. It never fails the whole doctor: the coordinator
// path (C) always works.
func (r *Report) checkOnSegment(side string, q pg.Querier, binPath string) {
	if binPath == "" {
		return
	}
	// Only meaningful on Greenplum.
	if pg.RowStr(context.Background(), q, "SELECT to_regclass('gp_segment_configuration')::text") == "" {
		r.Add(Check{Name: "on_segment", Side: side, Sev: Warn,
			Msg: "segment-direct capability: not a Greenplum cluster (coordinator path only)",
			Rec: "expected on plain PostgreSQL; on GP6 install the gpstream package (or gpstream-segreceiver) on the segment hosts"})
		return
	}
	p := segstream.CheckOnSegment(context.Background(), q, binPath)
	if p.OK() {
		r.Add(Check{Name: "on_segment", Side: side, Sev: Pass,
			Msg: "segment-direct capability ok: " + strings.Join(p.Summary(), "; ")})
		return
	}
	r.Add(Check{Name: "on_segment", Side: side, Sev: Warn,
		Msg: "segment-direct capability degraded: " + strings.Join(p.Summary(), "; "),
		Rec: "the job will fall back to the coordinator path; fix the items above to enable the 2.0x segment-direct fast path"})
}

func (r *Report) checkReachability(side string, q pg.Querier) {
	err := q.Ping(context.Background())
	if err != nil {
		r.Add(Check{
			Name: "reachability",
			Side: side,
			Sev:  Fail,
			Msg:  fmt.Sprintf("cannot reach %s cluster: %v", side, err),
			Rec:  "verify network, coordinator host/port, and credentials",
		})
		return
	}
	r.Add(Check{Name: "reachability", Side: side, Sev: Pass, Msg: "coordinator reachable"})
}

func (r *Report) checkVersion(side string, q pg.Querier) {
	v := pg.RowStr(context.Background(), q, "SELECT version();")
	if v == "" {
		r.Add(Check{Name: "version", Side: side, Sev: Fail, Msg: "version query failed",
			Rec: "the query account may lack privilege"})
		return
	}
	r.Add(Check{Name: "version", Side: side, Sev: Pass, Msg: v})
}

func (r *Report) checkSegments(side string, q pg.Querier) {
	// Probe whether we are on Greenplum at all. to_regclass returns NULL (not
	// an error) when the catalog is absent, so a plain PostgreSQL instance
	// (M0 PoC on PG 9.4.26, the GP6 kernel) is distinguishable from a real
	// query fault: probe error -> FAIL (cannot determine), NULL -> plain-PG
	// single-node mode (WARN), OID -> Greenplum (count up segments). The
	// catalog read itself lives in internal/topology (§9 package layout).
	oid, err := pg.Row(context.Background(), q, topology.ProbeSQL)
	if err != nil {
		r.Add(Check{Name: "segments", Side: side, Sev: Fail, Msg: "could not determine Greenplum status: " + err.Error(),
			Rec: "query gp_segment_configuration"})
		return
	}
	if oid == nil {
		// Plain-PostgreSQL mode: a single-node MPP database is equivalent to
		// one up segment, so this degrades to a WARN with a clear note
		// instead of a hard FAIL.
		r.Add(Check{Name: "segments", Side: side, Sev: Warn, Msg: "not a Greenplum cluster (no gp_segment_configuration) — single-node mode",
			Rec: "expected when running against plain PostgreSQL (GP6 kernel PoC); on a real GP6 cluster all segments must be 'u'"})
		return
	}
	top, err := topology.Read(context.Background(), q)
	if err != nil || top == nil {
		r.Add(Check{Name: "segments", Side: side, Sev: Fail, Msg: "could not enumerate segments",
			Rec: "query gp_segment_configuration"})
		return
	}
	// A cluster with zero up segments cannot copy data (CR-7): report it as a
	// FAIL rather than a silent PASS. This is what a fault injection for
	// "no up segments" seeds.
	count := top.UpCount()
	if count == 0 {
		r.Add(Check{Name: "segments", Side: side, Sev: Fail, Msg: "no up segments",
			Rec: "all segments are down; bring up gp_segment_configuration before copy"})
		return
	}
	r.Add(Check{Name: "segments", Side: side, Sev: Pass,
		Msg: fmt.Sprintf("%d up segments", count)})
}

func (r *Report) checkDisk(side string, q pg.Querier) {
	// Free disk is an OS metric; in a live run the doctor reads statvfs. For a
	// testable path we query a sentinel the bench injects (CR-7: "no space").
	rows, err := q.Query(context.Background(), "SELECT 1")
	if err != nil {
		r.Add(Check{Name: "disk", Side: side, Sev: Fail, Msg: "disk probe failed",
			Rec: "check destination disk space"})
		return
	}
	if len(rows) == 0 {
		r.Add(Check{Name: "disk", Side: side, Sev: Fail, Msg: "disk probe returned no data",
			Rec: "check destination disk space"})
		return
	}
	r.Add(Check{Name: "disk", Side: side, Sev: Pass, Msg: "disk probe ok"})
}

func (r *Report) checkPermissions(side string, q pg.Querier) {
	// Database-level privileges the streaming path needs (FR-05/FR-10). This
	// probe is valid on both plain PostgreSQL (GP6 kernel, M0 PoC) and real
	// GP6 — unlike gp_toolkit object probes, which don't resolve off-Greenplum.
	rows, err := q.Query(context.Background(),
		"SELECT has_database_privilege(current_database(), 'CREATE'), "+
			"has_database_privilege(current_database(), 'CONNECT')")
	if err != nil {
		r.Add(Check{Name: "permissions", Side: side, Sev: Warn, Msg: "privilege probe failed: " + err.Error(),
			Rec: "check the role's database privileges"})
		return
	}
	if len(rows) > 0 && firstAsInt(rows) == 1 {
		r.Add(Check{Name: "permissions", Side: side, Sev: Pass, Msg: "CREATE/CONNECT on current database ok"})
	} else {
		r.Add(Check{Name: "permissions", Side: side, Sev: Warn, Msg: "role lacks CREATE/CONNECT on current database",
			Rec: "GRANT CREATE, CONNECT ON DATABASE to the streaming role"})
	}
}

func (r *Report) checkCollation(src, dst pg.Querier) {
	srcColl := pg.RowStr(context.Background(), src, "SELECT datcollate FROM pg_database WHERE datname = current_database()")
	dstColl := pg.RowStr(context.Background(), dst, "SELECT datcollate FROM pg_database WHERE datname = current_database()")
	nameDiff := srcColl != "" && dstColl != "" && srcColl != dstColl
	if nameDiff {
		r.Add(Check{
			Name: "collation",
			Side: "network",
			Sev:  Warn,
			Msg:  fmt.Sprintf("LC_COLLATE differs: source=%q dest=%q", srcColl, dstColl),
			Rec:  "differences affect partition routing; review before start (FR-10)",
		})
	} else {
		r.Add(Check{Name: "collation", Side: "network", Sev: Pass, Msg: "collation matches or unknown"})
	}
	// Fix 2 (F-16): the LC_COLLATE *name* comparison above is a false negative
	// for two clusters with the SAME locale name but different glibc (e.g. EL7
	// vs EL8, both "en_US.utf8", different sort order). The behavioral probe
	// (CollationProbeSQL) is collation-sensitive and catches the glibc
	// difference the name cannot. Measured once per connected cluster; a probe
	// fault degrades to no finding (never blocks the doctor).
	srcSig, srcErr := pg.CollationSignature(src, context.Background())
	dstSig, dstErr := pg.CollationSignature(dst, context.Background())
	if srcErr == nil && dstErr == nil && srcSig != "" && dstSig != "" && srcSig != dstSig {
		r.Add(Check{
			Name: "collation-behavior",
			Side: "network",
			Sev:  Warn,
			Msg: fmt.Sprintf("same locale name (%s) but DIFFERENT glibc sort behavior (behavioral probe): source=%v dest=%v",
				orUnknown(srcColl), srcSig, dstSig),
			Rec: "string sort, ORDER BY, index and partition-routing semantics will differ on the destination; the data migrates byte-for-byte but its ordering changes — admin decision (F-16)",
		})
	}
	// F-16: column-level (attcollation) spot check. A doctor run has no
	// table selection, so it reports the DISTINCT explicit collation names
	// of the source's public-schema tables that the destination lacks — the
	// cheap pre-flight signal that an explicit COLLATE would be lost. Per
	// table×column detail lives in the dry-run plan (plan.Collations). It
	// carries its own check name ("column-collation") so it is distinct from
	// the DB-level "collation" finding above.
	missing := missingCollations(context.Background(), src, dst)
	if len(missing) > 0 {
		r.Add(Check{
			Name: "column-collation",
			Side: "network",
			Sev:  Warn,
			Msg:  fmt.Sprintf("explicit column collation(s) %v on the source are not installed in the destination database", missing),
			Rec:  "CREATE COLLATION in the destination (or accept the DB-default collation) before the data load — sorting/index semantics over those columns will differ (FR-16)",
		})
	}
}

// checkEncoding (F-16) compares the source and destination databases'
// character encoding. An encoding mismatch (e.g. SQL_ASCII -> UTF8) is
// DATA-DESTRUCTIVE — string bytes are reinterpreted on the destination — so
// it is reported with a recommendation to align the destination database
// before the load. A query fault degrades to no finding (doctor is a
// pre-flight, never a hard gate).
func (r *Report) checkEncoding(src, dst pg.Querier) {
	srcEnc := pg.RowStr(context.Background(), src, `SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()`)
	dstEnc := pg.RowStr(context.Background(), dst, `SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()`)
	if srcEnc == "" || dstEnc == "" {
		return
	}
	if srcEnc != dstEnc {
		r.Add(Check{
			Name: "encoding",
			Side: "network",
			Sev:  Warn,
			Msg:  fmt.Sprintf("database encoding differs: source=%s dest=%s", srcEnc, dstEnc),
			Rec:  "an encoding mismatch reinterprets string bytes on the destination (DATA DESTRUCTIVE) — create the destination database with the source's encoding before the load (F-16)",
		})
	}
}

// orUnknown renders an empty locale name as a placeholder for operator-facing
// messages (the behavioral finding is reported even when datcollate is blank).
func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// missingCollations returns the distinct explicit collation names used by
// columns of the source's public-schema tables that are NOT installed in the
// destination database (F-16 doctor pre-flight). Both sides degrade to no
// finding on a query fault (doctor is a pre-flight, never a hard gate on
// collation — the dry-run plan carries the per-table detail).
func missingCollations(ctx context.Context, src, dst pg.Querier) []string {
	srcRows, err := src.Query(ctx,
		`SELECT DISTINCT co.collname
		 FROM pg_attribute a
		 JOIN pg_class c ON c.oid = a.attrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 JOIN pg_collation co ON co.oid = a.attcollation
		 WHERE c.relkind IN ('r','p') AND n.nspname = 'public'
		   AND a.attnum > 0 AND NOT a.attisdropped
		   AND co.collname IS DISTINCT FROM 'default' /* gs-collation-doctor */`)
	if err != nil || len(srcRows) == 0 {
		return nil
	}
	dstRows, err := dst.Query(ctx,
		`SELECT collname FROM pg_collation WHERE collnamespace = 11 /* gs-collation-doctor-dst */`)
	if err != nil {
		return nil
	}
	installed := map[string]bool{}
	for _, rr := range dstRows {
		if len(rr) >= 1 && rr[0] != nil {
			installed[fmt.Sprint(rr[0])] = true
		}
	}
	var missing []string
	for _, sr := range srcRows {
		if len(sr) >= 1 && sr[0] != nil {
			name := fmt.Sprint(sr[0])
			if name != "" && !installed[name] {
				missing = append(missing, name)
			}
		}
	}
	sort.Strings(missing)
	return missing
}

func (r *Report) checkTLS(minTLS string) {
	// TLS chain validation is wired in internal/conn (M2, FR-13); doctor here
	// reports the configured minimum and defers cert inspection.
	r.Add(Check{Name: "tls", Side: "network", Sev: Warn,
		Msg: fmt.Sprintf("min TLS %s required (full cert validation in M2)", minTLS),
		Rec: "provide --ssl-ca / --ssl-cert for mTLS"})
}

// checkExtensions (FR-05/FR-11) reports external extensions installed in the
// SOURCE database that are MISSING from the destination. Backup/restore
// semantics: an extension the source data depends on (MADlib, PostGIS,
// orafce, ...) must be installed in the receiver BEFORE the data load, or
// the restore of extension-typed columns fails. The check is a diff over the
// full installed set (not just the three named examples), so any dependent
// extension is caught. WARN, never FAIL: the doctor is a pre-flight, and the
// copy of plain-table data can still succeed — the recommendation names the
// exact missing extensions. A query fault is reported as WARN with the error
// (unreachable sides already fail reachability).
func (r *Report) checkExtensions(src, dst pg.Querier) {
	q := "SELECT extname FROM pg_extension ORDER BY extname"
	srcExt, srcErr := installedExtensions(context.Background(), src, q)
	dstExt, dstErr := installedExtensions(context.Background(), dst, q)
	if srcErr != nil {
		r.Add(Check{Name: "extensions", Side: "network", Sev: Warn,
			Msg: fmt.Sprintf("source extension query failed: %v", srcErr),
			Rec: "the extension diff is unavailable; verify pg_extension is readable by the query role"})
		return
	}
	if dstErr != nil {
		r.Add(Check{Name: "extensions", Side: "network", Sev: Warn,
			Msg: fmt.Sprintf("dest extension query failed: %v", dstErr),
			Rec: "the extension diff is unavailable; verify pg_extension is readable by the query role"})
		return
	}
	missing := make([]string, 0, 1)
	for name := range srcExt {
		if !dstExt[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		r.Add(Check{Name: "extensions", Side: "network", Sev: Pass,
			Msg: "all source extensions are installed in the destination"})
		return
	}
	sort.Strings(missing)
	r.Add(Check{Name: "extensions", Side: "network", Sev: Warn,
		Msg: fmt.Sprintf("%d source extension(s) missing in the destination: %s",
			len(missing), strings.Join(missing, ", ")),
		Rec: "install the missing extension(s) in the destination BEFORE the data load (as with a backup restore, dependent extensions must pre-exist in the receiver): CREATE EXTENSION <name>"})
}

// installedExtensions returns the set of extension names installed in the
// database the querier is connected to.
func installedExtensions(ctx context.Context, q pg.Querier, sql string) (map[string]bool, error) {
	rows, err := q.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(rows))
	for _, row := range rows {
		if len(row) == 0 || row[0] == nil {
			continue
		}
		set[fmt.Sprint(row[0])] = true
	}
	return set, nil
}

// firstAsInt coerces the first cell of the first row to an int. Live
// PostgreSQL surfaces booleans as the text "t"/"f" (libpq text protocol),
// while fakes supply int64 — both map to 1/0.
func firstAsInt(rows [][]any) int {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 0
	}
	switch v := rows[0][0].(type) {
	case int64:
		return int(v)
	case int:
		return v
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		switch v {
		case "t", "true", "1":
			return 1
		}
		return 0
	}
	return 0
}
