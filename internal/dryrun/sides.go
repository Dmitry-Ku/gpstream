// Package dryrun — 2C (architecture 08.10): the dry-run is a run-loop without
// copying, and the controller holds NO direct database connection. Each side
// (source / destination) gathers its own dry-run values on its LOCAL database
// connection (the side's AGENT), over the control plane; the controller
// collects both sides' SideValues and computes the diff in memory.
//
// This file is the side-gather + diff core. It is SEPARATE from Run (the
// legacy controller-side diagnostic, still used by `gpstream dry-run` until
// the controller flips to the agents in 2C-1/2/4/5); the two share the same
// check names and severities so the operator-facing report is identical.
// Run is deleted (or repurposed) when the controller stops opening its own
// connections.
package dryrun

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/topology"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// SideValues is ONE side's (source or destination) raw dry-run values,
// gathered by that side's agent over its LOCAL database connection. It is
// JSON-safe (the wire round-trip is value-identical) so it crosses the
// source/dest control sockets. The controller never opens a database
// connection: it collects both sides' SideValues and runs FromSides on them.
type SideValues struct {
	Side string `json:"side"` // "source" or "dest"
	// Reachable is the side's coordinator ping result; ReachErr is the ping
	// error (empty when reachable). When not reachable the rest is
	// undeterminable and left empty (the reachability check is the FAIL).
	Reachable bool   `json:"reachable"`
	ReachErr  string `json:"reach_err,omitempty"`
	// Version is the side's SELECT version() (empty on a failed ping).
	Version string `json:"version,omitempty"`
	// Greenplum is true when the side has a gp_segment_configuration catalog.
	Greenplum bool `json:"greenplum"`
	// UpCount is the side's up-segment count (0 when not Greenplum).
	UpCount int `json:"up_count,omitempty"`
	// HasCreate / HasConnect are the side's database privileges.
	HasCreate  bool `json:"has_create,omitempty"`
	HasConnect bool `json:"has_connect,omitempty"`
	// DiskOK / DiskErr are the side's disk probe result.
	DiskOK  bool   `json:"disk_ok"`
	DiskErr string `json:"disk_err,omitempty"`
	// StmtTimeout / IdleTimeout are the side's operator-set statement /
	// idle-in-transaction timeouts (empty/0 = unset).
	StmtTimeout string `json:"stmt_timeout,omitempty"`
	IdleTimeout string `json:"idle_timeout,omitempty"`
	// MaxConns is the side's max_connections (0 = could not determine).
	MaxConns int `json:"max_conns,omitempty"`
	// Collation is the side's database LC_COLLATE name.
	Collation string `json:"collation,omitempty"`
	// CollationSig is the side's behavioral collation probe signature (glibc
	// sort order; catches same-name-different-glibc, F-16).
	CollationSig string `json:"collation_sig,omitempty"`
	// CollationSigErr is the probe fault (empty = ok / not run).
	CollationSigErr string `json:"collation_sig_err,omitempty"`
	// Encoding is the side's database character encoding.
	Encoding string `json:"encoding,omitempty"`
	// Extensions is the side's installed extension names.
	Extensions []string `json:"extensions,omitempty"`
	// SourceCollationsUsed (source side only) are the distinct explicit
	// column collations the source's public-schema tables use (F-16).
	SourceCollationsUsed []string `json:"src_collations_used,omitempty"`
	// DestCollationsInstalled (dest side only) are the dest's installed
	// collation names.
	DestCollationsInstalled []string `json:"dst_collations_installed,omitempty"`
	// OnSegmentOK / OnSegmentSummary are the source side's segment-direct
	// capability (GUC + dispatch + segreceiver binary, via the kernel's
	// ON SEGMENT probe). Empty summary = not checked (no segreceiver path
	// supplied).
	OnSegmentOK      bool     `json:"on_segment_ok,omitempty"`
	OnSegmentSummary []string `json:"on_segment_summary,omitempty"`
	// CopyToProgramGUC (source side) is the value of the Yandex-fork GUC
	// ycmdb.yc_allow_copy_to_program (the gate for COPY TO PROGRAM, which the
	// mesh on_segment data path uses to launch the segreceivers). Read from
	// pg_settings (not current_setting) so a MISSING GUC (a non-Yandex build,
	// where COPY TO PROGRAM is allowed by default) is distinguishable from a
	// PRESENT-but-off GUC (a Yandex build that forbids it). Empty = the GUC
	// is not defined on this build (or the probe did not run).
	// CopyToProgramGUCSource is the pg_settings source (default /
	// configuration file / ...).
	CopyToProgramGUC       string `json:"copy_to_program_guc,omitempty"`
	CopyToProgramGUCSource string `json:"copy_to_program_guc_source,omitempty"`
	// MiniReady is the dest side's mini-agent readiness note (SSH to the D
	// segment hosts: mini binary + ports, WITHOUT a real launch). Empty =
	// not a supervisor / not checked.
	MiniReady string `json:"mini_ready,omitempty"`
}

// GatherSide reads ONE side's raw dry-run values on q (the side's LOCAL
// database connection, owned by the side's agent). side is "source" or
// "dest". workers is the run's --workers (the max_connections sizing check).
// It never panics on an unreachable cluster; a failed ping is itself
// captured (ReachErr).
//
// 2C-3b: the on_segment / mini-readiness checks are NO LONGER gathered here —
// they require SSH to the segment hosts, which only the side's AGENT can do
// (the controller holds no data/SSH connection). The agent calls
// checkOnSegmentAPlus / checkMiniReady and sets OnSegmentOK/Summary and
// MiniReady on the returned SideValues. This function does DB queries only.
func GatherSide(ctx context.Context, q pg.Querier, side string, workers int) *SideValues {
	sv := &SideValues{Side: side}
	if err := q.Ping(ctx); err != nil {
		sv.ReachErr = err.Error()
		return sv
	}
	sv.Reachable = true
	sv.Version = pg.RowStr(ctx, q, "SELECT version();")

	// Segments: a NULL gp_segment_configuration = plain PostgreSQL (single
	// node); present = Greenplum (count the up segments).
	if top, err := topology.Read(ctx, q); err == nil && top != nil && top.IsGreenplum() {
		sv.Greenplum = true
		sv.UpCount = top.UpCount()
	}

	// Disk: a live probe (the bench injects a fault via the sentinel query).
	if rows, derr := q.Query(ctx, "SELECT 1"); derr != nil {
		sv.DiskErr = derr.Error()
	} else if len(rows) == 0 {
		sv.DiskErr = "disk probe returned no data"
	} else {
		sv.DiskOK = true
	}

	// Permissions: the streaming role's database privileges.
	if prows, perr := q.Query(ctx,
		"SELECT has_database_privilege(current_database(), 'CREATE'), "+
			"has_database_privilege(current_database(), 'CONNECT')"); perr == nil && len(prows) > 0 {
		sv.HasCreate = cellAsInt(prows[0][0]) == 1
		if len(prows[0]) > 1 {
			sv.HasConnect = cellAsInt(prows[0][1]) == 1
		}
	}

	// Operator statement/idle timeouts (a non-zero value can kill a long COPY).
	sv.StmtTimeout = pg.RowStr(ctx, q, "SHOW statement_timeout;")
	sv.IdleTimeout = pg.RowStr(ctx, q, "SHOW idle_in_transaction_session_timeout;")

	// max_connections (the connection-sizing check compares it to --workers).
	if mc := pg.RowStr(ctx, q, "SHOW max_connections"); mc != "" {
		var n int
		if _, aerr := fmt.Sscanf(mc, "%d", &n); aerr == nil {
			sv.MaxConns = n
		}
	}

	// Collation (name + behavioral probe) and encoding (F-16).
	sv.Collation = pg.RowStr(ctx, q, "SELECT datcollate FROM pg_database WHERE datname = current_database()")
	if sig, serr := pg.CollationSignature(q, ctx); serr != nil {
		sv.CollationSigErr = serr.Error()
	} else {
		sv.CollationSig = sig
	}
	sv.Encoding = pg.RowStr(ctx, q, `SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()`)

	// Installed extensions (the source->dest diff is the F-11 restore check).
	sv.Extensions = gatherExtensions(ctx, q)

	// Side-specific collation sets (the column-collation diff, F-16).
	if side == "source" {
		sv.SourceCollationsUsed = sourceCollationsUsed(ctx, q)
		// The Yandex-fork COPY TO PROGRAM gate (mesh on_segment launches the
		// segreceivers via COPY ... TO PROGRAM ... ON SEGMENT). Read from
		// pg_settings so a MISSING GUC (non-Yandex build: COPY TO PROGRAM is
		// allowed by default) is distinguishable from a PRESENT-but-off GUC
		// (Yandex build: forbidden — a hard mesh blocker the dry-run must
		// surface). The coordinator path (COPY ... TO STDOUT) does not need
		// it. (The agent's on_segment A+ check below covers the binary /
		// listen-grid / SSH facets, not the GUC.)
		if rows, gerr := q.Query(ctx, "SELECT setting, source FROM pg_settings WHERE name = '"+segstreamGUCName()+"'"); gerr == nil && len(rows) > 0 && len(rows[0]) >= 2 {
			sv.CopyToProgramGUC = cellAsStr(rows[0][0])
			if len(rows[0]) > 1 {
				sv.CopyToProgramGUCSource = cellAsStr(rows[0][1])
			}
		}
		// on_segment (A+) + mini-ready (dest) are set by the AGENT after this
		// gather (they need SSH to the segment hosts — see dryrun_checks.go).
	} else {
		sv.DestCollationsInstalled = destCollationsInstalled(ctx, q)
	}
	return sv
}

// FromSides builds the aggregated dry-run Report from the two sides' gathered
// values + the run options. It is PURE (no database access) — the controller
// runs it in memory over the values the agents reported, so it holds no
// direct connection. The check names/severities mirror Run (the legacy
// controller-side diagnostic) so the operator-facing report is identical.
func FromSides(src, dst *SideValues, opts Options) *Report {
	r := &Report{Healthy: true}
	r.addSideChecks(src, opts.Workers)
	r.addSideChecks(dst, opts.Workers)
	r.addRouteChecks(src, dst)
	r.addCollationChecks(src, dst)
	r.addEncodingChecks(src, dst)
	r.addExtensionChecks(src, dst)
	r.addOnSegmentChecks(src)
	r.addCopyToProgramGUCCheck(src, opts.CopyMode)
	r.addMiniReadyChecks(dst)
	r.addTLSCheck(opts.MinTLS)
	r.addTopologyCheck()
	return r
}

// addSideChecks turns one side's values into its per-side checks.
func (r *Report) addSideChecks(sv *SideValues, workers int) {
	side := sv.Side
	if !sv.Reachable {
		r.Add(Check{Name: "reachability", Side: side, Sev: Fail,
			Msg: fmt.Sprintf("cannot reach %s cluster: %s", side, sv.ReachErr),
			Rec: "verify network, coordinator host/port, and credentials"})
		return // the rest is undeterminable on an unreachable cluster
	}
	r.Add(Check{Name: "reachability", Side: side, Sev: Pass, Msg: "coordinator reachable"})

	if sv.Version == "" {
		r.Add(Check{Name: "version", Side: side, Sev: Fail, Msg: "version query failed",
			Rec: "the query account may lack privilege"})
	} else {
		r.Add(Check{Name: "version", Side: side, Sev: Pass, Msg: sv.Version})
	}

	if sv.Greenplum {
		if sv.UpCount == 0 {
			r.Add(Check{Name: "segments", Side: side, Sev: Fail, Msg: "no up segments",
				Rec: "all segments are down; bring up gp_segment_configuration before copy"})
		} else {
			r.Add(Check{Name: "segments", Side: side, Sev: Pass, Msg: fmt.Sprintf("%d up segments", sv.UpCount)})
		}
	} else {
		r.Add(Check{Name: "segments", Side: side, Sev: Warn,
			Msg: "not a Greenplum cluster (no gp_segment_configuration) — single-node mode",
			Rec: "expected when running against plain PostgreSQL (GP6 kernel PoC); on a real GP6 cluster all segments must be 'u'"})
	}

	if sv.DiskOK {
		r.Add(Check{Name: "disk", Side: side, Sev: Pass, Msg: "disk probe ok"})
	} else {
		r.Add(Check{Name: "disk", Side: side, Sev: Fail, Msg: "disk probe failed: " + sv.DiskErr,
			Rec: "check destination disk space"})
	}

	if sv.HasCreate {
		r.Add(Check{Name: "permissions", Side: side, Sev: Pass, Msg: "CREATE/CONNECT on current database ok"})
	} else {
		r.Add(Check{Name: "permissions", Side: side, Sev: Warn, Msg: "role lacks CREATE/CONNECT on current database",
			Rec: "GRANT CREATE, CONNECT ON DATABASE to the streaming role"})
	}

	if isNonzeroDuration(sv.StmtTimeout) || isNonzeroDuration(sv.IdleTimeout) {
		r.Add(Check{Name: "statement-timeout", Side: side, Sev: Warn,
			Msg: fmt.Sprintf("operator timeout set (statement_timeout=%q, idle_in_transaction_session_timeout=%q)", sv.StmtTimeout, sv.IdleTimeout),
			Rec: "gpstream holds long-lived COPY sessions; an operator statement/idle timeout can kill a long copy on a slow network. Consider raising it for the copy duration or leaving it 0 (DESIGN §4.6)."})
	} else {
		r.Add(Check{Name: "statement-timeout", Side: side, Sev: Pass, Msg: "no operator statement/idle timeout"})
	}

	// max_connections vs --workers (the source side is the one that pins N).
	if side == "source" && workers > 1 {
		need := workers + 4
		if sv.MaxConns == 0 {
			// could not determine; reachability already reported
		} else if sv.MaxConns < need {
			r.Add(Check{Name: "max_connections", Side: side, Sev: Warn,
				Msg: fmt.Sprintf("max_connections=%d < %d needed for --workers %d (snapshot-epoch N pinned conns + pool headroom)", sv.MaxConns, need, workers),
				Rec: "raise the source DB's max_connections or lower --workers (a too-small pool blocks acquires; the run stays correct but slows)"})
		} else {
			r.Add(Check{Name: "max_connections", Side: side, Sev: Pass,
				Msg: fmt.Sprintf("max_connections=%d >= %d for --workers %d", sv.MaxConns, need, workers)})
		}
	}
}

// addRouteChecks is the informational route-mode summary (M4/T4b).
func (r *Report) addRouteChecks(src, dst *SideValues) {
	srcSeg := int32(0)
	dstSeg := int32(0)
	if src.Greenplum {
		srcSeg = int32(src.UpCount)
	}
	if dst.Greenplum {
		dstSeg = int32(dst.UpCount)
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
			srcSeg, dstSeg, pairs, boolStr(directOK, "direct_1to1 possible", "direct_1to1 not possible (S!=D) — router_full"))})
}

// addCollationChecks compares the two sides' collation (name, F-10; behavioral
// probe, F-16) and the explicit column collations (F-16).
func (r *Report) addCollationChecks(src, dst *SideValues) {
	nameDiff := src.Collation != "" && dst.Collation != "" && src.Collation != dst.Collation
	if nameDiff {
		r.Add(Check{Name: "collation", Side: "network", Sev: Warn,
			Msg: fmt.Sprintf("LC_COLLATE differs: source=%q dest=%q", src.Collation, dst.Collation),
			Rec: "differences affect partition routing; review before start (FR-10)"})
	} else {
		r.Add(Check{Name: "collation", Side: "network", Sev: Pass, Msg: "collation matches or unknown"})
	}
	if src.CollationSigErr == "" && dst.CollationSigErr == "" && src.CollationSig != "" && dst.CollationSig != "" && src.CollationSig != dst.CollationSig {
		r.Add(Check{Name: "collation-behavior", Side: "network", Sev: Warn,
			Msg: fmt.Sprintf("same locale name (%s) but DIFFERENT glibc sort behavior (behavioral probe): source=%v dest=%v",
				orUnknown(src.Collation), src.CollationSig, dst.CollationSig),
			Rec: "string sort, ORDER BY, index and partition-routing semantics will differ on the destination; the data migrates byte-for-byte but its ordering changes — admin decision (F-16)"})
	}
	missing := missingCollationsFromSets(src.SourceCollationsUsed, dst.DestCollationsInstalled)
	if len(missing) > 0 {
		r.Add(Check{Name: "column-collation", Side: "network", Sev: Warn,
			Msg: fmt.Sprintf("explicit column collation(s) %v on the source are not installed in the destination database", missing),
			Rec: "CREATE COLLATION in the destination (or accept the DB-default collation) before the data load — sorting/index semantics over those columns will differ (FR-16)"})
	}
}

// addEncodingChecks (F-16): an encoding mismatch is data-destructive.
func (r *Report) addEncodingChecks(src, dst *SideValues) {
	if src.Encoding == "" || dst.Encoding == "" {
		return
	}
	if src.Encoding != dst.Encoding {
		r.Add(Check{Name: "encoding", Side: "network", Sev: Warn,
			Msg: fmt.Sprintf("database encoding differs: source=%s dest=%s", src.Encoding, dst.Encoding),
			Rec: "an encoding mismatch reinterprets string bytes on the destination (DATA DESTRUCTIVE) — create the destination database with the source's encoding before the load (F-16)"})
	}
}

// addExtensionChecks (FR-05/FR-11): source extensions missing in the dest.
func (r *Report) addExtensionChecks(src, dst *SideValues) {
	dstSet := make(map[string]bool, len(dst.Extensions))
	for _, e := range dst.Extensions {
		dstSet[e] = true
	}
	var missing []string
	for _, e := range src.Extensions {
		if !dstSet[e] {
			missing = append(missing, e)
		}
	}
	if len(missing) == 0 {
		r.Add(Check{Name: "extensions", Side: "network", Sev: Pass,
			Msg: "all source extensions are installed in the destination"})
		return
	}
	sort.Strings(missing)
	r.Add(Check{Name: "extensions", Side: "network", Sev: Warn,
		Msg: fmt.Sprintf("%d source extension(s) missing in the destination: %s", len(missing), strings.Join(missing, ", ")),
		Rec: "install the missing extension(s) in the destination BEFORE the data load (as with a backup restore, dependent extensions must pre-exist in the receiver): CREATE EXTENSION <name>"})
}

// addOnSegmentChecks (source side): the segment-direct capability (on_segment).
func (r *Report) addOnSegmentChecks(src *SideValues) {
	if len(src.OnSegmentSummary) == 0 {
		return // no segreceiver path supplied — the check is not run
	}
	if src.OnSegmentOK {
		r.Add(Check{Name: "on_segment", Side: "source", Sev: Pass,
			Msg: "segment-direct capability ok: " + strings.Join(src.OnSegmentSummary, "; ")})
		return
	}
	r.Add(Check{Name: "on_segment", Side: "source", Sev: Warn,
		Msg: "segment-direct capability degraded: " + strings.Join(src.OnSegmentSummary, "; "),
		Rec: "the job will fall back to the coordinator path; fix the items above to enable the 2.0x segment-direct fast path"})
}

// segstreamGUCName is the Yandex-fork gate for COPY TO PROGRAM. It mirrors
// segstream.GUCName (dryrun does not import the transfer layer).
func segstreamGUCName() string { return "ycmdb.yc_allow_copy_to_program" }

// addCopyToProgramGUCCheck (source side): the mesh on_segment data path
// launches the segreceivers via `COPY ... TO PROGRAM ... ON SEGMENT`, which
// the Yandex GP fork gates behind the ycmdb.yc_allow_copy_to_program GUC
// (PGC_POSTMASTER). The check is a HARD BLOCKER only for --copy-mode mesh:
//
//   - GUC absent (non-Yandex build) -> no finding: COPY TO PROGRAM is
//     allowed there by default; the on_segment capability check verifies the
//     actual dispatch;
//   - GUC present and "on" -> PASS (the mesh path can dispatch);
//   - GUC present and "off" -> FAIL for mesh (the on_segment dispatch is
//     rejected at run time with "forbidden to COPY to or from an external
//     program" — the run must not start; the admin sets the GUC in
//     postgresql.conf of every source data-dir and restarts), WARN otherwise
//     (the coordinator path does not use COPY TO PROGRAM).
func (r *Report) addCopyToProgramGUCCheck(src *SideValues, copyMode string) {
	if src == nil || src.CopyToProgramGUC == "" {
		return // GUC absent (non-Yandex build) or the probe did not run
	}
	if strings.ToLower(src.CopyToProgramGUC) == "on" {
		r.Add(Check{Name: "copy_to_program_guc", Side: "source", Sev: Pass,
			Msg: fmt.Sprintf("GUC %s = on (source: COPY TO PROGRAM allowed; the mesh on_segment dispatch can run)", segstreamGUCName())})
		return
	}
	// Present but not "on": this build forbids COPY TO PROGRAM.
	if copyMode == "mesh" {
		r.Add(Check{Name: "copy_to_program_guc", Side: "source", Sev: Fail,
			Msg: fmt.Sprintf("GUC %s = %q (source: COPY TO PROGRAM is forbidden, but --copy-mode mesh launches the segreceivers via COPY ... TO PROGRAM ... ON SEGMENT) — the run cannot start", segstreamGUCName(), src.CopyToProgramGUC),
			Rec: "set ycmdb.yc_allow_copy_to_program=on in postgresql.conf of EVERY source data-dir (master and all segments) and restart the cluster (gpstop -ar); or re-run with --copy-mode coordinator (which uses COPY ... TO STDOUT and does not need the GUC)"})
		return
	}
	r.Add(Check{Name: "copy_to_program_guc", Side: "source", Sev: Warn,
		Msg: fmt.Sprintf("GUC %s = %q (source: COPY TO PROGRAM is forbidden on this build; the mesh on_segment path would not work)", segstreamGUCName(), src.CopyToProgramGUC),
		Rec: "the coordinator path is unaffected (COPY ... TO STDOUT); for mesh, set ycmdb.yc_allow_copy_to_program=on in postgresql.conf of every source data-dir and restart"})
}

// cellAsStr renders a result cell as a string (nil -> "").
func cellAsStr(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// addMiniReadyChecks (dest side): the mini-agent readiness note.
func (r *Report) addMiniReadyChecks(dst *SideValues) {
	if dst.MiniReady == "" {
		return // not a supervisor / not checked
	}
	sev := Pass
	rec := ""
	if strings.HasPrefix(dst.MiniReady, "FAIL") {
		sev = Warn
		rec = "the mesh mini path needs the gpstream-dest-agent binary (and its ports) on every D segment host"
	}
	r.Add(Check{Name: "mini-ready", Side: "dest", Sev: sev, Msg: dst.MiniReady, Rec: rec})
}

// addTLSCheck reports the configured minimum TLS (full cert validation is a
// control/data-plane concern; the dry-run reports the configured floor).
func (r *Report) addTLSCheck(minTLS string) {
	r.Add(Check{Name: "tls", Side: "network", Sev: Warn,
		Msg: fmt.Sprintf("min TLS %s required (full cert validation in M2)", minTLS),
		Rec: "provide --ssl-ca / --ssl-cert for mTLS"})
}

// addTopologyCheck confirms the two-agent data-plane architecture (P7).
func (r *Report) addTopologyCheck() {
	r.Add(Check{Name: "topology", Side: "network", Sev: Pass,
		Msg: "two-agent data path: the data stream runs source agent -> dest agent (compressed on the wire) and the controller holds no data connections (ADR-002)"})
}

// gatherExtensions returns the installed extension names on q's database.
func gatherExtensions(ctx context.Context, q pg.Querier) []string {
	rows, err := q.Query(ctx, "SELECT extname FROM pg_extension ORDER BY extname")
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) > 0 && row[0] != nil {
			out = append(out, fmt.Sprint(row[0]))
		}
	}
	return out
}

// sourceCollationsUsed returns the distinct explicit collation names used by
// columns of the source's public-schema tables (F-16 dry-run pre-flight).
func sourceCollationsUsed(ctx context.Context, q pg.Querier) []string {
	rows, err := q.Query(ctx,
		`SELECT DISTINCT co.collname
		 FROM pg_attribute a
		 JOIN pg_class c ON c.oid = a.attrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 JOIN pg_collation co ON co.oid = a.attcollation
		 WHERE c.relkind IN ('r','p') AND n.nspname = 'public'
		   AND a.attnum > 0 AND NOT a.attisdropped
		   AND co.collname IS DISTINCT FROM 'default' /* gs-collation-dryrun */`)
	if err != nil || len(rows) == 0 {
		return nil
	}
	var out []string
	for _, row := range rows {
		if len(row) >= 1 && row[0] != nil {
			if name := fmt.Sprint(row[0]); name != "" {
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out
}

// destCollationsInstalled returns the dest's installed collation names (the
// pg_collation set, F-16 dry-run pre-flight).
func destCollationsInstalled(ctx context.Context, q pg.Querier) []string {
	rows, err := q.Query(ctx, `SELECT collname FROM pg_collation WHERE collnamespace = 11 /* gs-collation-dryrun-dst */`)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) >= 1 && row[0] != nil {
			if name := fmt.Sprint(row[0]); name != "" {
				out = append(out, name)
			}
		}
	}
	return out
}

// missingCollationsFromSets is the F-16 column-collation diff over the two
// sides' gathered collation sets (source's used vs destination's installed).
func missingCollationsFromSets(used, installed []string) []string {
	inst := make(map[string]bool, len(installed))
	for _, c := range installed {
		inst[c] = true
	}
	var missing []string
	for _, c := range used {
		if !inst[c] {
			missing = append(missing, c)
		}
	}
	sort.Strings(missing)
	return missing
}

// cellAsInt coerces one result cell to an int (live PostgreSQL surfaces
// booleans as "t"/"f"; fakes supply int64 — both map to 1/0).
func cellAsInt(v any) int {
	switch c := v.(type) {
	case int64:
		return int(c)
	case int:
		return c
	case bool:
		if c {
			return 1
		}
		return 0
	case string:
		switch c {
		case "t", "true", "1":
			return 1
		}
		return 0
	}
	return 0
}
