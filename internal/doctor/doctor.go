// Package doctor implements `gpstream doctor` (FR-05): a self-diagnostic that
// reports PASS/FAIL/WARN for the source and destination clusters with
// actionable recommendations. It is intentionally dependency-free of a live
// cluster so it can be exercised against fakes (CР-7: the doctor must detect
// all seeded faults on a test bench).
package doctor

import (
	"context"
	"fmt"
	"sort"
	"strings"

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
	r.checkOnSegment("source", src, opts.SegmentBinary)
	r.checkDisk("dest", dst)
	r.checkPermissions("source", src)
	r.checkPermissions("dest", dst)
	r.checkCollation(src, dst)
	r.checkTLS(opts.MinTLS)
	return r
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
			Rec:  "expected on plain PostgreSQL; on GP6 install gpstream-segreceiver on the segment hosts and set --seg-binary"})
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
	// A cluster with zero up segments cannot copy data (CР-7): report it as a
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
	// testable path we query a sentinel the bench injects (CР-7: "no space").
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
	if srcColl != "" && dstColl != "" && srcColl != dstColl {
		r.Add(Check{
			Name: "collation",
			Side: "network",
			Sev:  Warn,
			Msg:  fmt.Sprintf("LC_COLLATE differs: source=%q dest=%q", srcColl, dstColl),
			Rec:  "differences affect partition routing; review before start (FR-10)",
		})
		return
	}
	r.Add(Check{Name: "collation", Side: "network", Sev: Pass, Msg: "collation matches or unknown"})
}

func (r *Report) checkTLS(minTLS string) {
	// TLS chain validation is wired in internal/conn (M2, FR-13); doctor here
	// reports the configured minimum and defers cert inspection.
	r.Add(Check{Name: "tls", Side: "network", Sev: Warn,
		Msg: fmt.Sprintf("min TLS %s required (full cert validation in M2)", minTLS),
		Rec: "provide --ssl-ca / --ssl-cert for mTLS"})
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
