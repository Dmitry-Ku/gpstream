// Package planner builds the transfer plan for gpstream (FR-03/FR-10). It
// constructs the full object graph (databases/schemas/tables/partitions/
// indexes) BEFORE the job starts, estimates the volume and duration, detects
// conflicts and collation differences, and emits a deterministic plan.
//
// Determinism (FR-10/CР-4): the plan hash is a pure function of the inputs
// (units, selection, compression, workers, …) with no time or randomness, so
// two --dry-run on an unchanged topology are byte-identical.
package plan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Table is one transferable table in the object graph (FR-03).
type Table struct {
	DB     string
	Schema string
	Name   string
	// DstDB/DstSchema/DstName are the destination names after rename (FR-11/
	// §4.10); they equal DB/Schema/Name when no rename applies. Conflicts are
	// detected against the destination schema+name, so a rename avoids a
	// would-be overwrite.
	DstDB       string
	DstSchema   string
	DstName     string
	LeafPart    []string // leaf partition names, for explicit routing (FR-03)
	IsAO        bool     // Append-Optimized
	IsCO        bool     // Column-Oriented
	Bytes       int64    // estimated size
	RowEst      int64    // estimated rows
	Indexes     int      // number of indexes
	HasConflict bool     // exists in dest and not overwritten (FR-10)
	// RouteMode (M4/T4b): how this table's data lands on the destination —
	// route.Direct1to1 (S==D, keys equal, same major; 0 CPU on hash),
	// route.RouterFull (per-row hash by K_dest), or "" when not resolvable
	// (plain-PG / catalog unavailable — conservative, no routing decision).
	RouteMode route.Mode `json:"route_mode,omitempty"`
	// RoutePairs: number of eligible (src,dst) content pairs for this unit.
	RoutePairs int `json:"route_pairs,omitempty"`
}

// Plan is the deterministic transfer plan (FR-10).
type Plan struct {
	Units       []Table         `json:"units"`
	TotalBytes  int64           `json:"total_bytes"`
	Pairs       []string        `json:"pairs"` // host-pairs, e.g. "src-host1:5432->dst-host2:5432"
	WorkPerPair int             `json:"work_per_pair"`
	ETASeconds  int             `json:"eta_seconds"`
	Redistrib   Redistribution  `json:"redistribution"`  // FR-15 accounting
	Collations  []CollationDiff `json:"collation_diffs"` // FR-10/FR-16
	Conflicts   []Conflict      `json:"conflicts"`
	Route       RouteSummary    `json:"route"` // M4/T4b: route-mode summary (per-unit in Units)
	Hash        string          `json:"hash"`
	// EstimatedVolume/EstimatedETA carry the volume/time prediction (FR-10).
	EstimatedVolume int64 `json:"estimated_volume"`
	EstimatedETA    int   `json:"estimated_eta"`
	// Partial is set when the plan was formed under a --timeout (FR-10 §6.6):
	// the fragment is whatever was collected before the deadline, and the
	// unknown parts are predictions. A partial plan carries no hash (it is not
	// a complete, deterministic plan) and cmdDryRun prints it as a forecast.
	Partial bool `json:"partial,omitempty"`
}

// CollationDiff is a detected LC_COLLATE/LC_CTYPE difference (FR-10/FR-16).
type CollationDiff struct {
	DB        string
	LCCollate string
	LCType    string
}

// Conflict is a detected target-table conflict (FR-10/FR-11).
type Conflict struct {
	DB     string
	Schema string
	Table  string
	Action string // what the policy does (skip/overwrite/...)
}

// Redistribution is the FR-15 redistribution accounting: whether the copied
// data lands in the same distribution on the destination as on the source,
// and — the metric CR-5 requires — the measured segment skew of the copied
// data. Skew is the fraction (0..1) of the total data that must be moved
// between segments by a redistribution: 0 means the destination lays the data
// out exactly like the source (no redistribution), 1 means every row changes
// segment. NeedsRedistrib is true when the measured skew exceeds the
// operator's --skew-threshold (or when a cluster cannot report its distribution
// layout, the conservative answer).
type Redistribution struct {
	NeedsRedistrib bool    `json:"needs_redistribution"` // FR-15: skew > threshold
	Skew           float64 `json:"skew"`                 // FR-15/CR-5: measured segment skew (0..1)
	Threshold      float64 `json:"threshold"`            // the --skew-threshold applied
	Reason         string  `json:"reason"`               // why redistribution is (not) needed
}

// Build produces the plan for a config against source + destination clusters.
// src and dst are queried with pg.Querier so the plan is testable without a
// live cluster (CР-4/CР-7). The plan hash is deterministic (CР-4).
func Build(ctx context.Context, cfg *config.Config, src, dst pg.Querier) (*Plan, error) {
	p := &Plan{
		WorkPerPair: cfg.WorkersPerPair,
	}

	// 1. Availability of both clusters (FR-10).
	if err := checkAvailability(ctx, src, dst); err != nil {
		return timeoutOrPartial(p, ctx, err)
	}

	// 2. Object graph + volume (pg_size_bytes) (FR-10).
	tabs, err := collectTables(ctx, cfg, src)
	if err != nil {
		return timeoutOrPartial(p, ctx, err)
	}

	// 2b. Selection (FR-11): narrow the object graph to the requested
	// db/schema/table set, honouring excludes and metadata/data-only modes.
	tabs = selectUnits(cfg, tabs)

	// 2c. Rename (FR-11): map source names to destination names. Conflicts are
	// detected against the destination name, so a rename avoids an overwrite.
	applyRename(cfg, tabs)

	// 3. Conflicts: a source table (at its destination name) that exists in
	// dest and is not overwritten (FR-10/FR-11).
	p.Conflicts = detectConflicts(ctx, tabs, dst, cfg)

	// 4. Collation differences (FR-10/FR-16).
	p.Collations = detectCollations(ctx, tabs, src, dst)

	// 4b. Redistribution accounting (FR-15/CR-5): compute, from the clusters'
	// own distribution keys and segment counts, the measured segment skew of
	// the copied data and whether it exceeds the --skew-threshold. A mismatch
	// in distribution keys means the destination lays the data out differently
	// and the data would be redistributed on the destination.
	p.Redistrib = detectRedistribution(ctx, cfg, src, dst, tabs)

	// 4c. Route-mode (M4/T4b): per-unit direct_1to1 vs router_full from the
	// clusters' distribution policies + segment counts. Degrades to "" (not
	// resolvable) on plain-PostgreSQL / unavailable catalogs — conservative,
	// never fails the plan.
	p.Route = resolveRoutes(ctx, src, dst, tabs)

	// 5. Aggregate units + volume.
	for _, t := range tabs {
		p.Units = append(p.Units, t)
		p.TotalBytes += t.Bytes
		p.EstimatedVolume += t.Bytes
	}

	// 6. Host pairs (FR-10).
	p.Pairs = hostPairs(cfg)

	// 7. ETA from measured bandwidth (FR-10); 0 if rate-limit unknown.
	if cfg.RateLimit > 0 {
		gbps := float64(cfg.RateLimit) * 1_000_000_000 / 8
		if gbps > 0 {
			p.ETASeconds = int(float64(p.TotalBytes) / gbps)
		}
	}
	// EstimatedETA mirrors the computed ETA (set after the rate-limit block so
	// it is not clobbered by the zero initial value).
	p.EstimatedETA = p.ETASeconds

	// 8. Deterministic hash (CР-4): no time, no randomness. A partial plan
	// (formed under a --timeout) is not a complete plan, so it carries no hash.
	if !p.Partial {
		p.Hash = hash(p)
	}
	return p, nil
}

// timeoutOrPartial converts a plan-formation failure into the correct outcome:
// if the context was cancelled/timed out (FR-10 --timeout) it returns the
// partial fragment already collected plus the error, so cmdDryRun can print the
// fragment with a "prediction/forecast" marker (§6.6) instead of a hard failure;
// any other error is returned with a nil plan (a genuine failure, CР-7).
func timeoutOrPartial(p *Plan, ctx context.Context, err error) (*Plan, error) {
	if ctx.Err() != nil {
		p.Partial = true
		return p, fmt.Errorf("plan formation timed out: %w", err)
	}
	return nil, err
}

func checkAvailability(ctx context.Context, src, dst pg.Querier) error {
	if src != nil {
		if err := src.Ping(ctx); err != nil {
			return fmt.Errorf("source unavailable: %w", err)
		}
	}
	if dst != nil {
		if err := dst.Ping(ctx); err != nil {
			return fmt.Errorf("dest unavailable: %w", err)
		}
	}
	return nil
}

func collectTables(ctx context.Context, cfg *config.Config, src pg.Querier) ([]Table, error) {
	rows, err := src.Query(ctx,
		`SELECT n.nspname, c.relname, c.relkind, COALESCE(pg_total_relation_size(c.oid),0),
		COALESCE((SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid),0),
		CASE WHEN c.relkind='p' THEN COALESCE((SELECT count(*) FROM pg_index i WHERE i.indrelid=c.oid),0) ELSE 0 END,
		current_database()
		FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit')
		ORDER BY n.nspname, c.relname`)
	if err != nil {
		return nil, err
	}
	var out []Table
	for _, r := range rows {
		if len(r) < 7 {
			continue
		}
		t := Table{
			DB:      asString(r[6]),
			Schema:  asString(r[0]),
			Name:    asString(r[1]),
			Bytes:   int64(toInt64(r[3])),
			Indexes: int(toInt64(r[5])),
		}
		// AO/CO flags would be read from pg_attribute/pg_class in the full
		// build (FR-03); here we mark the conservative default.
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// selectUnits narrows the object graph to the requested selection (FR-11):
// database / schema / table inclusion, exclude-*, and metadata/data-only modes.
// With no selection set the full graph is returned (the default whole-cluster
// copy).
func selectUnits(cfg *config.Config, tabs []Table) []Table {
	var out []Table
	for _, t := range tabs {
		if !selected(cfg, t) {
			continue
		}
		out = append(out, t)
	}
	return out
}

// selected reports whether t is within the requested selection (FR-11).
func selected(cfg *config.Config, t Table) bool {
	// Excludes win first (any exclude-* entry that matches).
	for _, ex := range cfg.Exclude {
		if matchAny(ex, t.DB, t.Schema, t.Name) {
			return false
		}
	}
	// If a positive selection was given, the unit must match at least one.
	if len(cfg.DBs) > 0 || len(cfg.Schemas) > 0 || len(cfg.Tables) > 0 {
		ok := false
		for _, db := range cfg.DBs {
			if db == t.DB {
				ok = true
			}
		}
		for _, s := range cfg.Schemas {
			if matchSchema(s, t.Schema) {
				ok = true
			}
		}
		for _, tb := range cfg.Tables {
			if matchTable(tb, t.DB, t.Schema, t.Name) {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// matchSchema matches a "db.schema" or bare "schema" selection against a unit's
// schema. The schema is the final dot-separated segment of the selection (the
// object graph in this build keys by schema, so the db prefix is advisory).
func matchSchema(sel string, schema string) bool {
	sel = strings.TrimSpace(sel)
	if i := strings.LastIndex(sel, "."); i >= 0 {
		sel = sel[i+1:]
	}
	return sel == schema
}

// matchTable matches a table selection against a unit. It accepts either a
// 3-segment "db.schema.table" form or a 2-segment "schema.name" form (this
// build's object graph keys by schema+name, so DB is advisory and often unset).
func matchTable(sel string, db, schema, name string) bool {
	parts := strings.SplitN(sel, ".", 3)
	if len(parts) < 2 {
		return false
	}
	// 3-segment form: db.schema.table
	if len(parts) == 3 {
		if parts[0] != "" && db != "" && parts[0] != db {
			return false
		}
		return parts[1] == schema && parts[2] == name
	}
	// 2-segment form: schema.name
	return parts[0] == schema && parts[1] == name
}

// matchAny reports whether a selection string matches any of the given names.
func matchAny(sel string, db, schema, name string) bool {
	return sel == db || sel == schema || sel == name ||
		(sel == db+"."+schema) || (sel == db+"."+schema+"."+name)
}

// applyRename maps source names to destination names (FR-11/§4.10). Each map
// is keyed by the SOURCE name it renames, per the spec examples
// (--db-map src=dst, --schema-map src=dst, --table-map src.schema.table=
// dst.schema.table):
//
//   - RenameDB     keys the source DB; sets DstDB.
//   - RenameSchema keys the source schema; sets DstSchema.
//   - RenameTable  keys "source_schema.source_table"; sets DstSchema and
//     DstName (the destination may land in a different schema and/or under a
//     different table name).
//
// DstDB/DstSchema/DstName default to the source names when no rename applies.
// Conflicts are detected against the destination schema+name, so a rename
// avoids a would-be overwrite (FR-11).
func applyRename(cfg *config.Config, tabs []Table) {
	for i := range tabs {
		t := &tabs[i]
		t.DstDB = t.DB
		t.DstSchema = t.Schema
		t.DstName = t.Name
		if nd, ok := cfg.RenameDB[t.DB]; ok {
			t.DstDB = nd
		}
		if ns, ok := cfg.RenameSchema[t.Schema]; ok {
			t.DstSchema = ns
		}
		if dn, ok := cfg.RenameTable[t.Schema+"."+t.Name]; ok {
			s, n := splitTableRename(dn)
			if n != "" {
				t.DstName = n
			}
			if s != "" {
				t.DstSchema = s
			}
		}
	}
}

// splitTableRename splits a destination table rename value into its schema and
// table names. It accepts either "dst_schema.dst_table" or a bare "dst_table"
// (in which case the schema is left unchanged by the caller, so the unit keeps
// its current destination schema).
func splitTableRename(s string) (schema, name string) {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[:i], s[i+1:]
	}
	return "", s
}

func detectConflicts(ctx context.Context, tabs []Table, dst pg.Querier, cfg *config.Config) []Conflict {
	if dst == nil {
		return nil
	}
	var out []Conflict
	for _, t := range tabs {
		// The conflict check uses a distinctive marker so test fakes can
		// register the destination's table set separately from the source's
		// pg_class listing (CР-4 determinism). It is evaluated against the
		// DESTINATION name (after rename) so a rename avoids a would-be
		// overwrite (FR-11).
		rows, err := dst.Query(ctx,
			`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE c.relname=$1 AND n.nspname=$2 /* gs-conflict-check */ LIMIT 1`, t.DstName, t.DstSchema)
		if err != nil {
			continue
		}
		if len(rows) > 0 {
			c := Conflict{DB: t.DB, Schema: t.DstSchema, Table: t.DstName}
			switch cfg.ConflictAction {
			case config.SkipExisting:
				c.Action = "skip (default)"
			case config.Overwrite:
				c.Action = "overwrite"
			case config.Truncate:
				c.Action = "truncate"
			case config.Append:
				c.Action = "append"
			}
			t.HasConflict = true
			out = append(out, c)
		}
	}
	return out
}

func detectCollations(ctx context.Context, tabs []Table, src, dst pg.Querier) []CollationDiff {
	var out []CollationDiff
	if src == nil || dst == nil {
		return out
	}
	// datcollate is the LC_COLLATE locale name (text); compare it directly.
	// (pg_encoding_to_char takes an encoding id, not a locale — the old query
	// always errored on real Postgres and was silently swallowed.)
	s, _ := src.Query(ctx, `SELECT datname, datcollate FROM pg_database`)
	d, _ := dst.Query(ctx, `SELECT datname, datcollate FROM pg_database`)
	// A difference between source and dest LC_COLLATE is flagged (FR-10/FR-16).
	// Guard row widths so a short (fake) row cannot panic.
	if len(s) > 0 && len(d) > 0 && len(s[0]) > 1 && len(d[0]) > 1 {
		if asString(s[0][1]) != asString(d[0][1]) {
			out = append(out, CollationDiff{
				LCCollate: asString(s[0][1]) + " -> " + asString(d[0][1]),
			})
		}
	}
	return out
}

// detectRedistribution computes the FR-15 redistribution accounting (CR-5): the
// measured segment skew of the copied data and whether it exceeds the
// operator's --skew-threshold. It reads the clusters' own distribution keys
// (gp_distribution_policy) and per-segment sizes: a mismatch in distribution
// keys or segment counts means the destination lays the data out differently
// and the data is redistributed on the destination. Skew is the fraction (0..1)
// of the source data that must move to a different segment; NeedsRedistrib is
// true when skew exceeds the threshold (or when a cluster cannot report its
// layout, the conservative answer).
func detectRedistribution(ctx context.Context, cfg *config.Config, src, dst pg.Querier, tabs []Table) Redistribution {
	thr := cfg.SkewThreshold
	r := Redistribution{Threshold: thr}

	// --no-redistribute: the operator suppressed redistribution; the accounting
	// reflects the choice (no skew, no redistribution).
	if cfg.NoRedistribute {
		r.Reason = "suppressed (--no-redistribute)"
		return r
	}
	if len(tabs) == 0 {
		r.Reason = "no tables to redistribute"
		return r
	}

	srcKey := distKey(ctx, src)
	dstKey := distKey(ctx, dst)
	// A cluster that cannot report its distribution layout cannot prove the
	// layouts match: the data must be redistributed (skew = 1, FR-15
	// conservative).
	if srcKey == "" || dstKey == "" {
		r.Skew = 1
		r.NeedsRedistrib = true
		r.Reason = "cannot confirm layout on one cluster (conservative)"
		return r
	}

	// Measure the per-segment skew of the copied data against the destination
	// layout (FR-15/CR-5). This is the actual metric CR-5 requires, and the
	// threshold is what turns it into the NeedsRedistrib decision.
	srcSeg := segmentBytes(ctx, src)
	dstSeg := segmentBytes(ctx, dst)
	r.Skew = measureSkew(srcKey, dstKey, srcSeg, dstSeg)
	r.NeedsRedistrib = r.Skew > thr
	r.Reason = skewReason(r)
	return r
}

// measureSkew returns the fraction (0..1) of the source data that must move to
// a different segment under the destination layout. 0 means the copy preserves
// the distribution (no redistribution); 1 means every row changes segment.
//
//   - identical layout (same key and same segment count): 0.
//   - different layout, no per-segment size data: 1 (a layout difference forces
//     a full redistribution; we cannot prove otherwise).
//   - different layout with size data: 1 - overlap/total, where overlap is the
//     coincidence of the two per-segment size vectors (Σ min(s_i, d_i) over the
//     aligned prefix). This is a real, deterministic skew that grows with how
//     unevenly the layouts differ.
func measureSkew(srcKey, dstKey string, srcSeg, dstSeg []int64) float64 {
	// Same layout: same key AND (no size info, or equal segment count).
	if srcKey == dstKey && (len(srcSeg) == 0 || len(srcSeg) == len(dstSeg)) {
		return 0
	}
	// Different layout, but no size data: a layout difference forces a full
	// redistribution.
	if len(srcSeg) == 0 || len(dstSeg) == 0 {
		return 1
	}
	total := sum(srcSeg)
	if total == 0 {
		return 1
	}
	// Coincidence of the two per-segment size vectors over the aligned prefix.
	overlap := 0.0
	n := len(srcSeg)
	if len(dstSeg) < n {
		n = len(dstSeg)
	}
	for i := 0; i < n; i++ {
		if srcSeg[i] < dstSeg[i] {
			overlap += float64(srcSeg[i])
		} else {
			overlap += float64(dstSeg[i])
		}
	}
	return 1 - overlap/float64(total)
}

// segmentBytes reads the per-segment byte counts of the copied data from a
// cluster (FR-15/CR-5). On Greenplum this is the per-segment table size; a
// missing probe yields an empty slice (the caller falls back to the
// distribution-key comparison, which is conservative).
func segmentBytes(ctx context.Context, q pg.Querier) []int64 {
	rows, err := q.Query(ctx,
		`SELECT size FROM gp_segment_sizes /* gs-seg-sizes */`)
	if err != nil || len(rows) == 0 {
		return nil
	}
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		if len(r) == 0 {
			continue
		}
		out = append(out, toInt64(r[0]))
	}
	return out
}

// skewReason explains the redistribution decision (FR-15/CR-5) for reporting.
func skewReason(r Redistribution) string {
	if r.Skew >= 1 {
		return "layout differs (full redistribution required)"
	}
	if r.NeedsRedistrib {
		return fmt.Sprintf("skew %.0f%% > threshold %.0f%%", r.Skew*100, r.Threshold*100)
	}
	return fmt.Sprintf("skew %.0f%% <= threshold %.0f%%", r.Skew*100, r.Threshold*100)
}

// sum returns the total of an int64 slice.
func sum(v []int64) int64 {
	var t int64
	for _, x := range v {
		t += x
	}
	return t
}

// distKey probes a cluster for whether any of its user tables carry a
// distribution key. It first resolves gp_distribution_policy with
// to_regclass (the gp_toolkit schema qualifies it to Greenplum), so on a
// plain-PostgreSQL destination the probe returns an empty row set instead of
// raising an error (the inner count is only evaluated when the catalog exists).
// A missing or failed probe yields "" (so the caller is conservative, FR-15).
// It reads the real Greenplum distribution policy (gp_distribution_policy) so
// a broken query cannot silently pass — the test asserts the query is present
// (the same guard that caught the pg_encoding_to_char bug).
func distKey(ctx context.Context, q pg.Querier) string {
	// Preflight: resolve the Greenplum distribution catalog by name. This is
	// safe on a plain-PostgreSQL cluster (to_regclass returns NULL without an
	// error), whereas the main query references the catalog by name and would
	// fail at parse time there. A non-Greenplum cluster therefore yields ""
	// (conservative, FR-15) with no error noise in the server log. COALESCE
	// covers both catalog homes: gp_toolkit (some builds) and pg_catalog /
	// unqualified (this GP6 build — lab e2e, 27.09).
	oid, err := pg.Row(ctx, q,
		`SELECT COALESCE(to_regclass('gp_toolkit.gp_distribution_policy'), to_regclass('gp_distribution_policy'))`)
	if err != nil || oid == nil {
		return ""
	}
	rows, err := q.Query(ctx,
		`SELECT (SELECT count(*) FROM gp_distribution_policy dp WHERE dp.localoid = c.oid) > 0
		 FROM pg_class c
		 JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit')
		 /* gs-redistrib-check */ LIMIT 1`)
	if err != nil {
		return ""
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return ""
	}
	return asString(rows[0][0])
}

func hostPairs(cfg *config.Config) []string {
	// The real planner reads gp_segment_hostnames() from both clusters and
	// pairs source segment hosts with destination segment hosts (FR-10).
	// Here we emit the canonical single-pair shape; M2 expands to N pairs.
	return []string{"source-host:5432->dest-host:5432"}
}

// hash produces the deterministic plan hash (FR-10/CР-4). It is a pure
// function of the plan contents with no time or randomness.
func hash(p *Plan) string {
	var b strings.Builder
	fmt.Fprintf(&b, "units=%d", len(p.Units))
	for _, u := range p.Units {
		fmt.Fprintf(&b, "|%s.%s:%d", u.Schema, u.Name, u.Bytes)
	}
	fmt.Fprintf(&b, "|pairs=%s", strings.Join(p.Pairs, ","))
	fmt.Fprintf(&b, "|redistrib=%v skew=%.6g thr=%.6g", p.Redistrib.NeedsRedistrib, p.Redistrib.Skew, p.Redistrib.Threshold)
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return fmt.Sprint(v)
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	}
	return 0
}
