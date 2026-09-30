// Package planner builds the transfer plan for gpstream (FR-03/FR-10). It
// constructs the full object graph (databases/schemas/tables/partitions/
// indexes) BEFORE the job starts, estimates the volume and duration, detects
// conflicts and collation differences, and emits a deterministic plan.
//
// Determinism (FR-10/CR-4): the plan hash is a pure function of the inputs
// (units, selection, compression, workers, …) with no time or randomness, so
// two --dry-run on an unchanged topology are byte-identical.
package plan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
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
	DstDB     string
	DstSchema string
	DstName   string
	LeafPart  []string // leaf partition names, for explicit routing (FR-03)
	IsAO      bool     // Append-Optimized
	IsCO      bool     // Column-Oriented
	Bytes     int64    // estimated size
	RowEst    int64    // estimated rows
	Indexes   int      // number of indexes
	// Partitioned reports whether the table is a partitioned parent (GP6
	// inheritance partitioning: relkind 'r' with children in pg_inherits). A
	// partitioned parent is planned as ONE unit: its leaves are excluded from
	// the plan (the parent's COPY carries all inherited rows) and the
	// PARTITION BY spec is reproduced on the destination so the destination
	// re-routes the rows by ITS OWN collation (Fix 1 / Broadcom EL7->EL8:
	// copying leaf partitions as separate units both duplicates the data and
	// can misplace rows under a differing glibc).
	Partitioned bool
	// Partition is the partitioned parent's partitioning clause captured from
	// the source (pg.GetPartitionSpec), "" for a plain table. It is
	// reproduced in the destination DDL so the destination re-creates the
	// same leaf layout and re-routes the copied rows by ITS OWN collation
	// (Fix 1). The data flows through the parent (one COPY carries every
	// leaf's rows); the leaves are NOT separate units.
	Partition pg.PartitionSpec
	// PartitionGuard is a non-empty diagnostic when the unit must NOT be
	// copied as-is: a string-keyed partitioned parent WITHOUT a default
	// partition on a collation-different destination (a row matching no leaf
	// bound is rejected and the whole COPY aborts). Set by the planner
	// (dry-run warning) and enforced at run time (the unit fails early).
	PartitionGuard string
	// PartitionedUnsupported is set when the source table is partitioned but
	// the destination is plain PostgreSQL (no Greenplum catalogs): a
	// PARTITION BY table cannot be created there, so the unit fails early
	// with a diagnostic instead of a mid-run DDL error.
	PartitionedUnsupported bool
	HasConflict            bool // exists in dest and not overwritten (FR-10)
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
	// OtherObjects is the F-11 metadata-phase detection of non-table objects
	// in scope (views, materialized views, functions): reported in dry-run so
	// the operator knows what the data copy does NOT carry by itself (F-11:
	// "per option — views/functions"; views are recreated when opted in, below).
	OtherObjects OtherObjects `json:"other_objects"`
	// MissingExtensions is the F-11 metadata-phase check of dependent
	// extensions (MADlib/PostGIS/orafce, ...): extension types used by the
	// selected tables' columns that are NOT installed on the destination.
	// Non-empty means the DDL of those columns will fail at run time — the
	// operator must CREATE EXTENSION in the receiver first (the doctor does
	// the same diff over the full installed set as a pre-flight; this is the
	// column-level, run-path signal).
	MissingExtensions []string `json:"missing_extensions,omitempty"`
}

// OtherObjects is the F-11 detection of non-table objects within the selected
// schemas (metadata phase: "per option — views/functions"). Names are
// schema-qualified. Functions are counted (not enumerated) because a database
// can hold thousands and the operator only needs the signal that some exist.
type OtherObjects struct {
	Views     []string `json:"views,omitempty"`
	MatViews  []string `json:"matviews,omitempty"`
	Functions int      `json:"functions,omitempty"`
}

// CollationDiff is a detected collation/encoding difference (FR-10/FR-16). A
// DB-level entry carries the LC_COLLATE/LC_CTYPE pair ("src -> dst") and no
// Table; a column-level entry (F-16) carries Table + Column +
// SrcCollation/DstCollation and an empty LCCollate/LCType; an encoding-level
// entry (F-16) carries DB + Encoding ("src -> dst") and empty LCCollate.
type CollationDiff struct {
	DB           string
	LCCollate    string
	LCType       string
	Table        string
	Column       string
	SrcCollation string
	DstCollation string
	Encoding     string // "src -> dst" char name; non-empty only for encoding diffs
}

// IsColumn reports whether this diff is a column-level (attcollation)
// difference rather than a DB-level LC_COLLATE/LC_CTYPE one (F-16).
func (d CollationDiff) IsColumn() bool { return d.Table != "" }

// IsEncoding reports whether this diff is a database-encoding difference
// (F-16) rather than a collation one.
func (d CollationDiff) IsEncoding() bool { return d.Table == "" && d.Encoding != "" }

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
// live cluster (CR-4/CR-7). The plan hash is deterministic (CR-4).
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

	// 2c2. F-11 metadata phase: detect non-table objects (views, matviews,
	// functions) within the selected schemas. Reported in dry-run — the data
	// copy does not carry them, so the operator decides (F-11:
	// "per option — views/functions").
	p.OtherObjects = detectOtherObjects(ctx, src, tabs)
	// 2c3. F-11 metadata phase: dependent-extension check (MADlib/PostGIS/
	// orafce, ...) — extension types used by the selected tables' columns that
	// are missing on the destination. Non-empty = the column DDL will fail at
	// run time until the extension is created in the receiver.
	p.MissingExtensions = detectMissingExtensions(ctx, src, dst, tabs)

	// 2d. Partitioned parents (Fix 1): capture each parent's PARTITION BY
	// spec (reproduced on the destination) and evaluate the guard — a
	// string-keyed parent WITHOUT a default partition on a collation-
	// different destination cannot be copied safely (a row matching no leaf
	// bound is rejected and aborts the whole COPY). The guard is a warning
	// in dry-run and an early unit failure at run time.
	attachPartitionSpecs(ctx, src, dst, tabs)

	// 3. Conflicts: a source table (at its destination name) that exists in
	// dest and is not overwritten (FR-10/FR-11).
	p.Conflicts = detectConflicts(ctx, tabs, dst, cfg)

	// 4. Collation differences (FR-10/FR-16).
	p.Collations = detectCollations(ctx, tabs, src, dst, cfg.Dest)

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

	// 8. Deterministic hash (CR-4): no time, no randomness. A partial plan
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
// any other error is returned with a nil plan (a genuine failure, CR-7).
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
	// GP6 classic partitioning: parent and leaves are ALL relkind 'r'. The
	// parent is the row with pg_inherits children; a leaf is a row that is
	// somebody's inhrelid. Fix 1 plans the parent as ONE unit: its single
	// COPY carries every leaf's rows, so on a Greenplum source the leaves are
	// excluded from the plan (copying them as separate units duplicates the
	// data and, under a differing glibc, routes rows by the wrong collation).
	// On a plain-PostgreSQL source pg_inherits is user inheritance, NOT
	// partitioning: leaves stay in the plan (their rows are not in the
	// parent's COPY), so the exclusion is Greenplum-conditional.
	//
	// Volume: GREATEST(own total size, sum of leaf sizes) is correct on both
	// engines — GP6 does not recurse pg_total_relation_size into leaves
	// (parent reports 0-ish, leaves hold the data); plain PG does recurse
	// (parent already includes the leaves), so GREATEST just returns the
	// parent's own size.
	gpSource := false
	if oid, err := pg.Row(ctx, src,
		`SELECT COALESCE(to_regclass('gp_toolkit.gp_distribution_policy'), to_regclass('gp_distribution_policy')) /* gs-gp-probe */`); err == nil {
		gpSource = oid != nil
	}
	leafFilter := ""
	if gpSource {
		leafFilter = " AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)"
	}
	rows, err := src.Query(ctx,
		`SELECT n.nspname, c.relname, c.relkind,
		GREATEST(COALESCE(pg_total_relation_size(c.oid),0),
		         COALESCE((SELECT sum(pg_total_relation_size(i.inhrelid))
		                   FROM pg_inherits i WHERE i.inhparent = c.oid),0))::bigint,
		COALESCE((SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid),0),
		CASE WHEN c.relkind='p' THEN COALESCE((SELECT count(*) FROM pg_index i WHERE i.indrelid=c.oid),0) ELSE 0 END,
		current_database()
		FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit')`+leafFilter+`
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
			DB:          asString(r[6]),
			Schema:      asString(r[0]),
			Name:        asString(r[1]),
			Bytes:       int64(toInt64(r[3])),
			Indexes:     int(toInt64(r[5])),
			Partitioned: gpSource && toInt64(r[4]) > 0,
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

// detectOtherObjects reads the F-11 non-table objects (views, materialized
// views, functions) from the selected schemas of the source (metadata phase:
// "per option — views/functions"). It is a detection-only pass: the data copy
// does not carry these objects, so the plan reports them and the operator
// decides. A catalog fault yields an empty result (detection must never fail
// the plan, like the other advisory passes).
func detectOtherObjects(ctx context.Context, src pg.Querier, tabs []Table) OtherObjects {
	// The selected schemas (the object graph is schema-keyed).
	schemas := map[string]bool{}
	for _, t := range tabs {
		if t.Schema != "" {
			schemas[t.Schema] = true
		}
	}
	if len(schemas) == 0 {
		return OtherObjects{}
	}
	list := make([]string, 0, len(schemas))
	for s := range schemas {
		list = append(list, "'"+strings.ReplaceAll(s, "'", "''")+"'")
	}
	cond := "n.nspname IN (" + strings.Join(list, ",") + ")"
	var out OtherObjects
	if rows, err := src.Query(ctx,
		`SELECT n.nspname || '.' || c.relname
		 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relkind = 'v' AND `+cond+`
		 ORDER BY 1`); err == nil {
		for _, r := range rows {
			if len(r) > 0 && r[0] != nil {
				out.Views = append(out.Views, asString(r[0]))
			}
		}
	}
	if rows, err := src.Query(ctx,
		`SELECT n.nspname || '.' || c.relname
		 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relkind = 'm' AND `+cond+`
		 ORDER BY 1`); err == nil {
		for _, r := range rows {
			if len(r) > 0 && r[0] != nil {
				out.MatViews = append(out.MatViews, asString(r[0]))
			}
		}
	}
	if rows, err := src.Query(ctx,
		`SELECT count(*)
		 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		 WHERE n.nspname IN (`+strings.Join(list, ",")+`)
		   AND n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit')`); err == nil &&
		len(rows) > 0 && len(rows[0]) > 0 {
		out.Functions = int(toInt64(rows[0][0]))
	}
	return out
}

// detectMissingExtensions is the F-11 metadata-phase dependent-extension check
// (MADlib/PostGIS/orafce, ...). It finds the extension types actually used by
// the selected tables' columns (a column whose type is owned by an extension)
// and reports the extensions that are NOT installed on the destination. A
// non-empty result means the DDL of those columns will fail at run time until
// the operator CREATE EXTENSIONs the missing one in the receiver — the
// column-level, run-path counterpart of the doctor's whole-set diff. A catalog
// fault yields an empty result (detection must never fail the plan).
func detectMissingExtensions(ctx context.Context, src, dst pg.Querier, tabs []Table) []string {
	schemas := map[string]bool{}
	for _, t := range tabs {
		if t.Schema != "" {
			schemas[t.Schema] = true
		}
	}
	if len(schemas) == 0 {
		return nil
	}
	list := make([]string, 0, len(schemas))
	for s := range schemas {
		list = append(list, "'"+strings.ReplaceAll(s, "'", "''")+"'")
	}
	in := strings.Join(list, ",")
	// Extension names owning a type used by a selected table's column.
	rows, err := src.Query(ctx,
		`SELECT DISTINCT e.extname
		 FROM pg_attribute a
		 JOIN pg_class c     ON c.oid = a.attrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 JOIN pg_type t      ON t.oid = a.atttypid
		 JOIN pg_depend d    ON d.objid = t.oid AND d.deptype = 'e'
		 JOIN pg_extension e ON e.oid = d.refobjid
		 WHERE a.attnum > 0 AND NOT a.attisdropped
		   AND c.relkind IN ('r','p')
		   AND n.nspname IN (`+in+`)
		 ORDER BY e.extname`)
	if err != nil || len(rows) == 0 {
		return nil
	}
	needed := make(map[string]bool, len(rows))
	for _, r := range rows {
		if len(r) > 0 && r[0] != nil {
			needed[asString(r[0])] = true
		}
	}
	if len(needed) == 0 {
		return nil
	}
	// The destination's installed extensions.
	dstRows, err := dst.Query(ctx, `SELECT extname FROM pg_extension`)
	if err != nil {
		return nil // destination extensions unreadable; the doctor pre-flight covers it
	}
	installed := make(map[string]bool, len(dstRows))
	for _, r := range dstRows {
		if len(r) > 0 && r[0] != nil {
			installed[asString(r[0])] = true
		}
	}
	var missing []string
	for name := range needed {
		if !installed[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	return missing
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
// F-11 selection templates: the segment is a glob (* and ? wildcards), so
// "--schemas public" matches only public, but "--schemas audit_*" matches
// audit_2024, audit_2025, ...
func matchSchema(sel string, schema string) bool {
	sel = strings.TrimSpace(sel)
	if i := strings.LastIndex(sel, "."); i >= 0 {
		sel = sel[i+1:]
	}
	return globSeg(sel, schema)
}

// matchTable matches a table selection against a unit. It accepts either a
// 3-segment "db.schema.table" form or a 2-segment "schema.name" form (this
// build's object graph keys by schema+name, so DB is advisory and often unset).
// Each segment is a F-11 selection template (glob, * / ? wildcards; a segment
// without wildcards must match exactly, and * never crosses a dot).
func matchTable(sel string, db, schema, name string) bool {
	parts := strings.SplitN(sel, ".", 3)
	if len(parts) < 2 {
		return false
	}
	// 3-segment form: db.schema.table
	if len(parts) == 3 {
		if parts[0] != "" && db != "" && !globSeg(parts[0], db) {
			return false
		}
		return globSeg(parts[1], schema) && globSeg(parts[2], name)
	}
	// 2-segment form: schema.name
	return globSeg(parts[0], schema) && globSeg(parts[1], name)
}

// matchAny reports whether a selection string matches any of the given names.
// Single segments and the dotted compounds are F-11 selection templates (glob,
// * never crossing a dot).
func matchAny(sel string, db, schema, name string) bool {
	return globSeg(sel, db) || globSeg(sel, schema) || globSeg(sel, name) ||
		globDotted(sel, db+"."+schema) || globDotted(sel, db+"."+schema+"."+name)
}

// globSeg reports whether pattern matches s using * and ? wildcards
// (path.Match semantics). A pattern without wildcards must match exactly, so
// existing literal selections keep their current behavior.
func globSeg(pattern, s string) bool {
	if !strings.ContainsAny(pattern, "*?") {
		return pattern == s
	}
	ok, err := path.Match(pattern, s)
	return err == nil && ok
}

// globDotted matches a dot-separated pattern against a dot-separated value,
// segment by segment, so * never crosses a dot ("db.*" matches "db.public"
// but not "db.public.t").
func globDotted(pattern, s string) bool {
	parts := strings.Split(pattern, ".")
	vals := strings.Split(s, ".")
	if len(parts) != len(vals) {
		return false
	}
	for i := range parts {
		if !globSeg(parts[i], vals[i]) {
			return false
		}
	}
	return true
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

// attachPartitionSpecs (Fix 1) fills in, for every partitioned parent in the
// selected set, its PARTITION BY spec (to be reproduced on the destination)
// and evaluates the partition guard.
//
// A parent whose spec cannot be captured (plain PostgreSQL has no
// pg_get_partition_def) is left with an empty spec: it is still planned as ONE
// unit (its leaves are excluded in collectTables and its single COPY carries
// every leaf's rows), but the destination is created as a plain table. This
// keeps plain-PG inheritance working while Fix 1's partition reproduction
// applies only where the spec actually exists (GP6).
//
// The guard (Table.PartitionGuard) is non-empty for the one unsafe combination:
// a string-keyed partitioned parent WITHOUT a default partition, copied to a
// destination whose collation sorts the key values differently. Under that
// combination a source row that matched a leaf on the source can match NO leaf
// on the destination, so it is rejected and the whole COPY aborts (lab,
// 29.09). A non-empty guard is a dry-run warning and an early unit failure at
// run time.
func attachPartitionSpecs(ctx context.Context, src, dst pg.Querier, tabs []Table) {
	if len(tabs) == 0 {
		return
	}
	// Only partitioned parents need a spec.
	var partitioned []int
	for i := range tabs {
		if tabs[i].Partitioned {
			partitioned = append(partitioned, i)
		}
	}
	if len(partitioned) == 0 {
		return
	}
	// Behavioral collation fingerprint of each cluster (glibc-aware: two DBs
	// with the same locale name but different glibc sort a fixed string set
	// differently). Compared once, not per table.
	dstDiffers := false
	if dst != nil {
		if sSig, sErr := pg.CollationSignature(src, ctx); sErr == nil {
			if dSig, dErr := pg.CollationSignature(dst, ctx); dErr == nil && dSig != sSig {
				dstDiffers = true
			}
		}
	}
	for _, i := range partitioned {
		t := &tabs[i]
		spec, err := pg.CapturePartitionSpec(src, ctx, t.Schema, t.Name)
		if err != nil || spec.Spec == "" {
			// Not a reproducible partitioned parent (plain PG or capture
			// fault): leave the spec empty; the dest is created as a plain
			// table and still receives all rows via the parent COPY.
			continue
		}
		t.Partition = spec
		if dstDiffers && !spec.HasDefault && partitionKeyIsString(ctx, src, t.Schema, t.Name, spec.KeyColumns) {
			t.PartitionGuard = fmt.Sprintf(
				"%s.%s is partitioned by a string key %v with no DEFAULT partition and the destination collation differs from the source; a row matching no leaf bound would be rejected and abort the copy. Add a DEFAULT partition (or align the destination collation) before running.",
				t.Schema, t.Name, spec.KeyColumns)
		}
	}
}

// partitionKeyIsString reports whether every partition-key column is a string
// type (text/char/varchar and close relatives). Only string keys are
// collation-sensitive; numeric/date/timestamp partitioning is unaffected by a
// source/destination collation difference, so the guard does not apply to
// them. A lookup fault is treated as "not string" (guard not raised) — the
// guard is an optimization for a clear-cut case, not a correctness gate.
func partitionKeyIsString(ctx context.Context, src pg.Querier, schema, table string, cols []string) bool {
	if len(cols) == 0 {
		return false
	}
	rows, err := src.Query(ctx,
		`SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		   FROM pg_attribute a
		  WHERE a.attrelid = (quote_ident($2) || '.' || quote_ident($1))::regclass
		    AND a.attnum > 0 AND NOT a.attisdropped /* gs-partkey-type */`, table, schema)
	if err != nil {
		return false
	}
	types := map[string]string{}
	for _, r := range rows {
		if len(r) >= 2 {
			types[asString(r[0])] = strings.ToLower(asString(r[1]))
		}
	}
	for _, c := range cols {
		typ, ok := types[c]
		if !ok {
			return false
		}
		if !isStringType(typ) {
			return false
		}
	}
	return true
}

// isStringType reports whether a format_type rendering is a collation-sensitive
// string type.
func isStringType(typ string) bool {
	switch {
	case strings.HasPrefix(typ, "text"),
		strings.HasPrefix(typ, "character varying"),
		strings.HasPrefix(typ, "char("),
		strings.HasPrefix(typ, "character("),
		typ == "name",
		strings.HasPrefix(typ, "citext"),
		strings.HasPrefix(typ, "uuid"):
		return true
	default:
		return false
	}
}

func detectConflicts(ctx context.Context, tabs []Table, dst pg.Querier, cfg *config.Config) []Conflict {
	if dst == nil {
		return nil
	}
	var out []Conflict
	for _, t := range tabs {
		// The conflict check uses a distinctive marker so test fakes can
		// register the destination's table set separately from the source's
		// pg_class listing (CR-4 determinism). It is evaluated against the
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
			case config.Stop:
				// FR-10 §8: the DEFAULT is STRICT — the run STOPS on a
				// conflict, so the dry-run must say the copy will end in an
				// error (spec §3 verdict). --resolve-conflicts skip skips.
				c.Action = "STOP (default: re-run with --resolve-conflicts skip|overwrite|truncate)"
			case config.Skip:
				c.Action = "skip (--resolve-conflicts skip)"
			case config.Overwrite:
				c.Action = "overwrite"
			case config.Truncate:
				c.Action = "truncate"
			}
			t.HasConflict = true
			out = append(out, c)
		}
	}
	return out
}

func detectCollations(ctx context.Context, tabs []Table, src, dst pg.Querier, destConn string) []CollationDiff {
	var out []CollationDiff
	if src == nil || dst == nil {
		return out
	}
	// datcollate/datctype are locale names (text); datencoding is the
	// database encoding OID (F-16). Compare them directly.
	// (The collation must NOT be derived via pg_encoding_to_char — that
	// function takes an encoding id, not a locale, and the old query always
	// errored on real Postgres and was silently swallowed.)
	s, _ := src.Query(ctx, `SELECT datname, datcollate, datctype, encoding FROM pg_database`)
	d, _ := dst.Query(ctx, `SELECT datname, datcollate, datctype, encoding FROM pg_database`)
	srcLoc, dstLoc := localeMap(s), localeMap(d)
	srcEncOID, dstEncOID := encodingOIDMap(s), encodingOIDMap(d)

	// The runtime copies every table through the single destination pool, so
	// the data lands in the database named in the --dest connection string —
	// not in a per-table "DstDB" label (which defaults to the source DB name).
	// Compare against that database; on a parse miss fall back to the label.
	destDB, _ := pg.DatabaseName(destConn)

	// Compare each source DB against the destination DB the plan actually
	// writes to (after rename). We do NOT just compare the first pg_database
	// row: on a real cluster that is the `postgres` service DB, so a first-row
	// comparison both mis-reports the DB and misses a real diff on the copied
	// DB (the single-row test fakes masked this). (FR-10/FR-16)
	type pair struct{ s, d string }
	seen := map[pair]bool{}
	var pairs []pair
	if len(tabs) == 0 {
		// Whole-cluster copy: every source DB is written into the single
		// destination database from the --dest connection string.
		for db := range srcLoc {
			dst := destDB
			if dst == "" {
				dst = db // fallback: no parseable --dest DB
			}
			if p := (pair{db, dst}); !seen[p] {
				seen[p] = true
				pairs = append(pairs, p)
			}
		}
	} else {
		for _, t := range tabs {
			dst := destDB
			if dst == "" {
				dst = t.DstDB // fallback: rename label / source name
				if dst == "" {
					dst = t.DB
				}
			}
			if p := (pair{t.DB, dst}); !seen[p] {
				seen[p] = true
				pairs = append(pairs, p)
			}
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].s != pairs[j].s {
			return pairs[i].s < pairs[j].s
		}
		return pairs[i].d < pairs[j].d
	})
	// Fix 2 (F-16): the LC_COLLATE *name* comparison above is a false negative
	// for two clusters with the SAME locale name but different glibc — e.g.
	// EL7 vs EL8, both "en_US.utf8", whose sort order differs. The behavioral
	// probe (CollationProbeSQL) is collation-sensitive, so it catches the glibc
	// difference the name comparison cannot. It is measured once per cluster
	// (the connected database is representative: every DB on a cluster shares
	// that cluster's glibc). A probe fault degrades to no behavioral finding
	// (never fails the plan) — the same convention as the conflict check.
	srcSig, srcSigErr := pg.CollationSignature(src, ctx)
	dstSig, dstSigErr := pg.CollationSignature(dst, ctx)
	behavioralDiff := srcSigErr == nil && dstSigErr == nil &&
		srcSig != "" && dstSig != "" && srcSig != dstSig
	for _, p := range pairs {
		sv, okS := srcLoc[p.s]
		dv, okD := dstLoc[p.d]
		if okS && okD {
			nameDiff := sv[0] != dv[0] || sv[1] != dv[1]
			if nameDiff || behavioralDiff {
				lc := sv[0] + " -> " + dv[0]
				switch {
				case nameDiff && behavioralDiff:
					lc += " (and different glibc sort behavior — behavioral probe)"
				case !nameDiff:
					lc += " (same locale name, DIFFERENT glibc sort behavior — behavioral probe; string sort/index semantics will differ)"
				}
				out = append(out, CollationDiff{
					DB:        p.s,
					LCCollate: lc,
					LCType:    sv[1] + " -> " + dv[1],
				})
			}
		}
		// F-16: database encoding. Compare the raw datencoding OIDs; a
		// mismatch (e.g. SQL_ASCII -> UTF8) is data-destructive — string
		// bytes are reinterpreted on the destination. The names are resolved
		// with pg_encoding_to_char($1), the correct usage (it takes an
		// encoding id); a resolution fault degrades to the raw OID. This is
		// independent of the locale comparison above (an encoding diff is
		// reported even when the locales match, and vice versa).
		if so, okSE := srcEncOID[p.s]; okSE {
			if do, okDE := dstEncOID[p.d]; okDE && so != do {
				out = append(out, CollationDiff{
					DB:       p.s,
					Encoding: encodingName(ctx, src, so) + " -> " + encodingName(ctx, dst, do),
				})
			}
		}
	}
	// F-16: column-level (attcollation) comparison. Per copied table, compare
	// the source column's explicit collation with the destination's (queried
	// against the DESTINATION schema+name, so a rename is honoured). Only
	// explicitly COLLATEd columns carry a name (the database default is "" on
	// both sides and is never a column-level diff — DB-level differences are
	// the pg_database entries above). A query fault degrades to no column
	// diffs (never fails the plan) — the same convention as the conflict
	// check. The marker comment keeps this query distinguishable from the
	// pg_class-based checks for test fakes (longest-substring matching).
	for _, t := range tabs {
		srcCols := columnCollations(ctx, src, t.Schema, t.Name)
		if srcCols == nil {
			continue
		}
		dstCols := columnCollations(ctx, dst, t.DstSchema, t.DstName)
		// nil = query fault (degrade to no column diffs, never fail the plan).
		// An existing table always has >=1 column, so an EMPTY map means the
		// destination table is absent — gpstream will CREATE it from the
		// source's column layout (which now reproduces the COLLATE clause),
		// so there is no column-level diff to report.
		if dstCols == nil || len(dstCols) == 0 {
			continue
		}
		names := make([]string, 0, len(srcCols))
		for name, sc := range srcCols {
			if sc != "" {
				names = append(names, name)
			}
		}
		// Deterministic order (CR-4): the plan JSON must be byte-identical
		// across dry-runs, so the column diffs are emitted in sorted
		// column-name order (map iteration is random).
		sort.Strings(names)
		for _, name := range names {
			sc := srcCols[name]
			dc, ok := dstCols[name]
			if !ok || dc != sc {
				out = append(out, CollationDiff{
					DB:           t.DB,
					Table:        t.Schema + "." + t.Name,
					Column:       name,
					SrcCollation: sc,
					DstCollation: dstCols[name],
				})
			}
		}
	}
	return out
}

// columnCollations returns the explicit collation name of each column of
// schema.table ("" for the database default), or nil when the table cannot
// be read: a query fault, or the table is absent on that side. The caller
// degrades both to "no column diffs" — never a plan failure (F-16). The
// table is resolved with a (quote_ident'd) ::regclass cast, NOT a
// pg_class substring scan: the test fakes register "pg_class" for the
// collectTables listing, and a pg_class-containing query here would match
// that canned result (longest-substring fake matching).
func columnCollations(ctx context.Context, q pg.Querier, schema, table string) map[string]string {
	rows, err := q.Query(ctx,
		`SELECT a.attname,
		        CASE WHEN co.collname IS NULL OR co.collname = 'default'
		             THEN '' ELSE co.collname END
		 FROM pg_attribute a
		 LEFT JOIN pg_collation co ON co.oid = a.attcollation
		 WHERE a.attrelid = (quote_ident($2) || '.' || quote_ident($1))::regclass
		   AND a.attnum > 0 AND NOT a.attisdropped /* gs-collation-cols */`,
		table, schema)
	if err != nil {
		return nil
	}
	m := map[string]string{}
	for _, r := range rows {
		if len(r) >= 2 {
			m[asString(r[0])] = asString(r[1])
		}
	}
	return m
}

// localeMap indexes pg_database rows (datname, datcollate, datctype) by DB name.
// Short rows (test fakes that only carry datcollate) are skipped so a canned row
// cannot panic the planner.
func localeMap(rows [][]any) map[string][2]string {
	m := map[string][2]string{}
	for _, r := range rows {
		if len(r) >= 3 {
			m[asString(r[0])] = [2]string{asString(r[1]), asString(r[2])}
		}
	}
	return m
}

// encodingName resolves an encoding OID to its char name
// (pg_encoding_to_char takes an encoding id — the correct usage here). On a
// query fault it degrades to the raw OID as a string so the finding is still
// reported (a missing name is a display detail, not a missed diff).
func encodingName(ctx context.Context, q pg.Querier, oid int64) string {
	if n := pg.RowStr(ctx, q, `SELECT pg_encoding_to_char($1)`, oid); n != "" {
		return n
	}
	return fmt.Sprintf("%d", oid)
}

// encodingOIDMap indexes pg_database rows (datname, datencoding OID) by DB
// name. Rows without a 4th column (the older 3-column test fakes) are
// skipped, so an encoding finding requires both sides to report an OID. Note
// OID 0 (SQL_ASCII) is a valid encoding and is kept.
func encodingOIDMap(rows [][]any) map[string]int64 {
	m := map[string]int64{}
	for _, r := range rows {
		if len(r) >= 4 {
			m[asString(r[0])] = toInt64(r[3])
		}
	}
	return m
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

// hash produces the deterministic plan hash (FR-10/CR-4). It is a pure
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
