package plan

// route.go — M4: embedding routeMode (T4b) into the plan (FR-03/FR-10) and
// a cluster-level route summary for dry-run.
//
// For each table (unit) the plan determines how the data of the source cluster
// (S primary segments) lands in the destination cluster (D primary segments):
// direct_1to1 (S==D, distribution keys equal, one major version — 0 CPU
// spent on hashing) or router_full (per-row hash by K_dest, eligible narrowing =
// PairMap). Modes and their conditions — internal/route (M2/M3); fast-path fan-in and
// congruence were disproven (ANSWER-T4B-FASTPATH-DISPROVEN).
//
// All queries degrade gracefully: on plain PostgreSQL (no
// gp_distribution_policy / gp_segment_configuration) or on an unavailable
// catalog the result is "not determined" (RouteMode ""), and NOT a build error —
// this is conservative (FR-15) and does not break dry-run on M0-PoC (PG 9.4.26) and on
// planner fakes.

import (
	"context"
	"strconv"
	"strings"

	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/topology"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// RouteSummary — cluster-level route summary (for dry-run, "before start"):
// how many segments on each side, whether the major versions are equal, how many
// eligible (s,d) pairs are available for the given (S,D) and whether direct_1to1 is possible.
type RouteSummary struct {
	SrcSegments int32 `json:"src_segments"`
	DstSegments int32 `json:"dst_segments"`
	// GPVersionEqual — the major core versions of src/dest match (required for
	// direct_1to1: JCH is identical only within a single major branch).
	GPVersionEqual bool `json:"gp_version_equal"`
	// EligiblePairs — the number of reachable (s,d) pairs for (S,D): S==D -> S;
	// otherwise len(PairMap(S,D)); 0 if the segments are not determined.
	EligiblePairs int `json:"eligible_pairs"`
	// PossibleDirect1to1 — direct_1to1 is reachable in principle only when S==D.
	PossibleDirect1to1 bool `json:"possible_direct_1to1"`
	// Resolved — true if the segments of both sides are determined (otherwise the summary
	// is incomplete: on plain PG).
	Resolved bool `json:"resolved"`
	// LegacyHashops — either cluster runs with gp_use_legacy_hashops=on
	// (a post-upgrade mode): its cdbhash differs from the non-legacy JCH, so
	// segment-to-segment placement (direct_1to1 / router_full) is impossible
	// and the affected tables merge through the destination coordinator.
	LegacyHashops bool `json:"legacy_hashops,omitempty"`
}

// routeLayout — distribution of a single table: type + keys.
type routeLayout struct {
	dist string   // "hash" | "randomly" | "replicated"
	keys []string // distribution column names (empty for randomly/replicated)
}

// clusterRoute — segments, major version and hashops generation of one
// side (src/dest).
type clusterRoute struct {
	segments int32 // 0 if not GP
	major    int   // server_version_num/10000; 0 if unavailable
	legacy   bool  // gp_use_legacy_hashops=on (non-GP -> false)
}

// resolveRoutes computes the RouteSummary (cluster-level) and the RouteMode per unit.
// It reads the catalogs exactly ONCE and passes them to the per-table step. Degrades gracefully:
// any query failure -> RouteMode "" (not determined).
func resolveRoutes(ctx context.Context, src, dst pg.Querier, tabs []Table) RouteSummary {
	s := RouteSummary{}
	srcC := clusterRouteOf(ctx, src)
	dstC := clusterRouteOf(ctx, dst)
	s.SrcSegments = srcC.segments
	s.DstSegments = dstC.segments
	s.Resolved = s.SrcSegments > 0 && s.DstSegments > 0
	if !s.Resolved {
		// Not Greenplum (plain PG) or catalog unavailable: the route cannot be determined.
		return s
	}
	s.GPVersionEqual = srcC.major > 0 && srcC.major == dstC.major
	s.LegacyHashops = srcC.legacy || dstC.legacy
	s.PossibleDirect1to1 = s.SrcSegments == s.DstSegments
	s.EligiblePairs = route.PairCount(s.SrcSegments, s.DstSegments)
	attachRouteMode(ctx, src, dst, srcC, dstC, s, tabs)
	return s
}

// attachRouteMode computes the RouteMode for each table (unit).
func attachRouteMode(ctx context.Context, src, dst pg.Querier, srcC, dstC clusterRoute, sum RouteSummary, tabs []Table) {
	srcDist := tableDistMap(ctx, src)
	dstDist := tableDistMap(ctx, dst)
	for i := range tabs {
		t := &tabs[i]
		srcL, srcOK := srcDist[t.Schema+"."+t.Name]
		if !srcOK {
			continue // source layout unknown -> not determined
		}
		// Destination layout: use the destination table's actual policy when
		// it already exists. On a first copy the destination table does not
		// exist yet at plan time; the manager always creates it as a faithful
		// copy of the source's distribution (dist is read from the source and
		// passed to CreateTable/CreateStaging), so the source layout is the
		// destination's future layout. Without this fallback the fast-path
		// (direct_1to1) would never fire on a fresh migration — its primary
		// use case — because the plan is built before the dest table exists.
		dstL, dstOK := dstDist[t.DstSchema+"."+t.DstName]
		if !dstOK {
			dstL = srcL // first copy: dest will mirror the source's distribution
		}
		res, err := route.ResolveMode(route.ModeInput{
			SrcSegments:     sum.SrcSegments,
			DstSegments:     sum.DstSegments,
			SrcDistribution: srcL.dist,
			DstDistribution: dstL.dist,
			SrcHashKeys:     srcL.keys,
			DstHashKeys:     dstL.keys,
			GPMajorEqual:    sum.GPVersionEqual,
			LegacyHashops:   srcC.legacy || dstC.legacy,
		})
		if err != nil {
			continue
		}
		t.RouteMode = res.Mode
		t.RoutePairs = len(res.Pairs)
		t.SrcReplicated = srcL.dist == "replicated"
		if res.Mode == route.Merge {
			t.RouteReason = mergeReason(srcC, dstC, sum, srcL, dstL)
		}
	}
}

// mergeReason explains why a hash-distributed table cannot take a
// segment-to-segment sub-path ((a) 1:1 / (b) routed) and merges through the
// destination coordinator instead (design 04.10, question 5 + hash-mismatch
// guard). The manager logs it as routed_fallback / hash_mismatch_fallback.
func mergeReason(srcC, dstC clusterRoute, sum RouteSummary, srcL, dstL routeLayout) string {
	if srcC.legacy || dstC.legacy {
		return "hash-mismatch: legacy hashops (gp_use_legacy_hashops=on)"
	}
	if !sum.GPVersionEqual {
		return "hash-mismatch: different GP major versions"
	}
	if srcL.dist != "hash" || len(srcL.keys) == 0 {
		return "non-hash source distribution (" + srcL.dist + ")"
	}
	if dstL.dist != "hash" || len(dstL.keys) == 0 {
		return "non-hash destination distribution (" + dstL.dist + ")"
	}
	return "no deterministic segment placement"
}

// clusterRouteOf — segments + major version + hashops generation of one
// side; zero values when unavailable.
func clusterRouteOf(ctx context.Context, q pg.Querier) clusterRoute {
	return clusterRoute{
		segments: segmentCount(ctx, q),
		major:    routeMajor(ctx, q),
		legacy:   legacyHashops(ctx, q),
	}
}

// legacyHashops reports whether the cluster runs with
// gp_use_legacy_hashops=on (a post-upgrade mode whose cdbhash differs from
// the non-legacy JCH). On plain PostgreSQL the GUC does not exist and
// current_setting returns "" (missing_ok), so the answer is false.
func legacyHashops(ctx context.Context, q pg.Querier) bool {
	v := pg.RowStr(ctx, q, `SELECT current_setting('gp_use_legacy_hashops') /* gs-route-legacy */`)
	return strings.TrimSpace(v) == "on"
}

// segmentCount — the number of logical (distinct content) segments; 0 if not GP.
func segmentCount(ctx context.Context, q pg.Querier) int32 {
	top, err := topology.Read(ctx, q)
	if err != nil || top == nil || !top.IsGreenplum() {
		return 0
	}
	return int32(top.SegmentCount())
}

// routeMajor — the major core version (server_version_num / 10000); 0 if
// unavailable. 90426 -> 9 (GP6/PG9.4); 120001 -> 12 (GP7).
func routeMajor(ctx context.Context, q pg.Querier) int {
	v := pg.RowStr(ctx, q, `SELECT current_setting('server_version_num') /* gs-route-ver */`)
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0
	}
	return n / 10000
}

// tableDistMap — a "schema.name" -> routeLayout map for all tables with
// gp_distribution_policy. Catalog error/absence -> empty map.
// Preflight query COALESCE(gp_toolkit., unqualified): on this GP6 the catalog lives in
// pg_catalog (the unqualified name resolves), on other builds — in gp_toolkit.
func tableDistMap(ctx context.Context, q pg.Querier) map[string]routeLayout {
	m := map[string]routeLayout{}
	oid, err := pg.Row(ctx, q, `SELECT COALESCE(to_regclass('gp_toolkit.gp_distribution_policy'), to_regclass('gp_distribution_policy'))`)
	if err != nil || oid == nil {
		return m // plain PG / no catalog
	}
	rows, err := q.Query(ctx,
		`SELECT n.nspname, c.relname, dp.policytype::text,
		        (SELECT string_agg(a.attname, ',' ORDER BY k.pos)
		         FROM unnest(dp.distkey::int2[]) WITH ORDINALITY AS k(attnum, pos)
		         JOIN pg_attribute a ON a.attrelid = dp.localoid AND a.attnum = k.attnum) AS keys
		 FROM gp_distribution_policy dp
		 JOIN pg_class c ON c.oid = dp.localoid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit')
		   AND c.relkind IN ('r','p') /* gs-route-dist */`)
	if err != nil {
		return m
	}
	for _, r := range rows {
		if len(r) < 4 {
			continue
		}
		schema := asString(r[0])
		name := asString(r[1])
		ptype := asString(r[2])
		keys := asString(r[3])
		var k []string
		if keys != "" {
			k = strings.Split(keys, ",")
		}
		var dist string
		switch ptype {
		case "r":
			dist = "replicated"
		case "p":
			if len(k) == 0 {
				dist = "randomly"
			} else {
				dist = "hash"
			}
		default:
			continue
		}
		m[schema+"."+name] = routeLayout{dist: dist, keys: k}
	}
	return m
}
