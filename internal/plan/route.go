package plan

// route.go — M4: встраивание routeMode (T4b) в план (FR-03/FR-10) и
// cluster-level route-сводку для doctor/dry-run.
//
// Для каждой таблицы (unit) план определяет, как данные из source-кластера
// (S primary-сегментов) попадают в dest-кластер (D primary-сегментов):
// direct_1to1 (S==D, ключи распределения равны, одна мажорная версия — 0 CPU
// на hash) или router_full (per-row hash по K_dest, eligible-сужение =
// PairMap). Режимы и их условия — internal/route (M2/M3); fast-path fan-in и
// congruence опровергнуты (ANSWER-T4B-FASTPATH-DISPROVEN).
//
// Все запросы деградируют мягко: на plain-PostgreSQL (нет
// gp_distribution_policy / gp_segment_configuration) или на недоступном
// каталоге результат = "не определено" (RouteMode ""), а НЕ ошибка сборки —
// это консервативно (FR-15) и не ломает dry-run на M0-PoC (PG 9.4.26) и на
// фейках планировщика.

import (
	"context"
	"strconv"
	"strings"

	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/topology"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// RouteSummary — cluster-level route-сводка (для doctor/dry-run, "до старта"):
// сколько сегментов на каждой стороне, равны ли мажорные версии, сколько
// eligible-пар (s,d) доступно для данного (S,D) и возможен ли direct_1to1.
type RouteSummary struct {
	SrcSegments int32 `json:"src_segments"`
	DstSegments int32 `json:"dst_segments"`
	// GPVersionEqual — мажорные версии ядра src/dest совпадают (нужно для
	// direct_1to1: JCH одинаков только внутри одной мажорной ветки).
	GPVersionEqual bool `json:"gp_version_equal"`
	// EligiblePairs — число достижимых пар (s,d) для (S,D): S==D -> S;
	// иначе len(PairMap(S,D)); 0, если сегменты не определены.
	EligiblePairs int `json:"eligible_pairs"`
	// PossibleDirect1to1 — direct_1to1 достижим в принципе только при S==D.
	PossibleDirect1to1 bool `json:"possible_direct_1to1"`
	// Resolved — true, если сегменты обеих сторон определены (иначе сводка
	// неполная: на plain-PG).
	Resolved bool `json:"resolved"`
}

// routeLayout — распределение одной таблицы: тип + ключи.
type routeLayout struct {
	dist string   // "hash" | "randomly" | "replicated"
	keys []string // имена колонок распределения (пусто для randomly/replicated)
}

// clusterRoute — сегменты и мажорная версия стороны (src/dest).
type clusterRoute struct {
	segments int32 // 0 если не GP
	major    int   // server_version_num/10000; 0 если недоступно
}

// resolveRoutes вычисляет RouteSummary (cluster-level) и RouteMode per unit.
// Читает каталоги ОДИН раз и передаёт в per-table шаг. Мягко деградирует:
// любой сбой запроса -> RouteMode "" (не определено).
func resolveRoutes(ctx context.Context, src, dst pg.Querier, tabs []Table) RouteSummary {
	s := RouteSummary{}
	srcC := clusterRouteOf(ctx, src)
	dstC := clusterRouteOf(ctx, dst)
	s.SrcSegments = srcC.segments
	s.DstSegments = dstC.segments
	s.Resolved = s.SrcSegments > 0 && s.DstSegments > 0
	if !s.Resolved {
		// Не Greenplum (plain-PG) или каталог недоступен: route не определяется.
		return s
	}
	s.GPVersionEqual = srcC.major > 0 && srcC.major == dstC.major
	s.PossibleDirect1to1 = s.SrcSegments == s.DstSegments
	s.EligiblePairs = route.PairCount(s.SrcSegments, s.DstSegments)
	attachRouteMode(ctx, src, dst, srcC, dstC, s, tabs)
	return s
}

// attachRouteMode вычисляет RouteMode для каждой таблицы (unit).
func attachRouteMode(ctx context.Context, src, dst pg.Querier, srcC, dstC clusterRoute, sum RouteSummary, tabs []Table) {
	srcDist := tableDistMap(ctx, src)
	dstDist := tableDistMap(ctx, dst)
	for i := range tabs {
		t := &tabs[i]
		srcL, srcOK := srcDist[t.Schema+"."+t.Name]
		dstL, dstOK := dstDist[t.DstSchema+"."+t.DstName]
		if !srcOK || !dstOK {
			continue // не определено -> RouteMode остаётся ""
		}
		res, err := route.ResolveMode(route.ModeInput{
			SrcSegments:     sum.SrcSegments,
			DstSegments:     sum.DstSegments,
			SrcDistribution: srcL.dist,
			DstDistribution: dstL.dist,
			SrcHashKeys:     srcL.keys,
			DstHashKeys:     dstL.keys,
			GPMajorEqual:    sum.GPVersionEqual,
		})
		if err != nil {
			continue
		}
		t.RouteMode = res.Mode
		t.RoutePairs = len(res.Pairs)
	}
}

// clusterRouteOf — сегменты + мажорная версия стороны; 0 при недоступности.
func clusterRouteOf(ctx context.Context, q pg.Querier) clusterRoute {
	return clusterRoute{segments: segmentCount(ctx, q), major: routeMajor(ctx, q)}
}

// segmentCount — число логических (distinct content) сегментов; 0 если не GP.
func segmentCount(ctx context.Context, q pg.Querier) int32 {
	top, err := topology.Read(ctx, q)
	if err != nil || top == nil || !top.IsGreenplum() {
		return 0
	}
	return int32(top.SegmentCount())
}

// routeMajor — мажорная версия ядра (server_version_num / 10000); 0 если
// недоступно. 90426 -> 9 (GP6/PG9.4); 120001 -> 12 (GP7).
func routeMajor(ctx context.Context, q pg.Querier) int {
	v := pg.RowStr(ctx, q, `SELECT current_setting('server_version_num') /* gs-route-ver */`)
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0
	}
	return n / 10000
}

// tableDistMap — карта "schema.name" -> routeLayout для всех таблиц с
// gp_distribution_policy. Ошибка/отсутствие каталога -> пустая карта.
// Препроф-запрос COALESCE(gp_toolkit., unqualified): на этом GP6 каталог в
// pg_catalog (unqualified резолвится), на других сборках — в gp_toolkit.
func tableDistMap(ctx context.Context, q pg.Querier) map[string]routeLayout {
	m := map[string]routeLayout{}
	oid, err := pg.Row(ctx, q, `SELECT COALESCE(to_regclass('gp_toolkit.gp_distribution_policy'), to_regclass('gp_distribution_policy'))`)
	if err != nil || oid == nil {
		return m // plain-PG / каталог нет
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
