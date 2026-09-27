package plan

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// gpFake returns a fake GP6 cluster: segs primary segments (content 0..segs-1)
// + coordinator, the given server_version_num, and one hash-distributed table
// s.orders with distribution key <key>. All route queries (M4) are registered:
// to_regclass probe (Greenplum present), segment catalog, gs-route-dist.
func gpFake(segs int, verNum, key string) *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{
		{"s", "orders", "r", int64(1000), int64(0), int64(1), "srcdb"},
	})
	f.On("pg_database", [][]any{{"orders", "en_US.UTF-8"}})
	f.On("to_regclass", [][]any{{int64(16384)}}) // probe -> OID (Greenplum)
	f.On("server_version_num", [][]any{{verNum}})
	segs_ := [][]any{{int64(-1), "p", "p", "n", "u", "coordinator", 5432, "dwh"}}
	for i := 0; i < segs; i++ {
		segs_ = append(segs_, []any{int64(i), "p", "p", "n", "u", "host", 6000, "dwh"})
	}
	f.On("FROM gp_segment_configuration", segs_)
	f.On("gs-route-dist", [][]any{{"s", "orders", "p", key}})
	f.On("gs-redistrib-check", [][]any{{"t"}})
	f.On("gs-seg-sizes", [][]any{{int64(500)}, {int64(500)}})
	return f
}

// TestRouteDirect1to1 verifies M4: S==D, equal hash keys, equal major ->
// per-unit direct_1to1 with S eligible pairs and a resolved cluster summary.
func TestRouteDirect1to1(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"

	p, err := Build(context.Background(), &cfg, gpFake(2, "90426", "id"), gpFake(2, "90426", "id"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(p.Units) != 1 {
		t.Fatalf("units = %d, want 1", len(p.Units))
	}
	u := p.Units[0]
	if u.RouteMode != route.Direct1to1 {
		t.Errorf("RouteMode = %q, want direct_1to1", u.RouteMode)
	}
	if u.RoutePairs != 2 {
		t.Errorf("RoutePairs = %d, want 2", u.RoutePairs)
	}
	rs := p.Route
	if !rs.Resolved || !rs.GPVersionEqual || !rs.PossibleDirect1to1 {
		t.Errorf("summary resolved=%v versionEqual=%v direct1to1=%v, want true/true/true",
			rs.Resolved, rs.GPVersionEqual, rs.PossibleDirect1to1)
	}
	if rs.SrcSegments != 2 || rs.DstSegments != 2 || rs.EligiblePairs != 2 {
		t.Errorf("summary segs=%d/%d pairs=%d, want 2/2/2", rs.SrcSegments, rs.DstSegments, rs.EligiblePairs)
	}
}

// TestRouteFullOnSegmentMismatch verifies M4: S!=D forces router_full even
// with equal keys (JCH is not modular — all SxD pairs eligible).
func TestRouteFullOnSegmentMismatch(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"

	p, err := Build(context.Background(), &cfg, gpFake(4, "90426", "id"), gpFake(2, "90426", "id"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	u := p.Units[0]
	if u.RouteMode != route.RouterFull {
		t.Errorf("RouteMode = %q, want router_full", u.RouteMode)
	}
	if u.RoutePairs != 8 { // SxD = 4x2
		t.Errorf("RoutePairs = %d, want 8", u.RoutePairs)
	}
	rs := p.Route
	if rs.PossibleDirect1to1 {
		t.Errorf("PossibleDirect1to1 = true for S=4 D=2, want false")
	}
	if rs.EligiblePairs != route.PairCount(4, 2) {
		t.Errorf("EligiblePairs = %d, want PairCount(4,2)=%d", rs.EligiblePairs, route.PairCount(4, 2))
	}
}

// TestRouteFullOnKeyMismatch verifies M4: S==D but different distribution
// keys -> router_full (the fast path requires equal keys).
func TestRouteFullOnKeyMismatch(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"

	p, err := Build(context.Background(), &cfg, gpFake(2, "90426", "id"), gpFake(2, "90426", "other"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if p.Units[0].RouteMode != route.RouterFull {
		t.Errorf("RouteMode = %q, want router_full (keys differ)", p.Units[0].RouteMode)
	}
	if p.Units[0].RoutePairs != 4 { // SxD = 2x2
		t.Errorf("RoutePairs = %d, want 4", p.Units[0].RoutePairs)
	}
}

// TestRouteFullOnMajorMismatch verifies M4: S==D, equal keys, but different
// kernel majors -> router_full (JCH is only identical within one major line).
func TestRouteFullOnMajorMismatch(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"

	p, err := Build(context.Background(), &cfg, gpFake(2, "90426", "id"), gpFake(2, "120001", "id"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if p.Units[0].RouteMode != route.RouterFull {
		t.Errorf("RouteMode = %q, want router_full (majors differ)", p.Units[0].RouteMode)
	}
	if p.Route.GPVersionEqual {
		t.Errorf("GPVersionEqual = true for 90426 vs 120001, want false")
	}
}

// TestRoutePlainPGDegrades verifies M4: a plain-PostgreSQL endpoint (no
// Greenplum catalogs) yields RouteMode "" (not resolvable) and an unresolved
// summary — never a build error.
func TestRoutePlainPGDegrades(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"

	p, err := Build(context.Background(), &cfg, mkFake(), mkFake())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(p.Units) != 2 {
		t.Fatalf("units = %d, want 2", len(p.Units))
	}
	for _, u := range p.Units {
		if u.RouteMode != "" {
			t.Errorf("unit %s: RouteMode = %q, want \"\" (plain PG)", u.Name, u.RouteMode)
		}
		if u.RoutePairs != 0 {
			t.Errorf("unit %s: RoutePairs = %d, want 0", u.Name, u.RoutePairs)
		}
	}
	if p.Route.Resolved {
		t.Errorf("summary Resolved = true on plain PG, want false")
	}
	if p.Route.EligiblePairs != 0 {
		t.Errorf("summary EligiblePairs = %d, want 0", p.Route.EligiblePairs)
	}
}

// TestRouteRandomlyDest verifies M4: a DISTRIBUTED RANDOMLY destination has
// no hash keys; the mode is still resolvable (router_full, all pairs) as long
// as the destination is a GP cluster — the actual landing is decided by the
// destination's own layout, not by the source hash.
func TestRouteRandomlyDest(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"

	src := gpFake(2, "90426", "id")
	dst := gpFake(2, "90426", "id")
	dst.On("gs-route-dist", [][]any{{"s", "orders", "p", ""}}) // randomly

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if p.Units[0].RouteMode != route.RouterFull {
		t.Errorf("RouteMode = %q, want router_full (randomly dest)", p.Units[0].RouteMode)
	}
	if !p.Route.Resolved {
		t.Errorf("summary Resolved = false, want true (both are GP)")
	}
}
