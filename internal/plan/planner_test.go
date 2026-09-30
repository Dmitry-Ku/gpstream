package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// twoIdenticalPlans returns two plans built from the same topology and asserts
// they are byte-identical (CR-4: two --dry-run on unchanged topology).
func twoIdenticalPlans(t *testing.T, cfg *config.Config, mk func() pg.Querier) {
	t.Helper()
	ctx := context.Background()
	p1, err := Build(ctx, cfg, mk(), mk())
	if err != nil {
		t.Fatalf("build 1: %v", err)
	}
	p2, err := Build(ctx, cfg, mk(), mk())
	if err != nil {
		t.Fatalf("build 2: %v", err)
	}
	if p1.Hash != p2.Hash {
		t.Errorf("plan hashes differ: %s != %s (plan must be deterministic)", p1.Hash, p2.Hash)
	}
}

// mkFake returns a fake that answers the planner queries with a fixed topology.
func mkFake() pg.Querier {
	f := pg.NewFakeConn()
	// pg_class: two tables in schema "s". Note: the collectTables query
	// contains both "pg_class" and "pg_namespace", so only "pg_class" is
	// registered here (registering pg_namespace too would make the
	// longest-match pick the wrong canned result).
	f.On("pg_class", [][]any{
		{"s", "orders", "r", int64(1000), int64(0), int64(1), "srcdb"},
		{"s", "audit", "r", int64(500), int64(0), int64(0), "srcdb"},
	})
	f.On("pg_database", [][]any{{"orders", "en_US.UTF-8"}})
	return f
}

func TestPlanDeterministic(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	twoIdenticalPlans(t, &cfg, mkFake)
}

func TestPlanVolumeAndConflict(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := mkFake()
	// dest has tables that conflict with the source; the conflict check uses
	// the "gs-conflict-check" marker so the fake can answer it separately
	// from the source's pg_class listing.
	dst := pg.NewFakeConn()
	dst.On("gs-conflict-check", [][]any{{int64(1)}})
	dst.On("pg_database", [][]any{{"orders", "C"}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if p.TotalBytes != 1500 {
		t.Errorf("total bytes = %d, want 1500", p.TotalBytes)
	}
	// The dest fake answers the conflict check for every source table, so
	// both tables are flagged (the fake cannot match on the per-table args).
	if len(p.Conflicts) != 2 {
		t.Errorf("conflicts = %d, want 2", len(p.Conflicts))
	}
	// FR-10 §8: the default (--resolve-conflicts stop) is STRICT —
	// the run stops on a conflict, so the dry-run verdict says the copy ends
	// in an error.
	if p.Conflicts[0].Action != "STOP (default: re-run with --resolve-conflicts skip|overwrite|truncate)" {
		t.Errorf("conflict action = %q, want the strict STOP verdict", p.Conflicts[0].Action)
	}
	if len(p.Pairs) != 1 {
		t.Errorf("pairs = %d, want 1", len(p.Pairs))
	}
	if p.Hash == "" {
		t.Errorf("hash is empty")
	}
}

// collationFakes returns a source and destination fake whose pg_database rows
// carry the given LC_COLLATE / LC_CTYPE locales in columns 2/3 for DB "orders".
func collationFakes(srcCollate, dstCollate string) (*pg.FakeConn, *pg.FakeConn) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"orders", srcCollate, srcCollate}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", dstCollate, dstCollate}})
	return src, dst
}

// TestDetectCollationsDiff verifies (FR-16) that a differing LC_COLLATE between
// source and destination is reported.
func TestDetectCollationsDiff(t *testing.T) {
	src, dst := collationFakes("en_US.UTF-8", "C.UTF-8")
	out := detectCollations(context.Background(), nil, src, dst, "")
	if len(out) != 1 {
		t.Fatalf("diffs = %d, want 1", len(out))
	}
	if got := out[0].LCCollate; got != "en_US.UTF-8 -> C.UTF-8" {
		t.Errorf("lc_collate = %q, want %q", got, "en_US.UTF-8 -> C.UTF-8")
	}
}

// TestDetectCollationsSame verifies that matching collations produce no diff.
func TestDetectCollationsSame(t *testing.T) {
	src, dst := collationFakes("en_US.UTF-8", "en_US.UTF-8")
	out := detectCollations(context.Background(), nil, src, dst, "")
	if len(out) != 0 {
		t.Errorf("diffs = %d, want 0", len(out))
	}
}

// TestDetectCollationsQuery is a regression guard for the pg_encoding_to_char
// bug: the LC_COLLATE/LC_CTYPE comparison MUST read datcollate/datctype
// directly (locale strings), not derive them via pg_encoding_to_char (which
// takes an encoding id and errored on every real cluster). The F-16 encoding
// check DOES use pg_encoding_to_char — but only on datencoding, never on
// datcollate/datctype. The fake answers any substring match, so without this
// guard the broken query would have silently passed.
func TestDetectCollationsQuery(t *testing.T) {
	src, dst := collationFakes("en_US.UTF-8", "C.UTF-8")
	detectCollations(context.Background(), nil, src, dst, "")
	for _, fc := range []*pg.FakeConn{src, dst} {
		for _, q := range fc.Queried() {
			if strings.Contains(q, "pg_encoding_to_char(datcollate)") ||
				strings.Contains(q, "pg_encoding_to_char(datctype)") {
				t.Errorf("collation derived via pg_encoding_to_char (broken): %q", q)
			}
		}
		if !containsQuery(fc, "datcollate") {
			t.Errorf("no query read datcollate; got: %v", fc.Queried())
		}
	}
}

// containsQuery reports whether any recorded query contains substr.
func containsQuery(fc *pg.FakeConn, substr string) bool {
	for _, q := range fc.Queried() {
		if strings.Contains(q, substr) {
			return true
		}
	}
	return false
}

// TestDetectCollationsTargetsCopiedDB is a regression guard for the first-row
// bug: detectCollations used to compare only the FIRST pg_database row on each
// side, which on a real cluster is the `postgres` service DB — so it mis-reported
// the DB and missed a real diff on the DB being copied. The planner must compare
// the collation of the DB the plan actually copies, not an arbitrary first row.
func TestDetectCollationsTargetsCopiedDB(t *testing.T) {
	// Source: `postgres` is C, the copied DB `appdb` is en_US.UTF-8.
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{
		{"postgres", "C", "C"},
		{"appdb", "en_US.UTF-8", "en_US.UTF-8"},
	})
	// Destination: `postgres` is en_US.UTF-8 (differs from source postgres!),
	// the copied DB `appdb` is C (differs from source appdb).
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{
		{"postgres", "en_US.UTF-8", "en_US.UTF-8"},
		{"appdb", "C", "C"},
	})
	tabs := []Table{{DB: "appdb", Schema: "s", Name: "orders", Bytes: 100}}
	out := detectCollations(context.Background(), tabs, src, dst, "")
	if len(out) != 1 {
		t.Fatalf("diffs = %d, want 1 (only the copied DB)", len(out))
	}
	if out[0].DB != "appdb" {
		t.Errorf("diff db = %q, want appdb (not the postgres service DB)", out[0].DB)
	}
	if out[0].LCCollate != "en_US.UTF-8 -> C" {
		t.Errorf("lc_collate = %q, want %q", out[0].LCCollate, "en_US.UTF-8 -> C")
	}
}

// TestDetectCollationsWholeClusterCoversAllDBs verifies that with no explicit
// table selection the planner compares every source DB (whole-cluster copy).
func TestDetectCollationsWholeClusterCoversAllDBs(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{
		{"dbA", "C", "C"},
		{"dbB", "C", "C"},
	})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{
		{"dbA", "C", "C"},
		{"dbB", "en_US.UTF-8", "en_US.UTF-8"},
	})
	// tabs empty -> whole-cluster: both source DBs must be compared.
	out := detectCollations(context.Background(), nil, src, dst, "")
	if len(out) != 1 {
		t.Fatalf("diffs = %d, want 1 (dbB)", len(out))
	}
	if out[0].DB != "dbB" {
		t.Errorf("diff db = %q, want dbB", out[0].DB)
	}
}

// mkRedistribFake returns a fake that answers the pg_class listing and the
// distribution-key probe ("gs-redistrib-check") with the given hasKey value.
// It is the fake used to exercise detectRedistribution (FR-15).
func mkRedistribFake(hasKey bool) *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{{"s", "orders", "r", int64(1000), int64(0), int64(1), "srcdb"}})
	// The preflight probe resolves the distribution catalog by name
	// (non-nil OID = Greenplum); the distribution probe returns a boolean.
	f.On("to_regclass('gp_toolkit.gp_distribution_policy')", [][]any{{int32(16384)}})
	f.On("gs-redistrib-check", [][]any{{hasKey}})
	return f
}

// TestRedistributionNeeded verifies (FR-15/CR-5) that differing distribution
// keys between source and destination are reported as a full redistribution
// (skew 1, since a layout difference with no size data forces a full move).
func TestRedistributionNeeded(t *testing.T) {
	src := mkRedistribFake(true)  // source has a distribution key
	dst := mkRedistribFake(false) // destination does not
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	r := detectRedistribution(context.Background(), src, dst, tabs)
	if r.Skew != 1 {
		t.Errorf("skew = %v, want 1 (different layout, no size data)", r.Skew)
	}
}

// TestRedistributionNotNeeded verifies (FR-15/CR-5) that identical distribution
// keys are reported with zero skew (no redistribution).
func TestRedistributionNotNeeded(t *testing.T) {
	src := mkRedistribFake(true)
	dst := mkRedistribFake(true)
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	r := detectRedistribution(context.Background(), src, dst, tabs)
	if r.Skew != 0 {
		t.Errorf("skew = %v, want 0 (identical layout)", r.Skew)
	}
}

// TestRedistributionConservative verifies (FR-15/CR-5) that when a cluster
// cannot report its distribution key, the plan conservatively reports
// redistribution with skew 1.
func TestRedistributionConservative(t *testing.T) {
	src := mkRedistribFake(true)
	// dst does not register the gs-redistrib-check probe, so its query errors
	// and distKey returns "" (conservative).
	dst := pg.NewFakeConn()
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	r := detectRedistribution(context.Background(), src, dst, tabs)
	if r.Skew != 1 {
		t.Errorf("skew = %v, want 1 (conservative)", r.Skew)
	}
}

// TestRedistributionQuery is a regression guard for the FR-15 accounting: the
// distribution probe must actually read the Greenplum distribution policy
// (gp_distribution_policy), not a broken/empty query. The fake answers by
// substring, so without this guard a broken query could silently pass. It
// mirrors the TestDetectCollationsQuery guard that caught pg_encoding_to_char.
func TestRedistributionQuery(t *testing.T) {
	src := mkRedistribFake(true)
	dst := mkRedistribFake(true)
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	detectRedistribution(context.Background(), src, dst, tabs)
	for _, fc := range []*pg.FakeConn{src, dst} {
		if !containsQuery(fc, "gs-redistrib-check") {
			t.Errorf("no gs-redistrib-check probe; got: %v", fc.Queried())
		}
		if !containsQuery(fc, "gp_distribution_policy") {
			t.Errorf("query did not read the real distribution policy; got: %v", fc.Queried())
		}
	}
}
