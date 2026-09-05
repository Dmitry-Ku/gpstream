package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/pg"
)

// twoIdenticalPlans returns two plans built from the same topology and asserts
// they are byte-identical (CР-4: two --dry-run on unchanged topology).
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
		{"s", "orders", "r", int64(1000), int64(0), int64(1)},
		{"s", "audit", "r", int64(500), int64(0), int64(0)},
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
	if p.Conflicts[0].Action != "skip (default)" {
		t.Errorf("conflict action = %q, want skip (default)", p.Conflicts[0].Action)
	}
	if len(p.Pairs) != 1 {
		t.Errorf("pairs = %d, want 1", len(p.Pairs))
	}
	if p.Hash == "" {
		t.Errorf("hash is empty")
	}
}

// collationFakes returns a source and destination fake whose pg_database rows
// carry the given LC_COLLATE locale in column 2.
func collationFakes(srcCollate, dstCollate string) (*pg.FakeConn, *pg.FakeConn) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"orders", srcCollate}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", dstCollate}})
	return src, dst
}

// TestDetectCollationsDiff verifies (FR-16) that a differing LC_COLLATE between
// source and destination is reported.
func TestDetectCollationsDiff(t *testing.T) {
	src, dst := collationFakes("en_US.UTF-8", "C.UTF-8")
	out := detectCollations(context.Background(), nil, src, dst)
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
	out := detectCollations(context.Background(), nil, src, dst)
	if len(out) != 0 {
		t.Errorf("diffs = %d, want 0", len(out))
	}
}

// TestDetectCollationsQuery is a regression guard for the pg_encoding_to_char
// bug: the collation check MUST read datcollate directly (a locale string),
// not pg_encoding_to_char (which takes an encoding id and errored on every
// real cluster). The fake answers any substring match, so without this guard
// the broken query would have silently passed.
func TestDetectCollationsQuery(t *testing.T) {
	src, dst := collationFakes("en_US.UTF-8", "C.UTF-8")
	detectCollations(context.Background(), nil, src, dst)
	for _, fc := range []*pg.FakeConn{src, dst} {
		for _, q := range fc.Queried() {
			if strings.Contains(q, "pg_encoding_to_char") {
				t.Errorf("query uses pg_encoding_to_char (broken): %q", q)
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

// mkRedistribFake returns a fake that answers the pg_class listing and the
// distribution-key probe ("gs-redistrib-check") with the given hasKey value.
// It is the fake used to exercise detectRedistribution (FR-15).
func mkRedistribFake(hasKey bool) *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{{"s", "orders", "r", int64(1000), int64(0), int64(1)}})
	// The distribution probe returns a boolean in column 0.
	f.On("gs-redistrib-check", [][]any{{hasKey}})
	return f
}

// TestRedistributionNeeded verifies (FR-15/CR-5) that differing distribution
// keys between source and destination are flagged as requiring redistribution,
// with a measured skew of 1 (a layout difference forces a full redistribute).
func TestRedistributionNeeded(t *testing.T) {
	cfg := config.Default()
	src := mkRedistribFake(true)  // source has a distribution key
	dst := mkRedistribFake(false) // destination does not
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	r := detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	if !r.NeedsRedistrib {
		t.Errorf("expected redistribution needed (src key != dst key)")
	}
	if r.Skew != 1 {
		t.Errorf("skew = %v, want 1 (different layout, no size data)", r.Skew)
	}
}

// TestRedistributionNotNeeded verifies (FR-15/CR-5) that identical distribution
// keys are reported as not requiring redistribution, with zero skew.
func TestRedistributionNotNeeded(t *testing.T) {
	cfg := config.Default()
	src := mkRedistribFake(true)
	dst := mkRedistribFake(true)
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	r := detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	if r.NeedsRedistrib {
		t.Errorf("expected no redistribution (identical distribution keys)")
	}
	if r.Skew != 0 {
		t.Errorf("skew = %v, want 0 (identical layout)", r.Skew)
	}
}

// TestRedistributionConservative verifies (FR-15/CR-5) that when a cluster
// cannot report its distribution key, the plan conservatively reports
// redistribution with skew 1.
func TestRedistributionConservative(t *testing.T) {
	cfg := config.Default()
	src := mkRedistribFake(true)
	// dst does not register the gs-redistrib-check probe, so its query errors
	// and distKey returns "" (conservative).
	dst := pg.NewFakeConn()
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	r := detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	if !r.NeedsRedistrib {
		t.Errorf("expected redistribution when a cluster cannot report its key")
	}
	if r.Skew != 1 {
		t.Errorf("skew = %v, want 1 (conservative)", r.Skew)
	}
}

// TestRedistributionNoRedistribute verifies that --no-redistribute reports the
// operator's choice as a no-op (the accounting still reflects the choice).
func TestRedistributionNoRedistribute(t *testing.T) {
	cfg := config.Default()
	cfg.NoRedistribute = true
	src := mkRedistribFake(true)
	dst := mkRedistribFake(false)
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	r := detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	if r.NeedsRedistrib {
		t.Errorf("expected no redistribution when --no-redistribute is set")
	}
	if r.Skew != 0 {
		t.Errorf("skew = %v, want 0 (suppressed)", r.Skew)
	}
}

// TestRedistributionQuery is a regression guard for the FR-15 accounting: the
// distribution probe must actually read the Greenplum distribution policy
// (gp_distribution_policy), not a broken/empty query. The fake answers by
// substring, so without this guard a broken query could silently pass. It
// mirrors the TestDetectCollationsQuery guard that caught pg_encoding_to_char.
func TestRedistributionQuery(t *testing.T) {
	cfg := config.Default()
	src := mkRedistribFake(true)
	dst := mkRedistribFake(true)
	tabs := []Table{{Schema: "s", Name: "orders", Bytes: 1000}}
	detectRedistribution(context.Background(), &cfg, src, dst, tabs)
	for _, fc := range []*pg.FakeConn{src, dst} {
		if !containsQuery(fc, "gs-redistrib-check") {
			t.Errorf("no gs-redistrib-check probe; got: %v", fc.Queried())
		}
		if !containsQuery(fc, "gp_distribution_policy") {
			t.Errorf("query did not read the real distribution policy; got: %v", fc.Queried())
		}
	}
}

