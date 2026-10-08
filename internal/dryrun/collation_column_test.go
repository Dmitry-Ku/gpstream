package dryrun

import (
	"context"
	"testing"
)

// hasColumnCollationWarn reports whether the report carries a column-collation
// WARN (F-16). The check only exists when an explicit source collation is
// missing on the destination, so its absence means the pre-flight passed.
func hasColumnCollationWarn(r *Report) bool {
	for _, c := range r.Checks {
		if c.Name == "column-collation" && c.Sev == Warn {
			return true
		}
	}
	return false
}

// TestDryrunColumnCollationMissing verifies (F-16) that the diagnostic's
// column-level collation pre-flight reports a WARN when the source's
// public-schema tables use an explicit collation the destination does not
// have installed, while the DB-level "collation" check is unaffected.
func TestDryrunColumnCollationMissing(t *testing.T) {
	// Source has an explicit "C.utf8" column collation; the destination's
	// pg_collation (pg_catalog, oid 11) does NOT list it.
	src := healthy()
	src.On("gs-collation-dryrun", [][]any{{"C.utf8"}})
	dst := healthy()
	dst.On("gs-collation-dryrun-dst", [][]any{{"default"}, {"C"}, {"POSIX"}})
	// Same DB-level collation so the only finding is the column one.
	src.On("pg_database", [][]any{{"C"}})
	dst.On("pg_database", [][]any{{"C"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if !hasColumnCollationWarn(rep) {
		t.Fatalf("expected a column-collation WARN for a missing collation, got checks %+v", rep.Checks)
	}
	// The DB-level collation check is unaffected (both C -> PASS).
	if find(rep, "network", "collation") != Pass {
		t.Fatalf("DB-level collation should be PASS, got %v", find(rep, "network", "collation"))
	}
}

// TestDryrunColumnCollationInstalled verifies (F-16) that when the destination
// has the explicit collation installed, no column-collation WARN is raised.
func TestDryrunColumnCollationInstalled(t *testing.T) {
	src := healthy()
	src.On("gs-collation-dryrun", [][]any{{"C.utf8"}})
	dst := healthy()
	dst.On("gs-collation-dryrun-dst", [][]any{{"default"}, {"C"}, {"POSIX"}, {"C.utf8"}})
	src.On("pg_database", [][]any{{"C"}})
	dst.On("pg_database", [][]any{{"C"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if hasColumnCollationWarn(rep) {
		t.Fatalf("no column-collation WARN expected when installed, got %+v", rep.Checks)
	}
}

// TestDryrunColumnCollationNone verifies (F-16) that a source with no explicit
// column collation (empty gs-collation-dryrun answer) raises no
// column-collation WARN.
func TestDryrunColumnCollationNone(t *testing.T) {
	src := healthy()
	src.On("gs-collation-dryrun", [][]any{}) // no explicit collations
	dst := healthy()
	dst.On("gs-collation-dryrun-dst", [][]any{{"default"}, {"C"}})
	src.On("pg_database", [][]any{{"C"}})
	dst.On("pg_database", [][]any{{"C"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if hasColumnCollationWarn(rep) {
		t.Fatalf("no column-collation WARN expected with no explicit collation, got %+v", rep.Checks)
	}
}
