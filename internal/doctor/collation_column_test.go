package doctor

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

// TestDoctorColumnCollationMissing verifies (F-16) that the doctor's
// column-level collation pre-flight reports a WARN when the source's
// public-schema tables use an explicit collation the destination does not
// have installed, while the DB-level "collation" check is unaffected.
func TestDoctorColumnCollationMissing(t *testing.T) {
	// Source has an explicit "C.utf8" column collation; the destination's
	// pg_collation (pg_catalog, oid 11) does NOT list it.
	src := healthy()
	src.On("gs-collation-doctor", [][]any{{"C.utf8"}})
	dst := healthy()
	dst.On("gs-collation-doctor-dst", [][]any{{"default"}, {"C"}, {"POSIX"}})
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

// TestDoctorColumnCollationInstalled verifies (F-16) that when the destination
// has the explicit collation installed, no column-collation WARN is raised.
func TestDoctorColumnCollationInstalled(t *testing.T) {
	src := healthy()
	src.On("gs-collation-doctor", [][]any{{"C.utf8"}})
	dst := healthy()
	dst.On("gs-collation-doctor-dst", [][]any{{"default"}, {"C"}, {"POSIX"}, {"C.utf8"}})
	src.On("pg_database", [][]any{{"C"}})
	dst.On("pg_database", [][]any{{"C"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if hasColumnCollationWarn(rep) {
		t.Fatalf("no column-collation WARN expected when installed, got %+v", rep.Checks)
	}
}

// TestDoctorColumnCollationNone verifies (F-16) that a source with no explicit
// column collation (empty gs-collation-doctor answer) raises no
// column-collation WARN.
func TestDoctorColumnCollationNone(t *testing.T) {
	src := healthy()
	src.On("gs-collation-doctor", [][]any{}) // no explicit collations
	dst := healthy()
	dst.On("gs-collation-doctor-dst", [][]any{{"default"}, {"C"}})
	src.On("pg_database", [][]any{{"C"}})
	dst.On("pg_database", [][]any{{"C"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if hasColumnCollationWarn(rep) {
		t.Fatalf("no column-collation WARN expected with no explicit collation, got %+v", rep.Checks)
	}
}
