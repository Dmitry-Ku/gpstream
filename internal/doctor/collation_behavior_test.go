package doctor

import (
	"context"
	"testing"
)

// hasBehaviorCollationWarn reports whether the report carries the Fix-2
// behavioral collation WARN (same locale name, different glibc sort).
func hasBehaviorCollationWarn(r *Report) bool {
	for _, c := range r.Checks {
		if c.Name == "collation-behavior" && c.Sev == Warn {
			return true
		}
	}
	return false
}

// TestDoctorCollationBehaviorDiff is the Fix-2 core case (lab, 29.09):
// both clusters report the SAME LC_COLLATE name ("en_US.utf8") but the
// behavioral probe (CollationProbeSQL) yields different orderings — a glibc
// difference the name comparison cannot see. The doctor must raise a
// collation-behavior WARN while the name-based "collation" check stays PASS.
func TestDoctorCollationBehaviorDiff(t *testing.T) {
	src := healthy()
	dst := healthy()
	src.On("pg_database", [][]any{{"en_US.utf8"}})
	dst.On("pg_database", [][]any{{"en_US.utf8"}})
	src.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{"1",1,2}`}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if !hasBehaviorCollationWarn(rep) {
		t.Fatalf("expected a collation-behavior WARN (same locale, different glibc), got %+v", rep.Checks)
	}
	// The name-based check still passes (the names match).
	if find(rep, "network", "collation") != Pass {
		t.Fatalf("name-based collation check should be PASS, got %v", find(rep, "network", "collation"))
	}
}

// TestDoctorCollationBehaviorSame verifies no behavioral WARN when the probe
// signatures match (same glibc behavior).
func TestDoctorCollationBehaviorSame(t *testing.T) {
	src := healthy()
	dst := healthy()
	src.On("pg_database", [][]any{{"en_US.utf8"}})
	dst.On("pg_database", [][]any{{"en_US.utf8"}})
	src.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if hasBehaviorCollationWarn(rep) {
		t.Fatalf("no collation-behavior WARN expected for matching signatures, got %+v", rep.Checks)
	}
}

// TestDoctorCollationBehaviorProbeFault verifies a probe fault degrades to no
// behavioral finding (never a WARN, never a doctor failure).
func TestDoctorCollationBehaviorProbeFault(t *testing.T) {
	src := healthy()
	dst := healthy()
	src.On("pg_database", [][]any{{"en_US.utf8"}})
	dst.On("pg_database", [][]any{{"en_US.utf8"}})
	// Only src answers the probe; dst's probe errors -> no behavioral finding.
	src.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if hasBehaviorCollationWarn(rep) {
		t.Fatalf("probe fault must not raise a collation-behavior WARN, got %+v", rep.Checks)
	}
	if !rep.Healthy {
		t.Fatalf("probe fault must not make the doctor unhealthy, got %+v", rep.Checks)
	}
}
