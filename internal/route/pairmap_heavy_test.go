//go:build heavy

package route

// Heavy (non-CI-by-default) pair-map tests, per TASK-SPEC F-09: "heavy/
// scale tests — by tags and on a schedule". The default `go test` / CI run must
// not pay for a full 2^32-domain brute force.

import (
	"testing"
	"time"
)

// TestPairMapBruteForceNonGolden — full-domain brute force for a NON-golden
// (S,D) pair (3,5): it must work and agree with the consistency property.
//
// Why the `heavy` tag: the brute force sweeps the whole 2^32 hash domain.
// Plain it takes ~36 s; under `-cover` instrumentation 231–328 s were observed
// (28.09), which exceeded a previous hard 120 s wall-clock limit and turned
// the CI's `go test -cover` run red. The limit below is a sanity guard only
// (catch a real algorithmic regression); the test is scheduled in CI via the
// `heavy` tag (ci.yml schedule job), not the default run.
func TestPairMapBruteForceNonGolden(t *testing.T) {
	t0 := time.Now()
	m := PairMap(3, 5)
	dur := time.Since(t0)
	t.Logf("PairMap(3,5) brute force: %v", dur.Round(time.Millisecond))
	if dur > 600*time.Second {
		t.Fatalf("brute force too slow: %v (sanity guard; real regression?)", dur)
	}
	for s, ds := range m {
		if s < 0 || s >= 3 {
			t.Fatalf("bad s=%d", s)
		}
		for _, d := range ds {
			if d < s {
				t.Fatalf("consistency violated: (%d,%d)", s, d)
			}
		}
	}
	// at least one pair per s
	for s := int32(0); s < 3; s++ {
		if len(m[s]) == 0 {
			t.Fatalf("s=%d has no pairs", s)
		}
	}
}
