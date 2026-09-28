package route

import "testing"

// TestR5GroundTruthD4 — live D=4 ground truth (lab B rebuilt to 4 segments,
// 28.09): gp_segment_id for int4 dist keys, captured from the cluster and
// independently verified 111000/111000 by the R5 direct-path verifier.
//
// This pins the JCH truncation-order bug: the old Go implementation
// `(b+1) * int64(ratio)` (truncating the ratio first) diverged from GPDB's C
// `int64((b+1) * ratio)` at D>=4, misplacing ~12% of rows across the
// segment-2/3 boundary. D=2 ground truth cannot catch it.
func TestR5GroundTruthD4(t *testing.T) {
	pins := map[int32]int32{
		// (key -> live gp_segment_id on the 4-seg cluster)
		1: 1, 2: 3, 3: 0, 4: 0, 5: 2, 6: 2, 9: 3, 12: 1,
		// boundary keys where the old truncated-JCH gave the wrong segment:
		13: 2, 25: 2, 46: 3, 63: 2, 69: 3,
		12345: 3, 77777: 1, 100000: 2,
	}
	for v, want := range pins {
		if got := RouteInt4(v, 4); got != want {
			t.Errorf("int4 %d (D=4): got %d want %d", v, got, want)
		}
	}
}
