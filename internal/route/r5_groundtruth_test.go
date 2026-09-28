package route

import "testing"

// TestR5GroundTruth — live ground truth (lab B, D=2, 28.09): gp_segment_id
// for single-attr int4/int8/text dist keys, captured from the cluster.
// Guards the R5 router against any regression in the hash pipeline.
func TestR5GroundTruth(t *testing.T) {
	i4 := map[int32]int32{1: 1, 2: 0, 7: 0, 12345: 1, 100000: 1, 199999: 0}
	for v, want := range i4 {
		if got := RouteInt4(v, 2); got != want {
			t.Errorf("int4 %d: got %d want %d", v, got, want)
		}
	}
	i8 := map[int64]int32{7919: 1, 15838: 1, 97760055: 0, 791900000: 0}
	for v, want := range i8 {
		if got := JumpConsistentHash(uint64(HashInt8(v)), 2); got != want {
			t.Errorf("int8 %d: got %d want %d", v, got, want)
		}
	}
	tx := map[string]int32{"k1": 1, "k2": 0, "k7": 0, "k12345": 1, "k100000": 1}
	for v, want := range tx {
		if got := JumpConsistentHash(uint64(HashPGAny([]byte(v))), 2); got != want {
			t.Errorf("text %s: got %d want %d", v, got, want)
		}
	}
}
