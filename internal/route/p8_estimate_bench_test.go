package route

import "testing"

// P8 estimate (scratch, not committed): the per-row cost the direct_1to1
// fast path ELIMINATES — cdbhash of the int4 distribution key +
// JumpConsistentHash to a segment. This is the CPU the RowRouter spends on
// every row that a true segment->segment 1:1 stream does not pay.
func BenchmarkRouteInt4(b *testing.B) {
	var v int32
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v++
		_ = RouteInt4(v, 8)
	}
}

// The full per-row step RowRouter does (hash + JCH) for a single int4 key.
func BenchmarkPerRowHashJCH(b *testing.B) {
	var v int32
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v++
		h := HashInt4(v)
		_ = JumpConsistentHash(uint64(h), 8)
	}
}
