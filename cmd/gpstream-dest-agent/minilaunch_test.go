package main

import "testing"

// TestCatInt64 guards the segment-catalog column coercion (minilaunch.go).
// gp_segment_configuration: content is int2 (pgx scans to int16), port is int4
// (pgx scans to int32). The original raw row[n].(int64) silently returned 0 for
// both — the live A→A mesh run died with "contents not dense (got 0 at
// position 1)" because every segment parsed as content=0. These are the actual
// scan types, so they must round-trip.
func TestCatInt64(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int64
	}{
		{"int16-content", int16(2), 2},   // content column (int2)
		{"int32-port", int32(20002), 20002}, // port column (int4)
		{"int64", int64(7), 7},
		{"int", int(9), 9},
		{"uint32", uint32(5), 5},
		{"uint64", uint64(6), 6},
		{"float64", float64(8), 8},
		{"string", "123", 123},
		{"nil", nil, 0},
		{"other", true, 0},
	}
	for _, c := range cases {
		if got := catInt64(c.in); got != c.want {
			t.Errorf("%s: catInt64(%v) = %d, want %d", c.name, c.in, got, c.want)
		}
	}
}
