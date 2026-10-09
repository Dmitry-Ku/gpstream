package plan

import "testing"

// TestToInt64: pgx decodes the catalog int4 columns (e.g. pg_database.encoding)
// as int32; the older int64/int-only switch fell through to 0 (= SQL_ASCII) and
// produced a false "SQL_ASCII -> UTF8 [DATA DESTRUCTIVE]" finding for every
// non-SQL_ASCII source. Every integer family must round-trip.
func TestToInt64(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want int64
	}{
		{"int64", int64(6), 6},
		{"int", 6, 6},
		{"int32 (pgx int4)", int32(6), 6},
		{"int16", int16(6), 6},
		{"int8", int8(6), 6},
		{"uint", uint(6), 6},
		{"uint8", uint8(6), 6},
		{"uint16", uint16(6), 6},
		{"uint32", uint32(6), 6},
		{"uint64", uint64(6), 6},
		{"encoding SQL_ASCII 0", int32(0), 0},
		{"encoding UTF8 6", int32(6), 6},
		{"string not int", "6", 0},
		{"nil not int", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toInt64(tt.v); got != tt.want {
				t.Fatalf("toInt64(%v) = %d, want %d", tt.v, got, tt.want)
			}
		})
	}
}
