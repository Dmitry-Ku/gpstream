package source

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestIndexes captures a table's index definitions (F-17) — the source half of
// the background index rebuild. The live F-17 bug (n.nspname=$1 vs $2) is
// guarded by the integration tests; here we pin the decode and the empty case.
func TestIndexes(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_get_indexdef", [][]any{
		{"orders_pkey", true, true, "CREATE UNIQUE INDEX orders_pkey ON public.orders USING btree (id)"},
		{"orders_a_idx", true, false, "CREATE UNIQUE INDEX orders_a_idx ON public.orders USING btree (a)"},
		{"orders_b_idx", false, false, "CREATE INDEX orders_b_idx ON public.orders USING btree (b)"},
	})
	a := New(f)
	idx, err := a.Indexes(context.Background(), "public", "orders")
	if err != nil {
		t.Fatalf("Indexes: %v", err)
	}
	if len(idx) != 3 {
		t.Fatalf("got %d indexes, want 3", len(idx))
	}
	if !idx[0].PK || !idx[0].Unique || idx[0].Name != "orders_pkey" {
		t.Errorf("idx[0] = %+v, want PK+unique orders_pkey", idx[0])
	}
	if !idx[1].Unique || idx[1].PK {
		t.Errorf("idx[1] = %+v, want unique non-PK", idx[1])
	}
	if idx[2].Unique || idx[2].PK {
		t.Errorf("idx[2] = %+v, want secondary", idx[2])
	}
	if idx[0].Def == "" {
		t.Error("idx[0].Def must carry the CREATE INDEX definition")
	}
}

func TestIndexesEmpty(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_get_indexdef", [][]any{})
	a := New(f)
	idx, err := a.Indexes(context.Background(), "public", "no_idx")
	if err != nil {
		t.Fatalf("Indexes (none): %v", err)
	}
	if idx == nil || len(idx) != 0 {
		t.Errorf("want empty non-nil slice, got %#v", idx)
	}
}

// TestIntConverters pins the any->int64/uint32 decoding across the types pgx
// may hand back (numeric OIDs as uint32/int64, widths, float fallback).
func TestIntConverters(t *testing.T) {
	if toInt64(int64(5)) != 5 || toInt64(int32(5)) != 5 || toInt64(int16(5)) != 5 ||
		toInt64(int(5)) != 5 || toInt64(uint64(5)) != 5 || toInt64(float64(5)) != 5 {
		t.Error("toInt64 numeric cases wrong")
	}
	if toInt64(nil) != 0 || toInt64("x") != 0 {
		t.Error("toInt64 nil/non-numeric must be 0")
	}

	if toUint32(uint32(7)) != 7 || toUint32(int32(7)) != 7 || toUint32(int64(7)) != 7 || toUint32(uint64(7)) != 7 {
		t.Error("toUint32 numeric cases wrong")
	}
	if toUint32(nil) != 0 || toUint32("x") != 0 {
		t.Error("toUint32 nil/non-numeric must be 0")
	}
}
