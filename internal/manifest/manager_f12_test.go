package manifest

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// The F-12 decision logic (compressionOffFor / unitCompressors) is exercised
// through a real Manager so the test tracks the production wiring, not a copy
// of the predicate.

func TestCompressionOffForDecision(t *testing.T) {
	cases := []struct {
		name      string
		comp      config.Compression
		threshold int64
		bytes     int64
		want      bool
	}{
		{"none-mode-never-off", config.CompNone, 1 << 20, 100, false},
		{"zstd-small-off", config.CompZstd, 1 << 20, 1 << 19, true},
		{"zstd-large-on", config.CompZstd, 1 << 20, 2 << 20, false},
		{"zstd-at-threshold-on", config.CompZstd, 1 << 20, 1 << 20, false},
		{"zlib-small-off", config.CompZlib, 1 << 20, 1024, true},
		{"zero-threshold-disabled", config.CompZstd, 0, 100, false},
		{"unknown-size-keeps-mode", config.CompZstd, 1 << 20, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Compression = tc.comp
			cfg.SmallTableThreshold = tc.threshold
			m, _ := newManagerAt(t, &cfg, []byte("data"), []byte("data"))
			got := m.compressionOffFor(plan.Table{Bytes: tc.bytes})
			if got != tc.want {
				t.Fatalf("compressionOffFor(%s, thr=%d, bytes=%d) = %v, want %v",
					tc.comp, tc.threshold, tc.bytes, got, tc.want)
			}
		})
	}
}

func TestUnitCompressorsSwap(t *testing.T) {
	cfg := config.Default()
	cfg.Compression = config.CompZstd
	cfg.SmallTableThreshold = 1 << 20
	m, _ := newManagerAt(t, &cfg, []byte("data"), []byte("data"))

	// Small table: the none pair must be returned (and be mode none).
	comp, dec := m.unitCompressors(plan.Table{Bytes: 1 << 19})
	if comp.Mode() != compress.ModeNone || dec.Mode() != compress.ModeNone {
		t.Fatalf("small table must get the none pair, got comp=%v dec=%v", comp.Mode(), dec.Mode())
	}
	// Large table: the operator's zstd pair.
	comp, dec = m.unitCompressors(plan.Table{Bytes: 2 << 20})
	if comp.Mode() != compress.ModeZstd || dec.Mode() != compress.ModeZstd {
		t.Fatalf("large table must keep zstd, got comp=%v dec=%v", comp.Mode(), dec.Mode())
	}
}

func TestManagerSmallTableEmitsEvent(t *testing.T) {
	cfg := config.Default()
	cfg.Compression = config.CompZstd
	cfg.SmallTableThreshold = 1 << 20
	m, dir := newManagerAt(t, &cfg, []byte("data"), []byte("data"))

	// A single tiny unit: below the threshold -> compression_off event.
	p := oneUnitPlan()
	p.Units[0].Bytes = 1 << 19

	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	m.events.Close()
	evs := eventsOf(t, dir)
	offs := findEvent(t, evs, "compression_off")
	if len(offs) != 1 {
		t.Fatalf("got %d compression_off events, want 1: %v", len(offs), evs)
	}
	if offs[0]["table"] != "public.orders" {
		t.Errorf("table = %v, want public.orders", offs[0]["table"])
	}
	if offs[0]["threshold"] != float64(int64(1<<20)) {
		t.Errorf("threshold = %v, want %d", offs[0]["threshold"], 1<<20)
	}
}

func TestManagerLargeTableNoEvent(t *testing.T) {
	cfg := config.Default()
	cfg.Compression = config.CompZstd
	cfg.SmallTableThreshold = 1 << 20
	m, dir := newManagerAt(t, &cfg, []byte("data"), []byte("data"))

	// A single large unit: above the threshold -> no compression_off event.
	p := oneUnitPlan()
	p.Units[0].Bytes = 2 << 20

	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	m.events.Close()
	evs := eventsOf(t, dir)
	if offs := findEvent(t, evs, "compression_off"); len(offs) != 0 {
		t.Fatalf("got %d compression_off events, want 0: %v", len(offs), offs)
	}
}
