package segstream

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestSliceMuxByteExactLargeSlice: the mux must be a BYTE-transparent
// pass-through for a single slice. We feed a multi-MB slice (many 256 KiB
// frame chunks, the live path) through mux.Write in 256 KiB chunks and require
// the merged output to be byte-identical to the input slice (the mux writes
// the same 19-byte header and 2-byte EOF the slice already has, and strips the
// slice's own header/EOF). Any data-byte shift/loss in the chunked parser
// state machine shows up as a mismatch.
//
// This is the byte-exact regression for the E2 on_segment live bug (28.09):
// t1 (100k rows, ~2.3 MB/segment) and t2 (10k rows, ~639 KB/segment) corrupted
// the destination COPY ("invalid byte sequence for encoding UTF8: 0x00" /
// "unexpected EOF"), while t3 (1k rows, ~213 KB/segment, a single 256 KiB
// frame) succeeded — i.e. the failure only appears when a slice spans more
// than one 256 KiB frame chunk.
func TestSliceMuxByteExactLargeSlice(t *testing.T) {
	const chunk = 256 * 1024 // == segstream chunkSize (frame payload)

	for _, seed := range []int64{1, 7, 42, 2026} {
		t.Run("seed", func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			slice := genSlice(rnd, 6000) // ~2 MB, spans ~8 chunks
			if len(slice) <= chunk {
				t.Fatalf("slice too small to span chunks: %d", len(slice))
			}

			var merged bytes.Buffer
			mux := NewSliceMux(&merged, 1)
			if err := mux.Begin(); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			for off := 0; off < len(slice); {
				n := chunk
				if off+n > len(slice) {
					n = len(slice) - off
				}
				if err := mux.Write(0, slice[off:off+n]); err != nil {
					t.Fatalf("write chunk @%d: %v", off, err)
				}
				off += n
			}
			if !mux.AllDone() {
				t.Fatal("mux not AllDone after full slice")
			}
			if err := mux.End(); err != nil {
				t.Fatalf("End: %v", err)
			}

			got := merged.Bytes()
			if !bytes.Equal(got, slice) {
				// Locate the first divergence for a precise diagnosis.
				at := firstDiff(got, slice)
				t.Fatalf("merged != slice (byte corruption): len got=%d want=%d; first diff at byte %d\n  got  % x\n  want % x",
					len(got), len(slice), at,
					peek(got, at), peek(slice, at))
			}
		})
	}
}

// TestSliceMuxByteExactOddChunks: same byte-exact requirement but with
// awkward chunk sizes (prime-ish, small) so every parser phase boundary and
// every field-data/chunk alignment is exercised, not just the 256 KiB size.
func TestSliceMuxByteExactOddChunks(t *testing.T) {
	for _, chunk := range []int{3, 7, 64, 4096, 65537} {
		t.Run("chunk", func(t *testing.T) {
			rnd := rand.New(rand.NewSource(99))
			slice := genSlice(rnd, 2000) // ~800 KB
			var merged bytes.Buffer
			mux := NewSliceMux(&merged, 1)
			if err := mux.Begin(); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			for off := 0; off < len(slice); {
				n := chunk
				if off+n > len(slice) {
					n = len(slice) - off
				}
				if err := mux.Write(0, slice[off:off+n]); err != nil {
					t.Fatalf("write chunk @%d: %v", off, err)
				}
				off += n
			}
			if !mux.AllDone() {
				t.Fatal("mux not AllDone")
			}
			if err := mux.End(); err != nil {
				t.Fatalf("End: %v", err)
			}
			got := merged.Bytes()
			if !bytes.Equal(got, slice) {
				at := firstDiff(got, slice)
				t.Fatalf("chunk=%d: merged != slice: len got=%d want=%d; first diff at %d\n  got  % x\n  want % x",
					chunk, len(got), len(slice), at, peek(got, at), peek(slice, at))
			}
		})
	}
}

func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func peek(b []byte, at int) []byte {
	if at >= len(b) {
		at = len(b) - 1
	}
	s := at - 8
	if s < 0 {
		s = 0
	}
	e := at + 8
	if e > len(b) {
		e = len(b)
	}
	return b[s:e]
}
