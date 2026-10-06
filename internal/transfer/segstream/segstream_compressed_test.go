package segstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// fakeSegmentCompressed is fakeSegment with compression: it streams the slice
// through SendCompressed (the compressed wire form the P4 segreceiver uses).
func fakeSegmentCompressed(ch chan<- error, addr string, slice []byte, jobID, unitID uint32, content int16, m compress.Mode, level int) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		ch <- fmt.Errorf("fake segment dial: %w", err)
		return
	}
	if _, err := SendCompressed(context.Background(), conn, bytes.NewReader(slice), jobID, unitID, content, m, level, false); err != nil {
		ch <- fmt.Errorf("SendCompressed: %w", err)
		return
	}
	ch <- nil
}

// genCompressibleSlice is genSlice with COMPRESSIBLE content (repeated short
// values + a bit of structure) — like real table data, not /dev/urandom. The
// wire<logical assertion of the ADR-002 acceptance only holds for data that
// actually compresses; random bytes per 256KiB chunk gain nothing and the
// per-chunk zstd header even bloats them.
func genCompressibleSlice(rnd *rand.Rand, nRows int) []byte {
	var out []byte
	out = append(out, bcopyHeader()...)
	for i := 0; i < nRows; i++ {
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], 3)
		out = append(out, b[:]...)
		v := i % 64
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], 8)
		out = append(out, l[:]...)
		out = append(out, []byte(fmt.Sprintf("row-%03d-", v))...)
		binary.BigEndian.PutUint32(l[:], 1)
		out = append(out, l[:]...)
		out = append(out, byte(v%256))
		binary.BigEndian.PutUint32(l[:], 16)
		out = append(out, l[:]...)
		out = append(out, fmt.Sprintf("%016x", uint64(v*2654435761%16))...)
	}
	out = bcopyEOF(out)
	return out
}

// TestCompressedTwoSlicesMerged (P4.1): two segments stream their slices
// COMPRESSED (zstd) over real TCP to the destination; ReceiveCompressed
// decompresses each slice and SliceMux merges them into one binary-COPY
// stream. The merged stream must be byte-equivalent (by row multiset) to the
// UNcompressed reference merge, the row count must be the sum, the trailer
// digests must match the uncompressed content, and the on-wire (compressed)
// bytes must be strictly less than the logical (uncompressed) bytes — i.e.
// the network carries the compressed volume (the ADR-002 acceptance).
func TestCompressedTwoSlicesMerged(t *testing.T) {
	const level = 3
	rnd := rand.New(rand.NewSource(101))
	s0 := genCompressibleSlice(rnd, 5000)
	s1 := genCompressibleSlice(rnd, 5000)
	want := countRows(s0) + countRows(s1)

	var merged bytes.Buffer
	mux := NewSliceMux(&merged, 2)
	if err := mux.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	ch0 := make(chan error, 1)
	ch1 := make(chan error, 1)
	go fakeSegmentCompressed(ch0, addr, s0, 1, 1, 0, compress.ModeZstd, level)
	go fakeSegmentCompressed(ch1, addr, s1, 1, 1, 1, compress.ModeZstd, level)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, rerr := ReceiveCompressed(ctx, l, 2, compress.ModeZstd, func(contentID int16, p []byte) error {
		return mux.Write(contentID, p)
	})
	if serr := <-ch0; serr != nil {
		t.Fatalf("fake segment 0: %v", serr)
	}
	if serr := <-ch1; serr != nil {
		t.Fatalf("fake segment 1: %v", serr)
	}
	if rerr != nil {
		t.Fatalf("ReceiveCompressed: %v", rerr)
	}
	if !mux.AllDone() {
		t.Fatal("mux not AllDone after both compressed slices completed")
	}
	if err := mux.End(); err != nil {
		t.Fatalf("End: %v", err)
	}

	got := merged.Bytes()
	if n := countRows(got); n != want {
		t.Fatalf("merged rows = %d, want %d", n, want)
	}
	if err := checkRowMultiset(t, got, s0, s1); err != nil {
		t.Fatal(err)
	}
	// The trailer digest is over the UNcompressed payload: it must equal the
	// sha256 of each slice's raw bytes (the source's logical content).
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("slice %d err: %v", r.ContentID, r.Err)
		}
		var want []byte
		if r.ContentID == 0 {
			want = s0
		} else {
			want = s1
		}
		sum := sha256.Sum256(want)
		if r.SHA256 != sum {
			t.Fatalf("slice %d trailer sha256 = %x, want %x (must be over the uncompressed payload)", r.ContentID, r.SHA256, sum)
		}
		if r.Rows != countRows(want) {
			t.Fatalf("slice %d rows = %d, want %d", r.ContentID, r.Rows, countRows(want))
		}
	}
	// ADR-002 acceptance: the network carried the COMPRESSED volume. The
	// per-slice wire bytes (res.Bytes, the compressed framed payload) must be
	// strictly less than the uncompressed logical bytes of both slices.
	var wire, logical uint64
	for _, r := range results {
		wire += r.Bytes
	}
	logical = uint64(len(s0) + len(s1))
	if wire >= logical {
		t.Fatalf("compressed wire %d >= logical %d — the network did not carry the compressed volume", wire, logical)
	}
	t.Logf("P4.1 ok: rows=%d wire=%d logical=%d ratio=%.1fx", want, wire, logical, float64(logical)/float64(wire))
}

// TestCompressedSliceModeMismatch (P4.1): a source that compressed with a
// different mode than the destination expects must be rejected (a
// mis-configured agent pair), not silently mis-decompressed.
func TestCompressedSliceModeMismatch(t *testing.T) {
	rnd := rand.New(rand.NewSource(102))
	slice := genSlice(rnd, 50)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	ch := make(chan error, 1)
	// The segment compresses with zstd.
	go fakeSegmentCompressed(ch, addr, slice, 1, 1, 0, compress.ModeZstd, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The destination expects zlib -> the trailer's mode check must fail.
	// A per-slice error lands in results[i].Err (clean accept-completion
	// returns a nil transport error).
	results, _ := ReceiveCompressed(ctx, l, 1, compress.ModeZlib, nil)
	if serr := <-ch; serr != nil {
		t.Fatalf("fake segment: %v", serr)
	}
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("expected a mode-mismatch slice error, got results=%+v", results)
	}
	if !strings.Contains(results[0].Err.Error(), "announced mode") {
		t.Fatalf("expected a mode-mismatch error, got: %v", results[0].Err)
	}
}
