package segstream

import (
	"bytes"
	"fmt"
	"context"
	"encoding/binary"
	"math/rand"
	"net"
	"testing"
	"time"
)

// TestSliceMuxMergeTwoSlices: two concurrent slice streams are merged into a
// single valid binary-COPY stream (one header, all rows, one EOF) and the
// reference parser counts exactly the sum of both slices' rows.
func TestSliceMuxMergeTwoSlices(t *testing.T) {
	rnd := rand.New(rand.NewSource(11))
	s0 := genSlice(rnd, 150)
	s1 := genSlice(rnd, 250)
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
	go fakeSegment(ch0, addr, s0, 1, 1, 0)
	go fakeSegment(ch1, addr, s1, 1, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, rerr := Receive(ctx, l, 2, func(contentID int16, p []byte) error {
		return mux.Write(contentID, p)
	})
	if serr := <-ch0; serr != nil {
		t.Fatalf("fake segment 0: %v", serr)
	}
	if serr := <-ch1; serr != nil {
		t.Fatalf("fake segment 1: %v", serr)
	}
	if rerr != nil {
		t.Fatalf("Receive: %v", rerr)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("slice %d err: %v", r.ContentID, r.Err)
		}
	}
	if !mux.AllDone() {
		t.Fatal("mux not AllDone after both slices completed")
	}
	if err := mux.End(); err != nil {
		t.Fatalf("End: %v", err)
	}

	got := merged.Bytes()
	// Structural checks on the merged stream.
	if !bytes.HasPrefix(got, []byte("PGCOPY\n\xff\r\n\x00")) {
		t.Fatal("merged stream: missing header signature")
	}
	if len(got) < 3 {
		t.Fatal("merged stream: too short")
	}
	if eof := int16(binary.BigEndian.Uint16(got[len(got)-2:])); eof != -1 {
		t.Fatalf("merged stream: bad EOF marker %d", eof)
	}
	// Exactly one EOF: the only int16 -1 in row-header position is the last.
	if n := countRows(got); n != want {
		t.Fatalf("merged rows = %d, want %d", n, want)
	}
	// Row order is irrelevant (the destination redistributes by its own key);
	// verify the multiset of row lengths is preserved by re-parsing each
	// original slice and comparing the combined (fc, lens) multiset.
	if err := checkRowMultiset(t, got, s0, s1); err != nil {
		t.Fatal(err)
	}
}

// TestSliceMuxEmptySlice: a slice with zero rows (header + EOF only) merges
// cleanly and contributes nothing.
func TestSliceMuxEmptySlice(t *testing.T) {
	empty := bcopyEOF(bcopyHeader())
	data := genSlice(rand.New(rand.NewSource(12)), 50)
	want := countRows(data)

	var merged bytes.Buffer
	mux := NewSliceMux(&merged, 2)
	if err := mux.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := mux.Write(0, empty); err != nil {
		t.Fatalf("write empty: %v", err)
	}
	if err := mux.Write(1, data); err != nil {
		t.Fatalf("write data: %v", err)
	}
	if !mux.AllDone() {
		t.Fatal("mux not AllDone")
	}
	if err := mux.End(); err != nil {
		t.Fatal(err)
	}
	if n := countRows(merged.Bytes()); n != want {
		t.Fatalf("merged rows = %d, want %d (empty slice must contribute 0)", n, want)
	}
}

// TestSliceMuxChunkedFeeding feeds each slice in awkward 1-7 byte chunks
// through the mux, exercising every parser phase boundary.
func TestSliceMuxChunkedFeeding(t *testing.T) {
	rnd := rand.New(rand.NewSource(13))
	s0 := genSlice(rnd, 80)
	s1 := genSlice(rnd, 80)
	want := countRows(s0) + countRows(s1)

	var merged bytes.Buffer
	mux := NewSliceMux(&merged, 2)
	if err := mux.Begin(); err != nil {
		t.Fatal(err)
	}
	chunkFeed := func(content int16, s []byte, r *rand.Rand) {
		for off := 0; off < len(s); {
			n := 1 + r.Intn(7)
			if off+n > len(s) {
				n = len(s) - off
			}
			if err := mux.Write(content, s[off:off+n]); err != nil {
				t.Fatalf("chunk write %d: %v", content, err)
			}
			off += n
		}
	}
	chunkFeed(0, s0, rnd)
	chunkFeed(1, s1, rnd)
	if !mux.AllDone() {
		t.Fatal("mux not AllDone after chunked feeding")
	}
	if err := mux.End(); err != nil {
		t.Fatal(err)
	}
	if n := countRows(merged.Bytes()); n != want {
		t.Fatalf("merged rows = %d, want %d", n, want)
	}
}

// rowSig returns the (fieldCount, [fieldLens]) signature of one row.
type rowSig struct {
	fc   int16
	lens []int32
}

func rowSigs(stream []byte) []rowSig {
	var out []rowSig
	off := 19
	for off < len(stream) {
		fc := int16(binary.BigEndian.Uint16(stream[off:]))
		off += 2
		if fc == -1 {
			break
		}
		s := rowSig{fc: fc}
		for j := 0; j < int(fc); j++ {
			ln := int32(binary.BigEndian.Uint32(stream[off:]))
			off += 4
			s.lens = append(s.lens, ln)
			if ln >= 0 {
				off += int(ln)
			}
		}
		out = append(out, s)
	}
	return out
}

func sigKey(s rowSig) string {
	k := int16Key(s.fc) + "|"
	for _, l := range s.lens {
		k += int32Key(l) + ","
	}
	return k
}

func int16Key(v int16) string {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	return string(b[:])
}

func int32Key(v int32) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return string(b[:])
}

// checkRowMultiset verifies the merged stream contains exactly the union of
// the rows (by structural signature) of the input slices.
func checkRowMultiset(t *testing.T, merged, s0, s1 []byte) error {
	t.Helper()
	count := map[string]int{}
	for _, s := range rowSigs(s0) {
		count[sigKey(s)]++
	}
	for _, s := range rowSigs(s1) {
		count[sigKey(s)]++
	}
	seen := map[string]int{}
	for _, s := range rowSigs(merged) {
		seen[sigKey(s)]++
	}
	for k, n := range count {
		if seen[k] != n {
			return fmt.Errorf("row multiset mismatch: sig %x want %d got %d", k, n, seen[k])
		}
	}
	for k, n := range seen {
		if count[k] != n {
			return fmt.Errorf("merged has extra rows: sig %x got %d", k, n)
		}
	}
	return nil
}

