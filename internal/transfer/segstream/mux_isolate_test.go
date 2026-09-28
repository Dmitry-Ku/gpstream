package segstream

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"
)

// rowExtract safely extracts row byte-slices from a binary-COPY stream,
// stopping at the first structural error. Returns the rows parsed so far and
// ok=false if a structural error was hit.
func rowExtract(stream []byte) ([][]byte, bool) {
	off := 19
	const maxField = 100 * 1024 * 1024
	var rows [][]byte
	for off < len(stream) {
		if off+2 > len(stream) {
			return rows, false
		}
		fc := int16(binary.BigEndian.Uint16(stream[off:]))
		start := off
		off += 2
		if fc == -1 {
			return rows, true
		}
		if fc < 0 {
			return rows, false
		}
		rowStart := off
		for j := 0; j < int(fc); j++ {
			if off+4 > len(stream) {
				return rows, false
			}
			ln := int32(binary.BigEndian.Uint32(stream[off:]))
			off += 4
			if ln < -1 || ln > maxField {
				return rows, false
			}
			if ln >= 0 {
				if off+int(ln) > len(stream) {
					return rows, false
				}
				off += int(ln)
			}
		}
		rows = append(rows, stream[rowStart:off])
		_ = start
	}
	return rows, true
}

func rowMultiset(rows [][]byte) map[string]int {
	m := map[string]int{}
	for _, r := range rows {
		m[string(r)]++
	}
	return m
}

// TestTwoSlicesSequentialBuffer: feed slice0 fully, then slice1 fully, into a
// bytes.Buffer (no net.Pipe, no compression). The merged stream must be
// byte-identical to: 19B header + s0 rows + s1 rows + 2B EOF (the mux strips
// each slice's own 19B header and 2B EOF). Isolates the 2-slice mux logic.
func TestTwoSlicesSequentialBuffer(t *testing.T) {
	s0 := patternSlice(4000)
	s1 := patternSlice(4000)

	var merged bytes.Buffer
	mux := NewSliceMux(&merged, 2)
	if err := mux.Begin(); err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(s0); {
		n := 256 * 1024
		if off+n > len(s0) {
			n = len(s0) - off
		}
		if err := mux.Write(0, s0[off:off+n]); err != nil {
			t.Fatalf("s0 @%d: %v", off, err)
		}
		off += n
	}
	for off := 0; off < len(s1); {
		n := 256 * 1024
		if off+n > len(s1) {
			n = len(s1) - off
		}
		if err := mux.Write(1, s1[off:off+n]); err != nil {
			t.Fatalf("s1 @%d: %v", off, err)
		}
		off += n
	}
	if !mux.AllDone() {
		t.Fatal("not AllDone")
	}
	if err := mux.End(); err != nil {
		t.Fatal(err)
	}

	got := merged.Bytes()
	// Expected: 19B header + s0[19:-2] + s1[19:-2] + 2B EOF.
	want := make([]byte, 0, 19+len(s0)-21+len(s1)-21+2)
	want = append(want, bcopyHeader()...)
	want = append(want, s0[19:len(s0)-2]...)
	want = append(want, s1[19:len(s1)-2]...)
	want = append(want, 0xFF, 0xFF)
	if !bytes.Equal(got, want) {
		at := firstDiff(got, want)
		t.Fatalf("sequential 2-slice merge NOT byte-exact: got len=%d want len=%d; first diff at %d\n  got  % x\n  want % x",
			len(got), len(want), at, peek(got, at), peek(want, at))
	}
	t.Logf("sequential 2-slice: byte-exact OK (len=%d)", len(got))
}

// TestTwoSlicesConcurrentBuffer: feed two slices CONCURRENTLY (two goroutines)
// into a bytes.Buffer (no net.Pipe, no compression). Row order is
// arrival-nondeterministic, so compare the ROW multiset (each row's bytes)
// against the union of the two slices' rows. Isolates the concurrency from
// the transport.
func TestTwoSlicesConcurrentBuffer(t *testing.T) {
	for iter := 1; iter <= 5; iter++ {
		rnd := rand.New(rand.NewSource(int64(iter)))
		s0 := patternSlice(4000 + rnd.Intn(1000))
		s1 := patternSlice(4000 + rnd.Intn(1000))
		wantRows := int64(rowCountOf(s0) + rowCountOf(s1))

		var mu sync.Mutex
		var merged bytes.Buffer
		mux := NewSliceMux(&lockedWriter{mu: &mu, w: &merged}, 2)
		if err := mux.Begin(); err != nil {
			t.Fatal(err)
		}
		feed := func(content int16, s []byte) {
			for off := 0; off < len(s); {
				n := 256 * 1024
				if off+n > len(s) {
					n = len(s) - off
				}
				if err := mux.Write(content, s[off:off+n]); err != nil {
					t.Fatalf("s%d @%d: %v", content, off, err)
				}
				off += n
			}
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); feed(0, s0) }()
		go func() { defer wg.Done(); feed(1, s1) }()
		wg.Wait()
		if !mux.AllDone() {
			t.Fatal("not AllDone")
		}
		if err := mux.End(); err != nil {
			t.Fatal(err)
		}

		got := merged.Bytes()
		rows, ok := rowExtract(got)
		if !ok {
			at := firstDiff(got, got) // placeholder
			_ = at
			t.Fatalf("iter%d: merged stream STRUCTURALLY CORRUPT (len=%d)", iter, len(got))
		}
		if int64(len(rows)) != wantRows {
			t.Fatalf("iter%d: merged rows=%d want=%d", iter, len(rows), wantRows)
		}
		s0rows, _ := rowExtract(s0)
		want := rowMultiset(s0rows)
		s1rows, _ := rowExtract(s1)
		for _, r := range s1rows {
			want[string(r)]++
		}
		gotm := rowMultiset(rows)
		for k, n := range want {
			if gotm[k] != n {
				t.Fatalf("iter%d: row multiset mismatch: want %d got %d for a row of len %d",
					iter, n, gotm[k], len(k))
			}
		}
	}
}

// lockedWriter serializes Write calls (the mux already serializes via its
// mutex, but this mirrors the single-writer contract of the real pipe).
type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (lw *lockedWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.w.Write(p)
}

func rowCountOf(s []byte) int {
	rows, _ := rowExtract(s)
	return len(rows)
}

