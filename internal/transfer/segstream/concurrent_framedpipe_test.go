package segstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// patternSlice builds a binary-COPY slice of nRows rows; each row has a
// leading uint32 id field and a 300-byte text field with a per-row pattern.
// (Data bytes are deterministic per row index.)
func patternSlice(nRows int) []byte {
	b := bcopyHeader()
	for i := 0; i < nRows; i++ {
		var idb [4]byte
		binary.BigEndian.PutUint32(idb[:], uint32(i))
		text := make([]byte, 300)
		for k := range text {
			text[k] = byte(i>>16 ^ i ^ k)
		}
		b = bcopyRow(b, idb[:], text)
	}
	return bcopyEOF(b)
}

// safeRowCheck parses a binary-COPY stream structurally and returns (rows,
// firstBadOffset, firstBadLen) — it never panics, and reports the offset of
// the first field whose length is implausible (>100 MB) or that would run
// past the buffer end. A clean stream returns (n, -1, 0).
func safeRowCheck(stream []byte) (rows int64, badOff int, badLen int32) {
	off := 19 // skip 19-byte header
	const maxField = 100 * 1024 * 1024
	for off < len(stream) {
		if off+2 > len(stream) {
			return rows, off, -1
		}
		fc := int16(binary.BigEndian.Uint16(stream[off:]))
		off += 2
		if fc == -1 {
			return rows, -1, 0
		}
		if fc < 0 {
			return rows, off - 2, -1
		}
		for j := 0; j < int(fc); j++ {
			if off+4 > len(stream) {
				return rows, off, -1
			}
			ln := int32(binary.BigEndian.Uint32(stream[off:]))
			off += 4
			if ln < -1 || ln > maxField {
				return rows, off - 4, ln
			}
			if ln >= 0 {
				if off+int(ln) > len(stream) {
					return rows, off, ln
				}
				off += int(ln)
			}
		}
		rows++
	}
	return rows, -1, 0
}

// runConcurrentTwoSlicesFramedPipe runs the EXACT copyStreamSegmentDirect data
// plane with the given compression mode and returns the bytes the destination
// read off the FramedPipe.
func runConcurrentTwoSlicesFramedPipe(t *testing.T, mode compress.Mode, s0, s1 []byte) []byte {
	t.Helper()
	comp := compress.New(mode, 3)
	dec := compress.NewDec(mode)

	var merged []byte
	var dmu sync.Mutex
	w, r := net.Pipe()
	fp := transfer.NewFramedPipe(w, r, 1, 1, comp, dec)

	var out bytes.Buffer
	dstDone := make(chan struct{})
	go func() {
		defer close(dstDone)
		buf := make([]byte, 64*1024)
		for {
			n, err := fp.Read(buf)
			if n > 0 {
				out.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
		dmu.Lock()
		merged = out.Bytes()
		dmu.Unlock()
	}()

	mux := NewSliceMux(fp, 2)
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	results, rerr := Receive(ctx, l, 2, mux.Write)
	if serr := <-ch0; serr != nil {
		t.Fatalf("seg0: %v", serr)
	}
	if serr := <-ch1; serr != nil {
		t.Fatalf("seg1: %v", serr)
	}
	if rerr != nil {
		t.Fatalf("Receive: %v", rerr)
	}
	for _, res := range results {
		if res.Err != nil {
			t.Fatalf("slice %d: %v", res.ContentID, res.Err)
		}
	}
	if err := mux.End(); err != nil {
		t.Fatalf("End: %v", err)
	}
	_ = fp.Close()
	<-dstDone
	return merged
}

// TestConcurrentTwoSlicesFramedPipe: the regression harness for the E2
// on_segment live corruption (28.09: t1/t2 "invalid byte sequence for encoding
// UTF8: 0x00" / "unexpected EOF in COPY data"; t3 OK). It runs 2 concurrent
// segments → Receive → mux → a REAL FramedPipe (net.Pipe + compression) and
// requires the destination stream to be a structurally valid binary-COPY with
// exactly n0+n1 rows. It is parameterised over the compression mode so the
// zstd path and the no-compression path can be compared.
func TestConcurrentTwoSlicesFramedPipe(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode compress.Mode
	}{
		{"zstd", compress.ModeZstd},
		{"none", compress.ModeNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for iter := 1; iter <= 3; iter++ {
				rnd := rand.New(rand.NewSource(int64(iter)))
				n0 := 4000 + rnd.Intn(2000) // ~1.5-2.2 MB/slice (many frames)
				n1 := 4000 + rnd.Intn(2000)
				s0 := patternSlice(n0)
				s1 := patternSlice(n1)

				merged := runConcurrentTwoSlicesFramedPipe(t, tc.mode, s0, s1)

				if !bytes.HasPrefix(merged, []byte("PGCOPY\n\xff\r\n\x00")) {
					t.Fatalf("iter%d: merged missing header sig (len=%d)", iter, len(merged))
				}
				rows, badOff, badLen := safeRowCheck(merged)
				if badOff >= 0 {
					// Show a hex window around the first corruption.
					lo := badOff - 16
					if lo < 0 {
						lo = 0
					}
					hi := badOff + 16
					if hi > len(merged) {
						hi = len(merged)
					}
					t.Fatalf("iter%d: CORRUPT at offset %d (len %d): merged len=%d rows-so-far=%d want=%d\n  hex@%d: % x",
						iter, badOff, badLen, len(merged), rows, n0+n1, badOff, merged[lo:hi])
				}
				if rows != int64(n0+n1) {
					t.Fatalf("iter%d: merged rows = %d, want %d (len=%d)", iter, rows, n0+n1, len(merged))
				}
			}
		})
	}
}
