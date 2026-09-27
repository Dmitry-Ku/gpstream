package transfer

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"testing"
)

// --- helpers: build binary COPY streams (real PG 9.4.26 layout) ---

// copyHeader is the fixed 19-byte binary COPY header (no extension block).
func copyHeader() []byte {
	b := []byte("PGCOPY\n\xff\r\n\x00")
	var ext, flags [4]byte // 0,0
	b = append(b, ext[:]...)
	b = append(b, flags[:]...)
	return b
}

// appendRow appends one row: int16 nfields then (int32 len | bytes)*.
// nil field = NULL (len -1).
func appendRow(dst []byte, fields ...[]byte) []byte {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(fields)))
	dst = append(dst, n[:]...)
	for _, f := range fields {
		var l [4]byte
		if f == nil {
			binary.BigEndian.PutUint32(l[:], 0xFFFFFFFF) // -1
		} else {
			binary.BigEndian.PutUint32(l[:], uint32(len(f)))
		}
		dst = append(dst, l[:]...)
		if f != nil {
			dst = append(dst, f...)
		}
	}
	return dst
}

// copyEof is the end-of-stream marker (int16 -1).
func copyEof(dst []byte) []byte {
	var m [2]byte
	binary.BigEndian.PutUint16(m[:], 0xFFFF)
	return append(dst, m[:]...)
}

// genCopyStream builds a binary COPY stream with nRows random rows (mixed
// NULL/empty/small/medium fields, random lengths).
func genCopyStream(rnd *rand.Rand, nRows int) []byte {
	b := copyHeader()
	for i := 0; i < nRows; i++ {
		nf := rnd.Intn(6)
		if nf == 0 && rnd.Intn(4) == 0 {
			b = appendRow(b) // zero-field row (SELECT FROM t)
			continue
		}
		if nf == 0 {
			nf = 1
		}
		var fields [][]byte
		for j := 0; j < nf; j++ {
			roll := rnd.Intn(10)
			switch {
			case roll == 0:
				fields = append(fields, nil) // NULL
			case roll == 1:
				fields = append(fields, []byte{}) // empty
			default:
				l := rnd.Intn(4000)
				fb := make([]byte, l)
				rnd.Read(fb)
				fields = append(fields, fb)
			}
		}
		b = appendRow(b, fields...)
	}
	return copyEof(b)
}

// TestRowCounterBasic verifies counting on a small hand-built stream with
// NULL, empty, zero-field and large-field rows.
func TestRowCounterBasic(t *testing.T) {
	var b []byte
	b = append(b, copyHeader()...)
	b = appendRow(b, int32be(1), []byte("hi"), int32be(10))
	b = appendRow(b, nil, int32be(20))           // NULL first field
	b = appendRow(b, []byte{})                    // empty field
	b = appendRow(b)                              // zero-field row
	big := bytes.Repeat([]byte{0xAB}, 70000)
	b = appendRow(b, big)
	b = appendRow(b, int32be(3), []byte("x"), nil, int32be(4))
	b = copyEof(b)

	rc := NewRowCounter(bytes.NewReader(b))
	n, err := CountRows(io.Reader(rc))
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 6 {
		t.Fatalf("rows = %d, want 6", n)
	}
}

func int32be(v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return b[:]
}

// TestRowCounterTruncated verifies a stream cut before the EOF marker is
// flagged as bad (truncated), not silently accepted.
func TestRowCounterTruncated(t *testing.T) {
	b := genCopyStream(rand.New(rand.NewSource(1)), 50)
	// cut off the last 3 bytes (EOF marker + part of a row)
	cut := b[:len(b)-3]
	if _, err := CountRows(bytes.NewReader(cut)); err == nil {
		t.Fatal("truncated stream: expected error, got nil")
	}
}

// TestRowCounterBadSignature verifies a non-COPY stream is rejected.
func TestRowCounterBadSignature(t *testing.T) {
	if _, err := CountRows(bytes.NewReader([]byte("not a copy stream at all"))); err == nil {
		t.Fatal("bad signature: expected error, got nil")
	}
}

// TestRowCounterBadFieldLength verifies an impossible field length (less than
// -1) is rejected.
func TestRowCounterBadFieldLength(t *testing.T) {
	var b []byte
	b = append(b, copyHeader()...)
	var fc [2]byte
	binary.BigEndian.PutUint16(fc[:], 1)
	b = append(b, fc[:]...)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], 0xFFFFFFF0) // -16, invalid
	b = append(b, l[:]...)
	if _, err := CountRows(bytes.NewReader(b)); err == nil {
		t.Fatal("bad field length: expected error, got nil")
	}
}

// TestRowCounterFuzz cross-checks RowCounter against a byte-by-byte reference
// parser on many random streams. The reference parser is trivially correct
// (it indexes the whole stream) — any disagreement is a RowCounter bug.
func TestRowCounterFuzz(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < 200; i++ {
		nRows := rnd.Intn(400)
		stream := genCopyStream(rnd, nRows)
		// Reference: parse the whole stream structurally.
		ref := 0
		off := 19
		for off < len(stream) {
			fc := int16(binary.BigEndian.Uint16(stream[off:]))
			off += 2
			if fc == -1 {
				break
			}
			for j := 0; j < int(fc); j++ {
				ln := int32(binary.BigEndian.Uint32(stream[off:]))
				off += 4
				if ln >= 0 {
					off += int(ln)
				}
			}
			ref++
		}
		// RowCounter, read in awkwardly small chunks to stress the
		// incremental path (1-7 byte reads).
		rc := NewRowCounter(&chunkedReader{r: bytes.NewReader(stream), n: 1 + rnd.Intn(7)})
		got, err := CountRows(io.Reader(rc))
		if err != nil {
			t.Fatalf("iter %d: count: %v", i, err)
		}
		if got != int64(ref) {
			t.Fatalf("iter %d: rows = %d, want %d", i, got, ref)
		}
	}
}

// chunkedReader wraps r and serves at most n bytes per Read (to stress the
// incremental parser with partial structures).
type chunkedReader struct {
	r io.Reader
	n int
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(p) > c.n {
		p = p[:c.n]
	}
	return c.r.Read(p)
}

// TestRowCounterRoundTripThroughFramedPipe is a smoke that RowCounter sits in
// the data path: source -> RowCounter -> (buffer stand-in for the framed+
// compressed pipe) -> dest RowCounter, and the two row counts agree. The
// buffer is fully written and synchronised (channel) before the reader runs,
// so bytes.Buffer is never read and written concurrently (no -race false
// positive, no premature-EOF flake).
func TestRowCounterRoundTripThroughFramedPipe(t *testing.T) {
	stream := genCopyStream(rand.New(rand.NewSource(7)), 300)
	srcRC := NewRowCounter(bytes.NewReader(stream))

	var mid bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(&mid, srcRC)
		done <- err
	}()
	// Wait for the source side to finish writing before reading.
	if err := <-done; err != nil {
		t.Fatalf("source tee: %v", err)
	}
	if srcRC.Err() != nil {
		t.Fatalf("source tee error: %v", srcRC.Err())
	}

	dstRC := NewRowCounter(bytes.NewReader(mid.Bytes()))
	got, err := CountRows(dstRC)
	if err != nil {
		t.Fatalf("dest count: %v", err)
	}
	if srcRC.Rows() != got {
		t.Fatalf("src rows %d != dest rows %d", srcRC.Rows(), got)
	}
	if srcRC.Rows() == 0 {
		t.Fatal("expected non-zero rows")
	}
}

// TestRowCounterLargeFieldOverBuffer verifies a single field larger than the
// initial 512 KiB buffer is handled (buffer growth path) and counted once.
func TestRowCounterLargeFieldOverBuffer(t *testing.T) {
	big := make([]byte, 3*1024*1024) // 3 MiB field > 512 KiB initial buffer
	var b []byte
	b = append(b, copyHeader()...)
	b = appendRow(b, big)
	b = appendRow(b, []byte("after"))
	b = copyEof(b)
	n, err := CountRows(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
}

// TestRowCounterFormatRegression pins the real PG 9.4.26 binary COPY header
// (11-byte sig + int32 rem + int32 flags) so a future format change is caught
// here rather than silently.
func TestRowCounterFormatRegression(t *testing.T) {
	var b []byte
	b = append(b, copyHeader()...)
	b = appendRow(b, []byte("a"))
	b = copyEof(b)
	if !bytes.HasPrefix(b, []byte("PGCOPY\n\xff\r\n\x00")) {
		t.Fatal("header prefix changed")
	}
	if len(b) != 19+2+(4+1)+2 {
		// 19 hdr + row(int16 fc + int32 len + 1 byte) + eof(int16) = 28
		t.Fatalf("layout size drifted: len=%d", len(b))
	}
	n, err := CountRows(bytes.NewReader(b))
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want 1,nil", n, err)
	}
}

// BenchmarkRowCounter measures the pass-through+count rate. DESIGN §3.3 target
// is ≥ 2 GB/s/core; the benchmark reports GB/s (it does not fail the suite so
// slow CI hosts don't flake — a regression below target shows up in the
// benchmark delta).
func BenchmarkRowCounter(b *testing.B) {
	stream := genCopyStream(rand.New(rand.NewSource(9)), 200_000) // ~a few hundred MB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc := NewRowCounter(bytes.NewReader(stream))
		buf := make([]byte, 256*1024)
		for {
			if _, err := rc.Read(buf); err != nil {
				break
			}
		}
	}
	b.SetBytes(int64(len(stream)))
	b.ReportAllocs()
}
