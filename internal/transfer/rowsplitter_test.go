package transfer

import (
	"bytes"
	"io"
	"testing"
)

// limitedReader serves r in slices of at most limit bytes per Read, so the
// splitter is forced to buffer across many reads (the realistic case for a
// network stream). Used to prove the RowSplitter buffer stays bounded.
type limitedReader struct {
	r     io.Reader
	left  []byte
	limit int
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if len(l.left) == 0 {
		return 0, io.EOF
	}
	n := l.limit
	if n > len(l.left) {
		n = len(l.left)
	}
	copy(p, l.left[:n])
	l.left = l.left[n:]
	return n, nil
}

// TestRowSplitterBoundedBuffer — regression for R1-8 (01.10): the direct
// path OOM'd a 7.6G host because readChunk read the whole free capacity each
// call, leaving the buffer nearly full after every single-row commit and
// tripping the "nearly full → double capacity" growth on every NextRow, so
// the allocation ratcheted 512KB → 1MB → … → 512MB. The fix parses from
// buffered data before reading, so the buffer stays at ~one read chunk plus
// the in-flight row. This test streams ~20MB in 1KiB reads and asserts the
// buffer never balloons (the old code peaked near the whole 20MB).
func TestRowSplitterBoundedBuffer(t *testing.T) {
	const rows = 300000 // 300k rows × ~66B ≈ 20MB
	var src bytes.Buffer
	src.Write(copyHeader())
	for i := 0; i < rows; i++ {
		src.Write(binRow(int4BE(int32(i)), []byte("val-padding-to-fill-the-row")))
	}
	src.Write(copyTrailer())

	lr := &limitedReader{left: src.Bytes(), limit: 1024}
	rs := NewRowSplitter(lr)

	var got int64
	var maxCap int
	for {
		row, err := rs.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("NextRow: %v", err)
		}
		if len(row) == 0 {
			t.Fatal("empty row")
		}
		got++
		if cap(rs.buf) > maxCap {
			maxCap = cap(rs.buf)
		}
	}
	if got != rows {
		t.Fatalf("rows %d, want %d", got, rows)
	}
	// The buffer must stay near one read chunk + one row, not grow to the
	// whole stream. 16MiB is far above the ~1024B read + ~66B row but far
	// below the ~20MiB the buggy ratchet reached.
	if maxCap > 16*1024*1024 {
		t.Fatalf("RowSplitter buffer peaked at %d bytes; want bounded (<=16MiB)", maxCap)
	}
}

// TestRowSplitterHugeSingleField — a single field larger than the initial
// buffer must still be handled: readChunk grows the buffer to hold it (the
// growth path that legitimately exists), and the row is returned intact.
func TestRowSplitterHugeSingleField(t *testing.T) {
	big := make([]byte, 700*1024) // > 512KiB initial buffer
	for i := range big {
		big[i] = byte(i % 251)
	}
	var src bytes.Buffer
	src.Write(copyHeader())
	src.Write(binRow(int4BE(1), big))
	src.Write(copyTrailer())

	rs := NewRowSplitter(&src)
	row, err := rs.NextRow()
	if err != nil {
		t.Fatalf("NextRow: %v", err)
	}
	// row = fieldCount(2) + [len(4)+int4BE(1)(4)] + [len(4)+big(700KiB)]
	wantLen := 2 + 4 + 4 + 4 + len(big)
	if len(row) != wantLen {
		t.Fatalf("row len %d, want %d", len(row), wantLen)
	}
	if !bytes.Equal(row[14:], big) {
		t.Fatal("huge field not byte-identical")
	}
	if _, err := rs.NextRow(); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

// TestRowSplitterRowSplitAcrossReads — a single row spanning several read
// boundaries must be reassembled correctly (the splitter must hold a
// partial row until it is complete).
func TestRowSplitterRowSplitAcrossReads(t *testing.T) {
	var src bytes.Buffer
	src.Write(copyHeader())
	r1 := binRow(int4BE(5), []byte("abcdefg"))
	src.Write(r1)
	src.Write(copyTrailer())

	lr := &limitedReader{left: src.Bytes(), limit: 7} // tiny reads, mid-field
	rs := NewRowSplitter(lr)
	got, err := rs.NextRow()
	if err != nil {
		t.Fatalf("NextRow: %v", err)
	}
	if !bytes.Equal(got, r1) {
		t.Fatal("row split across reads not reassembled byte-identical")
	}
	if _, err := rs.NextRow(); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}
