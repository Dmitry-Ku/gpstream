package transfer

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/route"
)

// binRow renders one binary-COPY row: int16 fieldCount + (int32 len, data)*.
// A nil field = NULL (length -1, no data bytes).
func binRow(fields ...[]byte) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(len(fields)))
	for _, f := range fields {
		if f == nil {
			b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF) // -1 = NULL
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(f)))
		b = append(b, f...)
	}
	return b
}

// int4BE/int8BE render the big-endian (network-order) wire form of an
// integer, as PostgreSQL writes it in a binary-COPY stream.
func int4BE(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}
func int8BE(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// copyTrailer is the int16 -1 end-of-stream marker.
func copyTrailer() []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, 0xFFFF)
	return b
}

func TestRowSplitterRows(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(copyHeader())
	r1 := binRow(int4BE(1), []byte("a"))
	r2 := binRow(nil, []byte("bb")) // first field NULL
	r3 := binRow(int4BE(7), nil)    // second field NULL
	buf.Write(r1)
	buf.Write(r2)
	buf.Write(r3)
	buf.Write(copyTrailer())

	rs := NewRowSplitter(&buf)
	var got [][]byte
	for i := 0; i < 3; i++ {
		row, err := rs.NextRow()
		if err != nil {
			t.Fatalf("NextRow %d: %v", i, err)
		}
		fc := int(binary.BigEndian.Uint16(row[:2]))
		if fc != 2 {
			t.Fatalf("row %d: fieldCount %d, want 2", i, fc)
		}
		got = append(got, row)
	}
	// byte-identical pass-through
	if !bytes.Equal(got[0], r1) || !bytes.Equal(got[1], r2) || !bytes.Equal(got[2], r3) {
		t.Fatalf("rows not byte-identical to source rows")
	}
	// the next call must be io.EOF (marker seen, reader at EOF)
	if _, err := rs.NextRow(); err != io.EOF {
		t.Fatalf("expected io.EOF after last row, got %v", err)
	}
}

func TestRowSplitterTruncated(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(copyHeader())
	buf.Write(binRow(int4BE(1), []byte("a")))
	// no trailer
	if _, err := NewRowSplitter(&buf).NextRow(); err != nil {
		t.Fatalf("first row should parse: %v", err)
	}
	// (exhausting the second call is not asserted; the splitter must not panic)
}

func TestSplitRowFields(t *testing.T) {
	row := binRow(int4BE(42), []byte("x"), nil)
	data, isNull, err := SplitRowFields(row, []int{0, 2})
	if err != nil {
		t.Fatalf("SplitRowFields: %v", err)
	}
	if !bytes.Equal(data[0], int4BE(42)) {
		t.Errorf("key0 = %x, want %x", data[0], int4BE(42))
	}
	if !isNull[1] || data[1] != nil {
		t.Errorf("key2 should be NULL (isNull=%v data=%v)", isNull[1], data[1])
	}
	// out-of-range key
	if _, _, err := SplitRowFields(row, []int{5}); err == nil {
		t.Errorf("expected out-of-range key error")
	}
}

// TestHashKeyDatumGroundTruth — the live lab ground truth (B, D=2, 28.09):
// gp_segment_id for single-attr int4/int8/text keys. The R5 router must
// reproduce it exactly.
func TestHashKeyDatumGroundTruth(t *testing.T) {
	// int4: id -> seg (from the live cluster)
	i4 := map[int32]int32{1: 1, 2: 0, 7: 0, 12345: 1, 100000: 1, 199999: 0}
	for v, want := range i4 {
		hk, err := route.HashKeyDatum(route.OidInt4, int4BE(v))
		if err != nil {
			t.Fatalf("HashKeyDatum int4 %d: %v", v, err)
		}
		if got := route.JumpConsistentHash(uint64(route.Combine(0, hk, false)), 2); got != want {
			t.Errorf("int4 %d: seg %d, want %d", v, got, want)
		}
	}
	i8 := map[int64]int32{7919: 1, 15838: 1, 97760055: 0, 791900000: 0}
	for v, want := range i8 {
		hk, err := route.HashKeyDatum(route.OidInt8, int8BE(v))
		if err != nil {
			t.Fatalf("HashKeyDatum int8 %d: %v", v, err)
		}
		if got := route.JumpConsistentHash(uint64(route.Combine(0, hk, false)), 2); got != want {
			t.Errorf("int8 %d: seg %d, want %d", v, got, want)
		}
	}
	tx := map[string]int32{"k1": 1, "k2": 0, "k7": 0, "k12345": 1, "k100000": 1}
	for v, want := range tx {
		hk, err := route.HashKeyDatum(route.OidText, []byte(v))
		if err != nil {
			t.Fatalf("HashKeyDatum text %s: %v", v, err)
		}
		if got := route.JumpConsistentHash(uint64(route.Combine(0, hk, false)), 2); got != want {
			t.Errorf("text %s: seg %d, want %d", v, got, want)
		}
	}
	// unsupported type (e.g. numeric oid 1700) must error
	if _, err := route.HashKeyDatum(1700, []byte{0, 0, 0, 0}); err == nil {
		t.Errorf("expected unsupported-type error for oid 1700")
	}
}

// TestRowRouterRouting — end-to-end router: a source stream of (int4 key,
// text val) rows is routed to D=2 segment writers; the per-segment row bytes
// must be byte-identical to the source and land on the kernel's segment
// (ground truth). The per-segment output must be a valid binary-COPY stream.
func TestRowRouterRouting(t *testing.T) {
	const D = 2
	// ground-truth routing for single int4 key, D=2
	rows := []struct {
		id  int32
		val string
		seg int32
	}{
		{1, "a", 1},
		{2, "bb", 0},
		{7, "ccc", 0},
		{12345, "dddd", 1},
		{100000, "e", 1},
		{199999, "ff", 0},
	}
	var src bytes.Buffer
	src.Write(copyHeader())
	rawRows := make([][]byte, len(rows))
	for i, r := range rows {
		raw := binRow(int4BE(r.id), []byte(r.val))
		rawRows[i] = raw
		src.Write(raw)
	}
	src.Write(copyTrailer())

	var seg0, seg1 bytes.Buffer
	rr, err := NewRowRouter(&src, []int{0}, []uint32{route.OidInt4}, D,
		[]io.Writer{&seg0, &seg1})
	if err != nil {
		t.Fatalf("NewRowRouter: %v", err)
	}
	n, err := rr.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n != int64(len(rows)) {
		t.Fatalf("rows %d, want %d", n, len(rows))
	}

	// Each segment stream must be a valid binary-COPY: header + its rows +
	// trailer, rows byte-identical to the source.
	check := func(name string, out *bytes.Buffer, wantSeg int32) {
		b := out.Bytes()
		if !bytes.HasPrefix(b, copyHeader()) {
			t.Fatalf("%s: missing binary-COPY header", name)
		}
		if !bytes.HasSuffix(b, copyTrailer()) {
			t.Fatalf("%s: missing end-of-stream trailer", name)
		}
		body := b[len(copyHeader()) : len(b)-len(copyTrailer())]
		// walk the body, slicing each complete raw row (fieldCount + fields)
		// and comparing it verbatim to the source row that was routed here.
		pos := 0
		got := [][]byte{}
		for pos < len(body) {
			rowStart := pos
			fc := int(binary.BigEndian.Uint16(body[pos : pos+2]))
			if fc != 2 {
				t.Fatalf("%s: bad fieldCount %d", name, fc)
			}
			pos += 2
			for k := 0; k < fc; k++ {
				ln := int(int32(binary.BigEndian.Uint32(body[pos : pos+4])))
				pos += 4
				if ln == -1 {
					continue // NULL: no data bytes
				}
				pos += ln
			}
			got = append(got, body[rowStart:pos])
		}
		// the routed rows for this segment, in source order (verbatim)
		want := [][]byte{}
		for i, r := range rows {
			if r.seg == wantSeg {
				want = append(want, rawRows[i])
			}
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d rows, want %d", name, len(got), len(want))
		}
		for i := range got {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("%s: row %d = %x, want %x", name, i, got[i], want[i])
			}
		}
	}
	check("seg0", &seg0, 0)
	check("seg1", &seg1, 1)

	// per-segment counts
	ps := rr.PerSegment()
	if ps[0] != 3 || ps[1] != 3 {
		t.Fatalf("perSeg = %v, want [3 3]", ps)
	}
}

// TestRowRouterNullKey — a NULL distribution-key attribute contributes only a
// rotl1 (no XOR). The router must route it without error and the row must be
// forwarded intact (NULL field preserved as len -1).
func TestRowRouterNullKey(t *testing.T) {
	const D = 2
	var src bytes.Buffer
	src.Write(copyHeader())
	row := binRow(nil, []byte("v")) // key NULL, val present
	src.Write(row)
	src.Write(copyTrailer())
	var seg0, seg1 bytes.Buffer
	rr, err := NewRowRouter(&src, []int{0}, []uint32{route.OidInt4}, D, []io.Writer{&seg0, &seg1})
	if err != nil {
		t.Fatalf("NewRowRouter: %v", err)
	}
	n, err := rr.Run()
	if err != nil {
		t.Fatalf("Run (NULL key): %v", err)
	}
	if n != 1 {
		t.Fatalf("rows %d, want 1", n)
	}
	// exactly one segment got the row
	total := rr.PerSegment()[0] + rr.PerSegment()[1]
	if total != 1 {
		t.Fatalf("total routed %d, want 1", total)
	}
	// the row on that segment is byte-identical (NULL preserved)
	for name, out := range map[string]*bytes.Buffer{"seg0": &seg0, "seg1": &seg1} {
		b := out.Bytes()
		if len(b) <= len(copyHeader())+len(copyTrailer()) {
			continue
		}
		if !bytes.Contains(b, row) {
			t.Fatalf("%s: routed row not byte-identical (NULL lost?)", name)
		}
	}
}

// TestRowRouterUnsupportedKey — an unsupported dist-key type must fail fast at
// construction (the manager degrades to the coordinator path).
func TestRowRouterUnsupportedKey(t *testing.T) {
	var src bytes.Buffer
	if _, err := NewRowRouter(&src, []int{0}, []uint32{1700}, 2, []io.Writer{&src, &src}); err == nil ||
		!strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported-key-type error, got %v", err)
	}
}
