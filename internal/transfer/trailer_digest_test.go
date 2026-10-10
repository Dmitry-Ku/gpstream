package transfer

import (
	"testing"
)

// TestTrailerDigestRoundTrip72 verifies the 72-byte Trailer payload
// (sha256 + per-row digest + rows) round-trips through the v2 frame encoding
// (P0: the mesh srcDigest/dstDigest wire form).
func TestTrailerDigestRoundTrip72(t *testing.T) {
	var sha, dig [32]byte
	for i := range sha {
		sha[i] = byte(i)
		dig[i] = byte(255 - i)
	}
	tr := Trailer{SHA256: sha, Digest: dig, Rows: 18234567}
	f := NewTrailerFrame(1, 5, 7, 2, tr)
	if len(f.Data) != TrailerPayloadLen || TrailerPayloadLen != 72 {
		t.Fatalf("trailer payload len = %d, want 72", len(f.Data))
	}
	b := f.Encode()
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Flags&FlagTrailer == 0 {
		t.Fatal("FlagTrailer not set")
	}
	gotTr, err := got.Trailer()
	if err != nil {
		t.Fatalf("trailer: %v", err)
	}
	if gotTr != tr {
		t.Errorf("trailer mismatch: got %+v, want %+v", gotTr, tr)
	}
}

// TestTrailerDigestRejectsOld40Payload verifies a 40-byte (pre-P0) payload is
// rejected rather than mis-parsed (no compat: there are no installations, and
// silently reading the wrong fields would be worse than a hard error).
func TestTrailerDigestRejectsOld40Payload(t *testing.T) {
	if _, err := DecodeTrailerPayload(make([]byte, 40)); err == nil {
		t.Fatal("40-byte payload accepted, want error")
	}
}

// TestContentDigestAccumulator exposes the raw per-row XOR accumulator of a
// completed binary-COPY stream and cross-checks it against the one-shot
// ContentDigest: sha256(acc || rows) must equal the combined digest's hex.
func TestContentDigestAccumulator(t *testing.T) {
	slice := testCopyStream() // 19B header + 3 rows + EOF marker
	d := NewContentDigest()
	if _, err := d.Write(slice); err != nil {
		t.Fatalf("write: %v", err)
	}
	acc, rows, err := d.Accumulator()
	if err != nil {
		t.Fatalf("accumulator: %v", err)
	}
	if rows != 3 {
		t.Fatalf("rows = %d, want 3", rows)
	}
	sum, err := d.Sum()
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	want := CombinedDigestString(acc, rows)
	if sum != want {
		t.Errorf("Sum() = %q, want %q (derived from Accumulator)", sum, want)
	}
}

// TestContentDigestAccumulatorTruncated verifies Accumulator errors on a
// stream that ends without the end-of-stream marker.
func TestContentDigestAccumulatorTruncated(t *testing.T) {
	slice := testCopyStream()
	d := NewContentDigest()
	// Drop the final 2 bytes (the EOF marker).
	if _, err := d.Write(slice[:len(slice)-2]); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := d.Accumulator(); err == nil {
		t.Fatal("truncated stream accepted, want error")
	}
}

// testCopyStream builds a small self-contained binary-COPY stream: 19-byte
// header + 3 rows (data / NULL / mixed) + the int16 -1 EOF marker.
func testCopyStream() []byte {
	b := []byte("PGCOPY\n\xff\r\n\x00")
	b = append(b, 0, 0, 0, 0) // remainingHeaderSize
	b = append(b, 0, 0, 0, 0) // flags
	// row 1: 2 fields (data, data)
	b = append(b, 0, 2)
	b = append(b, 0, 0, 0, 3)
	b = append(b, 'a', 'b', 'c')
	b = append(b, 0, 0, 0, 2)
	b = append(b, 'x', 'y')
	// row 2: 1 field (NULL)
	b = append(b, 0, 1)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)
	// row 3: 1 field (empty)
	b = append(b, 0, 1)
	b = append(b, 0, 0, 0, 0)
	// EOF marker
	b = append(b, 0xFF, 0xFF)
	return b
}
