package agent

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer"
)

// buildBinaryCopyPayload builds a valid PostgreSQL binary-COPY payload from
// rows of field data (field value == nil means NULL). It mirrors the
// on-the-wire format the source streams (COPY ... TO STDOUT (FORMAT binary)):
// signature + int32 remainingHeaderSize(0) + int32 flags(0) + per row: int16
// field count + per field int32 length (-1 == NULL) + data, terminated by an
// int16 -1 end-of-stream marker.
func buildBinaryCopyPayload(t *testing.T, rows [][][]byte) []byte {
	t.Helper()
	var b []byte
	// signature (11 bytes) + int32 remainingHeaderSize (0) + int32 flags (0).
	b = append(b, []byte("PGCOPY\n\xff\r\n\x00")...)
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], 0)
	binary.BigEndian.PutUint32(hdr[4:8], 0)
	b = append(b, hdr[:]...)
	for _, row := range rows {
		var fc [2]byte
		binary.BigEndian.PutUint16(fc[:], uint16(len(row)))
		b = append(b, fc[:]...)
		for _, f := range row {
			var ln [4]byte
			if f == nil {
				binary.BigEndian.PutUint32(ln[:], 0xFFFFFFFF)
				b = append(b, ln[:]...)
				continue
			}
			binary.BigEndian.PutUint32(ln[:], uint32(len(f)))
			b = append(b, ln[:]...)
			b = append(b, f...)
		}
	}
	var eom [2]byte
	binary.BigEndian.PutUint16(eom[:], 0xFFFF) // -1: end of stream
	return append(b, eom[:]...)
}

// TestP9EInlineDigestMatchesBuffered is the P9-E (FR-02) core invariant:
// the content digest computed ON THE DATA PATH — the source's writer tee
// (digestTee) and the destination's reader tee (digestReader) — equals the
// buffered ContentDigest of the same payload, and the two sides agree with
// EACH OTHER even when the bytes arrive in different chunk sizes (the source
// streams from pgx in one chunking, the destination reads the framed stream
// in another). This is what lets the manager verify by comparing two inline
// digests instead of re-reading either table.
func TestP9EInlineDigestMatchesBuffered(t *testing.T) {
	// Build a payload with mixed field widths + NULLs + empty fields.
	var rows [][][]byte
	for i := 0; i < 500; i++ {
		var field [4]byte
		binary.BigEndian.PutUint32(field[:], uint32(i*i))
		s := make([]byte, 1+i%16)
		if _, err := rand.Read(s); err != nil {
			t.Fatalf("rand: %v", err)
		}
		row := [][]byte{field[:], s}
		if i%7 == 0 {
			row = append(row, nil) // a NULL field
		}
		if i%11 == 0 {
			row = append(row, []byte{}) // an empty (non-NULL) field
		}
		rows = append(rows, row)
	}
	payload := buildBinaryCopyPayload(t, rows)

	// Reference: the buffered, order-independent digest of the whole payload.
	ref, err := transfer.ContentDigest(payload)
	if err != nil {
		t.Fatalf("ContentDigest: %v", err)
	}

	// Source side: the writer tee, fed in small random chunks.
	src := transfer.NewContentDigest()
	tee := &digestTee{next: io.Discard, d: src}
	off := 0
	for off < len(payload) {
		chunk := 1 + (off*7919)%13
		if off+chunk > len(payload) {
			chunk = len(payload) - off
		}
		if _, err := tee.Write(payload[off : off+chunk]); err != nil {
			t.Fatalf("digestTee.Write: %v", err)
		}
		off += chunk
	}
	srcSum, err := src.Sum()
	if err != nil {
		t.Fatalf("source inline Sum: %v", err)
	}

	// Destination side: the reader tee, draining in a different chunk size
	// (io.Copy uses a 32 KiB buffer — a different boundary than the source).
	dst := transfer.NewContentDigest()
	dr := &digestReader{next: dst, src: bytes.NewReader(payload)}
	if _, err := io.Copy(io.Discard, dr); err != nil {
		t.Fatalf("drain digestReader: %v", err)
	}
	dstSum, err := dst.Sum()
	if err != nil {
		t.Fatalf("dest inline Sum: %v", err)
	}

	if srcSum != ref {
		t.Fatalf("source inline digest %q != buffered %q", srcSum, ref)
	}
	if dstSum != ref {
		t.Fatalf("dest inline digest %q != buffered %q", dstSum, ref)
	}
	if srcSum != dstSum {
		t.Fatalf("source %q != dest %q (inline digests must match across chunkings)", srcSum, dstSum)
	}
	t.Logf("P9-E inline digest == buffered digest == %q (rows=%d bytes=%d)", srcSum, len(rows), len(payload))
}

// TestP9EInlineDigestDetectsMismatch verifies the negative control: flipping
// a single bit in one field on the destination side changes its inline
// digest (so the manager's compare catches a corrupted copy), while the
// source's digest is unchanged.
func TestP9EInlineDigestDetectsMismatch(t *testing.T) {
	var rows [][][]byte
	for i := 0; i < 200; i++ {
		var field [4]byte
		binary.BigEndian.PutUint32(field[:], uint32(i))
		rows = append(rows, [][]byte{field[:], []byte{byte(i), byte(i >> 8)}})
	}
	payload := buildBinaryCopyPayload(t, rows)
	srcSum, err := transfer.ContentDigest(payload)
	if err != nil {
		t.Fatalf("ContentDigest: %v", err)
	}

	// Corrupt one byte in the middle of the payload (a field data byte).
	corrupted := append([]byte(nil), payload...)
	corrupted[len(corrupted)/2] ^= 0x01

	dstSum, err := transfer.ContentDigest(corrupted)
	if err != nil {
		t.Fatalf("ContentDigest(corrupted): %v", err)
	}
	if srcSum == dstSum {
		t.Fatalf("corrupted payload produced the same digest %q — mismatch not detected", srcSum)
	}
	t.Logf("mismatch detected: %q -> %q", srcSum, dstSum)
}
