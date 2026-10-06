package transfer

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
)

// ContentDigest returns a row-ORDER-INDEPENDENT digest of a PostgreSQL binary
// COPY payload (FR-02 content verification).
//
// Why not md5 of the raw payload: the data path copies a distributed source
// table (rows gathered from the source segments in an order that is NOT the
// physical order) into a destination staging table that is a plain heap (rows
// in COPY-FROM insertion order). Two physically-identical tables can therefore
// produce byte streams whose ROWS are permuted relative to each other. An md5
// of the raw stream is row-order-dependent, so identical data yields different
// digests and the copy is wrongly reported as a mismatch (the E1 e2e failure,
// 27.09). A correct content checksum must depend only on the SET of rows, not
// their order.
//
// The digest is the XOR of one SHA-256 per row (over the row's on-the-wire
// bytes, including its field-count and per-field length prefixes, so the value
// and column position of every field are covered) combined with the row count.
// XOR is commutative/associative, so the result is invariant to row order; the
// row count is folded in so two different multisets that happen to XOR to the
// same value are still distinguished.
//
// A payload that does not begin with the binary-COPY signature (test fakes use
// opaque payloads) is treated as an opaque blob: the whole payload is hashed.
// That path is only exercised by unit-test fakes; live data is always a valid
// binary COPY stream.
//
// Format: "<rows>:<hex>" where <hex> is 64 chars (the 32-byte digest).
// An empty table is "0:".
func ContentDigest(payload []byte) (string, error) {
	if len(payload) == 0 {
		return "0:", nil
	}
	if len(payload) < copySigLen+copyHdrLen ||
		string(payload[:copySigLen]) != copySignature {
		// Not a binary COPY stream (opaque test payload): hash the blob.
		h := sha256.Sum256(payload)
		return fmt.Sprintf("blob:%x", h), nil
	}
	pos := copySigLen + copyHdrLen // skip signature + int32 rem + int32 flags
	n := len(payload)
	var acc [32]byte
	var rows int64
	for {
		// Row: int16 fieldCount (or -1 end-of-stream). Include the fieldCount
		// in the per-row hash so the column structure is covered too.
		rowStart := pos
		if pos+2 > n {
			return "", fmt.Errorf("rowcount: truncated row header")
		}
		fc := int16(binary.BigEndian.Uint16(payload[pos:]))
		pos += 2
		if fc == -1 {
			break
		}
		if fc < 0 {
			return "", fmt.Errorf("rowcount: bad fieldCount %d", fc)
		}
		for i := int16(0); i < fc; i++ {
			if pos+4 > n {
				return "", fmt.Errorf("rowcount: truncated field length")
			}
			ln := int32(binary.BigEndian.Uint32(payload[pos:]))
			pos += 4
			if ln == -1 {
				continue // NULL field: no data bytes
			}
			if ln < 0 {
				return "", fmt.Errorf("rowcount: bad field length %d", ln)
			}
			if pos+int(ln) > n {
				return "", fmt.Errorf("rowcount: truncated field data")
			}
			pos += int(ln)
		}
		// Hash the raw on-the-wire bytes of this row (fieldCount + all fields).
		rh := sha256.Sum256(payload[rowStart:pos])
		for j := range acc {
			acc[j] ^= rh[j]
		}
		rows++
	}
	final := sha256.Sum256(append(acc[:], int64ToBytes(rows)...))
	return fmt.Sprintf("%d:%x", rows, final), nil
}

func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// NewContentDigest returns an io.Writer that consumes a binary-COPY payload
// incrementally (O(1) memory) and produces the SAME row-order-independent
// digest as ContentDigest (FR-02). pgx CopyTo streams the payload in chunks,
// so this is the correct shape for tables whose payload is larger than RAM
// (a 2.6 GB table OOM-killed the buffer-all variant, 29.09).
//
// Usage:
//
//	d := transfer.NewContentDigest()
//	rows, err := q.ReadCopy(ctx, d, sql) // or pgx CopyTo
//	sum, err := d.Sum()
//
// The digest format matches ContentDigest: "<rows>:<hex>" (or "blob:<hex>"
// for non-COPY payloads, "0:" for an empty payload).
func NewContentDigest() *ContentDigestWriter {
	return &ContentDigestWriter{}
}

type ContentDigestWriter struct {
	head []byte
	mode int // 0=header pending, 1=opaque blob, 2=rows
	blob hash.Hash
	acc  [32]byte
	rows int64
	buf  []byte
	eof  bool
}

func (w *ContentDigestWriter) Write(p []byte) (int, error) {
	n := len(p)
	if w.mode == 0 {
		w.head = append(w.head, p...)
		if len(w.head) < copySigLen+copyHdrLen {
			return n, nil
		}
		hdr := w.head[:copySigLen+copyHdrLen]
		rest := w.head[copySigLen+copyHdrLen:]
		w.head = nil
		if string(hdr[:copySigLen]) != copySignature {
			w.mode = 1
			w.blob = sha256.New()
			w.blob.Write(hdr)
			w.blob.Write(rest)
			return n, nil
		}
		w.mode = 2
		p = rest
	}
	if w.mode == 1 {
		w.blob.Write(p)
		return n, nil
	}
	w.buf = append(w.buf, p...)
	for {
		end, ok, err := rowEndOffset(w.buf)
		if err != nil {
			return 0, err
		}
		if !ok {
			break // row not fully buffered yet
		}
		if end == -1 {
			w.buf = nil
			w.eof = true
			break
		}
		rh := sha256.Sum256(w.buf[:end])
		for j := range w.acc {
			w.acc[j] ^= rh[j]
		}
		w.rows++
		rest := copy(w.buf, w.buf[end:])
		w.buf = w.buf[:rest]
	}
	return n, nil
}

// Sum finalizes the digest. It errors if the stream ended without the
// binary-COPY end marker (truncated payload).
func (w *ContentDigestWriter) Sum() (string, error) {
	switch w.mode {
	case 0:
		if len(w.head) == 0 {
			return "0:", nil
		}
		// The stream ended before the full COPY header was seen, so it can
		// never be a binary-COPY stream: treat it as an opaque blob, exactly
		// like ContentDigest does for payloads shorter than sig+header.
		h := sha256.Sum256(w.head)
		return fmt.Sprintf("blob:%x", h), nil
	case 1:
		return fmt.Sprintf("blob:%x", w.blob.Sum(nil)), nil
	case 2:
		if !w.eof {
			if len(w.buf) > 0 {
				return "", fmt.Errorf("digest: truncated copy stream (row data after end)")
			}
			return "", fmt.Errorf("digest: missing copy end marker")
		}
		final := sha256.Sum256(append(w.acc[:], int64ToBytes(w.rows)...))
		return fmt.Sprintf("%d:%x", w.rows, final), nil
	}
	return "", fmt.Errorf("digest: no payload")
}

// Accumulator exposes the RAW per-row XOR accumulator and row count of a
// completed binary-COPY stream (mode 2, end-of-stream marker seen). This is
// the uncombined value the Trailer carries in its Digest field: it is
// XOR-composable across slices, so the destination can XOR its per-slice
// accumulators and compare against the source's. For a non-COPY (opaque)
// stream or a stream that has not completed, it returns an error (the digest
// is only meaningful for a well-formed binary-COPY payload).
// CombinedDigestString renders the final combined content digest
// (sha256(acc || rows)) from a raw per-row XOR accumulator and a row count,
// in the same "<rows>:<hex>" form as ContentDigest / ContentDigestWriter.Sum.
// It is the inverse of the split used by the mesh: a unit's digest can be
// derived from the XOR of its per-slice accumulators (which is commutative /
// associative, so it is independent of the slice partition) plus the total
// row count.
func CombinedDigestString(acc [32]byte, rows int64) string {
	final := sha256.Sum256(append(acc[:], int64ToBytes(rows)...))
	return fmt.Sprintf("%d:%x", rows, final)
}

func (w *ContentDigestWriter) Accumulator() ([32]byte, int64, error) {
	var zero [32]byte
	if w.mode != 2 {
		return zero, 0, fmt.Errorf("digest: accumulator is only defined for a binary-COPY stream")
	}
	if !w.eof {
		if len(w.buf) > 0 {
			return zero, 0, fmt.Errorf("digest: truncated copy stream (row data after end)")
		}
		return zero, 0, fmt.Errorf("digest: missing copy end marker")
	}
	return w.acc, w.rows, nil
}

// rowEndOffset scans buf for the first complete row and returns the offset
// just past it. It returns (-1, true, nil) when the first entry is the
// end-of-stream marker, (0, false, nil) when the row is not fully present
// yet, and an error on malformed field counts/lengths (mirroring the
// validation of ContentDigest).
func rowEndOffset(buf []byte) (end int, ok bool, err error) {
	if len(buf) < 2 {
		return 0, false, nil
	}
	fc := int16(binary.BigEndian.Uint16(buf[0:2]))
	if fc == -1 {
		return -1, true, nil
	}
	if fc < 0 {
		return 0, false, fmt.Errorf("digest: bad fieldCount %d", fc)
	}
	pos := 2
	for i := int16(0); i < fc; i++ {
		if pos+4 > len(buf) {
			return 0, false, nil
		}
		ln := int32(binary.BigEndian.Uint32(buf[pos : pos+4]))
		pos += 4
		if ln == -1 {
			continue
		}
		if ln < 0 {
			return 0, false, fmt.Errorf("digest: bad field length %d", ln)
		}
		pos += int(ln)
		if pos > len(buf) {
			return 0, false, nil
		}
	}
	return pos, true, nil
}
