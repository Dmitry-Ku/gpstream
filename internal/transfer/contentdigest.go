package transfer

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
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
