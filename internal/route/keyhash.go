package route

import "fmt"

// Key-hash dispatch (R5-M1).
//
// cdbhash() combines each distribution-key attribute as
//   hash = rotl1(hash) ^ hashfunc(datum)          (NULL: rotl1 only)
// where hashfunc is the type's own hash function. The datum arrives from a
// binary-COPY stream in the type's WIRE encoding, so before hashing we must
// convert wire bytes to the value the kernel's hash function actually takes:
//
//   - integer types are stored in the stream as big-endian (network order);
//     the kernel hashes the NATIVE integer via hash_uintN (not hash_any over
//     the 4/8 bytes). We parse the wire bytes to the native value and call
//     hash_uintN.
//   - varlena / raw types (text, varchar, bytea) are stored as their raw
//     data bytes and hashed with hash_any over those exact bytes.
//
// Only the key types below are supported in R5-M1. Any other distribution-key
// type makes the table fall back to the coordinator path (the router reports
// an unsupported type and the manager degrades gracefully) — we never guess a
// hash, because a wrong hash would silently mis-route rows to the wrong
// segment (a data-placement bug, worse than a slower copy).

// ErrUnsupportedKeyType is returned for a distribution-key type R5-M1 does not
// hash. The caller degrades the table to the coordinator path.
var ErrUnsupportedKeyType = fmt.Errorf("route: unsupported distribution-key type for direct copy")

// ErrShortKeyDatum is returned when a key field's wire bytes have an unexpected
// length for its type.
var ErrShortKeyDatum = fmt.Errorf("route: key datum has unexpected length for its type")

// KeyOIDs (PostgreSQL pg_type oid).
const (
	OidInt2    = 21
	OidInt4    = 23
	OidInt8    = 20
	OidText    = 25
	OidVarchar = 1043
	OidBytea   = 17
)

// hashUint16 == hash_uint32(uint32(v)) (PG hashfunc.c).
func hashUint16(v uint16) uint32 { return HashUint32(uint32(v)) }

// HashInt2 — cdbhash for one int2 attribute (non-legacy).
func HashInt2(v int16) uint32 { return HashUint32(uint32(uint16(v))) }

// HashKeyDatum converts a distribution-key field's binary-COPY wire bytes to
// the hkey the kernel's cdbhash() would XOR in, dispatching on the type OID.
// data is the field's data bytes (absent for NULL; the caller passes the
// isNull flag separately and should not call this for a NULL key).
//
// It returns ErrUnsupportedKeyType for a type R5-M1 does not hash and
// ErrShortKeyDatum for a malformed length.
func HashKeyDatum(oid uint32, data []byte) (uint32, error) {
	switch oid {
	case OidInt2:
		if len(data) != 2 {
			return 0, ErrShortKeyDatum
		}
		// big-endian (network order) on the wire.
		v := int16(uint16(data[0])<<8 | uint16(data[1]))
		return HashInt2(v), nil
	case OidInt4:
		if len(data) != 4 {
			return 0, ErrShortKeyDatum
		}
		v := int32(uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3]))
		return HashInt4(v), nil
	case OidInt8:
		if len(data) != 8 {
			return 0, ErrShortKeyDatum
		}
		var v int64
		for i := 0; i < 8; i++ {
			v = v<<8 | int64(data[i])
		}
		return HashInt8(v), nil
	case OidText, OidVarchar, OidBytea:
		// varlena / raw: hash_any over the exact data bytes.
		return HashPGAny(data), nil
	default:
		return 0, fmt.Errorf("%w: oid %d", ErrUnsupportedKeyType, oid)
	}
}
