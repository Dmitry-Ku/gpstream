package route

// Multi-attr hash (M1, T4b prep).
//
// CdbHash::cdbhash (src/backend/cdb/cdbhash.c, open-gpdb 6.30.1, non-legacy):
//
//	hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);  // rotl1
//	if (!isnull)
//		hashkey ^= hkey;   // hkey = hashfunc(datum) of the attribute
//
// i.e. for EVERY key attribute (in declaration order) — rotate-LEFT-1,
// and XOR hkey only for non-NULL. A NULL attribute contributes only rotl1.
//
// Validated on a live GPDB 6.30.1 (cluster A, 2 primary):
// t02_multi (a int, b text) DISTRIBUTED BY (a, b), 10000 rows,
// ~10% NULL in each column — 10000/10000 (multi_live_test.go).

// Combine — one step of cdbhash(): rotl1(hash) ^ hkey (hkey is ignored for NULL).
func Combine(hash, hkey uint32, isNull bool) uint32 {
	hash = (hash << 1) | ((hash >> 31) & 1)
	if !isNull {
		hash ^= hkey
	}
	return hash
}

// RouteMulti — the full pipeline for a multi-attr key:
// hash = 0; for each attribute (in key order):
//
//	hash = Combine(hash, HashPGAny(datum), isNull)
//
// segment = JumpConsistentHash(hash, numSegments).
//
// attrs — the byte representations of the datums (int4 = 4 bytes little-endian,
// text = raw bytes); nulls[i] — whether attribute i is NULL (attrs[i] may be nil).
// For a single non-NULL attribute it matches RouteInt4/HashPGAny (T0-2).
func RouteMulti(attrs [][]byte, nulls []bool, numSegments int32) int32 {
	var hash uint32
	for i, a := range attrs {
		if len(nulls) <= i {
			return JumpConsistentHash(uint64(hash), numSegments)
		}
		var hkey uint32
		if !nulls[i] && len(a) > 0 {
			hkey = HashPGAny(a)
		}
		hash = Combine(hash, hkey, nulls[i])
	}
	return JumpConsistentHash(uint64(hash), numSegments)
}
