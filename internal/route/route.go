// Package route — a reproduction of GPDB 6.x hash distribution (T0-2).
//
// Validated on a live GPDB 6.30.1 (open-gpdb/Yandex): 2000/2000 rows of
// int4 DISTRIBUTED BY (id), 2 primary — 0 mismatches (T0-SPIKE-REPORT
// 2026-09-27, §T0-2).
//
// Algorithm (non-legacy, the default for a fresh cluster):
//  1. hash = 0
//  2. for each key attribute in order: hash = rotl1(hash);
//     hash ^= HashPGAny(datumBytes)  (NULL — no XOR)
//  3. segment = JumpConsistentHash(hash, numSegments)
//
// The rotl1 direction only matters for multi-attr (for a single attribute
// rot(0)=0) — cdbhash.c:191 (open-gpdb 6.30.1), see multi.go.
//
// Result = content (0..numSegments-1).
package route

// HashPGAny — PostgreSQL hash_any (src/backend/access/hash/hashfunc.c),
// little-endian. a=b=c=0x9e3779b9+len+3****95, word mixing, final-mix.
func HashPGAny(k []byte) uint32 {
	var a, b, c, l = uint32(0x9e3779b9) + uint32(len(k)) + 3923095,
		uint32(0x9e3779b9) + uint32(len(k)) + 3923095,
		uint32(0x9e3779b9) + uint32(len(k)) + 3923095,
		uint32(len(k))
	ka := k
	for l >= 12 {
		a += le32(ka[0:])
		b += le32(ka[4:])
		c += le32(ka[8:])
		mix(&a, &b, &c)
		ka = ka[12:]
		l -= 12
	}
	switch {
	case l >= 11:
		c += uint32(ka[10]) << 24
		fallthrough
	case l >= 10:
		c += uint32(ka[9]) << 16
		fallthrough
	case l >= 9:
		c += uint32(ka[8]) << 8
		fallthrough
	case l >= 8:
		b += le32(ka[4:])
		a += le32(ka[0:])
	case l >= 7:
		b += uint32(ka[6]) << 16
		fallthrough
	case l >= 6:
		b += uint32(ka[5]) << 8
		fallthrough
	case l >= 5:
		b += uint32(ka[4])
		fallthrough
	case l >= 4:
		a += le32(ka[0:])
	case l >= 3:
		a += uint32(ka[2]) << 16
		fallthrough
	case l >= 2:
		a += uint32(ka[1]) << 8
		fallthrough
	case l >= 1:
		a += uint32(ka[0])
	}
	final(&a, &b, &c)
	return c
}

// HashUint32 — hash_uint32, equivalent to HashPGAny(&k,4) without allocations.
func HashUint32(k uint32) uint32 {
	const s = uint32(0x9e3779b9) + 4 + 3923095
	a, b, c := s, s, s
	a += k
	final(&a, &b, &c)
	return c
}

// HashInt4 — cdbhash for a single int4 attribute (non-legacy).
func HashInt4(v int32) uint32 {
	var h uint32 // rotr1(0) = 0
	return h ^ HashUint32(uint32(v))
}

// HashInt8 — hashint8 (src/backend/access/hash/hashfunc.c):
// lohalf ^= (val>=0 ? hihalf : ~hihalf); return hash_uint32(lohalf).
func HashInt8(v int64) uint32 {
	var h uint32 // rotr1(0) = 0
	lo := uint32(v & 0xffffffff)
	hi := uint32(uint64(v) >> 32)
	if v >= 0 {
		lo ^= hi
	} else {
		lo ^= ^hi
	}
	return h ^ HashUint32(lo)
}

// JumpConsistentHash — jump consistent hash (arXiv:1406.2294), as in
// GPDB cdbhash.c. numSegments = the number of primaries (policy->numsegments).
//
// NOTE: the C source computes `j = (b+1) * ((double)(1<<31) / (double)((key>>33)+1))`
// with the multiplication done in DOUBLE and the truncation to int64 applied
// AFTER the product. Truncating the ratio first (integer multiply) diverges
// from C at higher segment counts (D>=4) — verified against live gp_segment_id
// on a 4-segment GP6 cluster.
func JumpConsistentHash(key uint64, numSegments int32) int32 {
	var b, j int64 = -1, 0
	for j < int64(numSegments) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64(uint64(key>>33)+1)))
	}
	return int32(b)
}

// RouteInt4 — the full pipeline: int4 key → content.
func RouteInt4(v int32, numSegments int32) int32 {
	return JumpConsistentHash(uint64(HashInt4(v)), numSegments)
}

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func rot(x, s uint32) uint32 { return (x << s) | (x >> (32 - s)) }

func mix(a, b, c *uint32) {
	*a -= *c
	*a ^= rot(*c, 4)
	*c += *b
	*b -= *a
	*b ^= rot(*a, 6)
	*a += *c
	*c -= *b
	*c ^= rot(*b, 8)
	*b += *a
	*a -= *c
	*a ^= rot(*c, 16)
	*c += *b
	*b -= *a
	*b ^= rot(*a, 19)
	*a += *c
	*c -= *b
	*c ^= rot(*b, 4)
	*b += *a
}

func final(a, b, c *uint32) {
	*c ^= *b
	*c -= rot(*b, 14)
	*a ^= *c
	*a -= rot(*c, 11)
	*b ^= *a
	*b -= rot(*a, 25)
	*c ^= *b
	*c -= rot(*b, 16)
	*a ^= *c
	*a -= rot(*c, 4)
	*b ^= *a
	*b -= rot(*a, 14)
	*c ^= *b
	*c -= rot(*b, 24)
}
