package route

// Multi-attr hash (M1, T4b-подготовка).
//
// CdbHash::cdbhash (src/backend/cdb/cdbhash.c, open-gpdb 6.30.1, non-legacy):
//
//	hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);  // rotl1
//	if (!isnull)
//		hashkey ^= hkey;   // hkey = hashfunc(datum) атрибута
//
// т.е. на КАЖДЫЙ атрибут ключа (в порядке объявления) — rotate-LEFT-1,
// и XOR hkey только для не-NULL. NULL-атрибут вносит только rotl1.
//
// Валидировано на живом GPDB 6.30.1 (кластер A, 2 primary):
// t02_multi (a int, b text) DISTRIBUTED BY (a, b), 10000 строк,
// ~10% NULL в каждой колонке — 10000/10000 (multi_live_test.go).

// Combine — один шаг cdbhash(): rotl1(hash) ^ hkey (hkey для NULL игнорируется).
func Combine(hash, hkey uint32, isNull bool) uint32 {
	hash = (hash << 1) | ((hash >> 31) & 1)
	if !isNull {
		hash ^= hkey
	}
	return hash
}

// RouteMulti — полный пайплайн multi-attr ключа:
// hash = 0; для каждого атрибута (в порядке ключа):
//
//	hash = Combine(hash, HashPGAny(datum), isNull)
//
// segment = JumpConsistentHash(hash, numSegments).
//
// attrs — байтовые представления датумов (int4 = 4 байта little-endian,
// text = сырые байты); nulls[i] — NULL ли i-й атрибут (attrs[i] может быть nil).
// Для одного не-NULL атрибута совпадает с RouteInt4/HashPGAny (T0-2).
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
