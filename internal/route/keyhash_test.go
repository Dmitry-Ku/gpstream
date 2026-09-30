package route

// Key-hash unit tests (R5-M1): HashKeyDatum dispatch, HashInt2, HashPGAny
// full-length sweep, HashInt8 negative path, RouteMulti short-nulls guard.
// Golden values were probed from the implementation itself and cross-
// validated against live GPDB 6.30.1 placements (route_test.go,
// r5_groundtruth_test.go); they pin the algorithm against regressions.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestHashKeyDatumDispatch(t *testing.T) {
	// int2: wire bytes are big-endian (network order).
	i2 := int16(-12345)
	if got, err := HashKeyDatum(OidInt2, []byte{0xCF, 0xC7}); err != nil || got != HashInt2(i2) {
		t.Fatalf("int2: got %x err %v, want %x", got, err, HashInt2(i2))
	}
	// int4.
	i4 := int32(0x11223344)
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(i4))
	if got, err := HashKeyDatum(OidInt4, b4); err != nil || got != HashInt4(i4) {
		t.Fatalf("int4: got %x err %v, want %x", got, err, HashInt4(i4))
	}
	// int8.
	i8 := int64(0x1122334455667788)
	b8 := make([]byte, 8)
	binary.BigEndian.PutUint64(b8, uint64(i8))
	if got, err := HashKeyDatum(OidInt8, b8); err != nil || got != HashInt8(i8) {
		t.Fatalf("int8: got %x err %v, want %x", got, err, HashInt8(i8))
	}
	// text / varchar / bytea: hash_any over the raw data bytes.
	for _, oid := range []uint32{OidText, OidVarchar, OidBytea} {
		if got, err := HashKeyDatum(oid, []byte("abc")); err != nil || got != HashPGAny([]byte("abc")) {
			t.Fatalf("oid %d: got %x err %v, want %x", oid, got, err, HashPGAny([]byte("abc")))
		}
	}
	// Goldens (pin the algorithm).
	if got, _ := HashKeyDatum(OidInt2, []byte{0xCF, 0xC7}); got != 0x0e13f103 {
		t.Errorf("int2 golden: got %x, want 0e13f103", got)
	}
	if got, _ := HashKeyDatum(OidText, []byte("abc")); got != 0xd12feb97 {
		t.Errorf("text golden: got %x, want d12feb97", got)
	}
	if got := HashInt8(-9876543210123); got != 0x22abf0bd {
		t.Errorf("int8 negative golden: got %x, want 22abf0bd", got)
	}
}

func TestHashKeyDatumErrors(t *testing.T) {
	// Malformed lengths for the fixed-width types.
	if _, err := HashKeyDatum(OidInt2, []byte{0x01}); !errors.Is(err, ErrShortKeyDatum) {
		t.Errorf("int2 short: err = %v, want ErrShortKeyDatum", err)
	}
	if _, err := HashKeyDatum(OidInt4, []byte{0x01, 0x02, 0x03}); !errors.Is(err, ErrShortKeyDatum) {
		t.Errorf("int4 short: err = %v, want ErrShortKeyDatum", err)
	}
	if _, err := HashKeyDatum(OidInt8, make([]byte, 7)); !errors.Is(err, ErrShortKeyDatum) {
		t.Errorf("int8 short: err = %v, want ErrShortKeyDatum", err)
	}
	// Unsupported key type (e.g. bool, oid 16): the router degrades the
	// table to the coordinator path — it must be reported, never guessed.
	if _, err := HashKeyDatum(16, []byte{0x01}); !errors.Is(err, ErrUnsupportedKeyType) {
		t.Errorf("unsupported: err = %v, want ErrUnsupportedKeyType", err)
	}
	if _, err := HashKeyDatum(1000, nil); !errors.Is(err, ErrUnsupportedKeyType) {
		t.Errorf("unsupported 1000: err = %v, want ErrUnsupportedKeyType", err)
	}
}

// HashPGAny must stay byte-exact with PG's hash_any for every key length
// (the tail switch has a case per length 1..11, plus the 12-byte loop for
// longer keys). Sweep 0..25 pins all tail cases and the loop.
func TestHashPGAnyLengthSweep(t *testing.T) {
	base := []byte("gpstream-cov-golden-key-1234567890")
	golden := map[int]uint32{
		0: 0xa7ea466d, 1: 0xd5d73257, 2: 0x756bb9cd, 3: 0x59da1449,
		4: 0x79c1bd0a, 5: 0x7a2a878b, 6: 0x41dc0cfa, 7: 0x2a194d5e,
		8: 0x00cc866d, 9: 0xef351ca4, 10: 0xda464370, 11: 0x3c902141,
		12: 0x17b3de1f, 13: 0x5e24ba65, 14: 0xe6936f9f, 15: 0xb621b743,
		16: 0x08349e10, 17: 0x8194a459, 18: 0x93413ecc, 19: 0xefc6f351,
		20: 0xd839b682, 21: 0x765aa549, 22: 0x29ef4bb1, 23: 0xa852e071,
		24: 0xbfe5c8b4, 25: 0x0a4813d7,
	}
	for n := 0; n <= 25; n++ {
		if got, want := HashPGAny(base[:n]), golden[n]; got != want {
			t.Errorf("len=%d: got %x, want %x", n, got, want)
		}
	}
}

// HashPGAny over a 12-byte block must equal HashUint32 of the same words'
// composition — instead pin consistency: the same bytes hashed as one 24-byte
// key (two loop iterations) differ from the 12-byte key (length feeds in),
// and the empty key is the length-0 constant.
func TestHashPGAnyLoopConsistency(t *testing.T) {
	if HashPGAny(nil) != HashPGAny([]byte{}) {
		t.Error("nil and empty key must hash identically")
	}
	k := bytes.Repeat([]byte("abc"), 8) // 24 bytes: two loop iterations
	if HashPGAny(k) == HashPGAny(k[:12]) {
		t.Error("24-byte key must differ from its 12-byte prefix (len feeds in)")
	}
}

// RouteMulti: a nulls slice shorter than attrs is malformed input — the
// router must not panic and must fall back to the hash accumulated so far
// (documented guard; the manager always passes matching lengths).
func TestRouteMultiShortNulls(t *testing.T) {
	// Two attributes, only one null flag: stops after the first attribute.
	got := RouteMulti([][]byte{[]byte("a"), []byte("b")}, []bool{false}, 4)
	want := JumpConsistentHash(uint64(Combine(0, HashPGAny([]byte("a")), false)), 4)
	if got != want {
		t.Errorf("short nulls: got seg %d, want %d", got, want)
	}
}

// PairCount: S==D identity, zero/invalid inputs, and golden-table pairs
// (no brute force).
func TestPairCount(t *testing.T) {
	if PairCount(0, 4) != 0 || PairCount(4, 0) != 0 || PairCount(-1, 2) != 0 {
		t.Error("invalid (S,D) must give 0 pairs")
	}
	if PairCount(2, 2) != 2 || PairCount(4, 4) != 4 || PairCount(8, 8) != 8 {
		t.Error("S==D must give S pairs")
	}
	// Golden-table sanity: counts must be positive and not exceed S*D.
	for _, s := range []int32{2, 4, 6, 8} {
		for _, d := range []int32{2, 4, 6, 8} {
			if s == d {
				continue
			}
			n := PairCount(s, d)
			if n <= 0 || n > int(s*d) {
				t.Errorf("PairCount(%d,%d) = %d, want in (0, %d]", s, d, n, int(s*d))
			}
		}
	}
	// Known sparsity: (4,6) has exactly 10 reachable pairs (27.09 empiric).
	if n := PairCount(4, 6); n != 10 {
		t.Errorf("PairCount(4,6) = %d, want 10", n)
	}
}

// sweepPairDomain + mergeSeen on a SMALL domain: the result must equal the
// pairs the brute force would produce for that domain (recomputed here
// directly from JumpConsistentHash). This exercises the worker sweep and the
// OR-merge without paying for the full 2^32 domain (that is the `heavy` test).
func TestSweepAndMergeSmallDomain(t *testing.T) {
	const S, D = int32(3), int32(5)
	const workers = 4
	const domain = 1 << 20 // small: fast, still exercises multi-worker merge

	local := make([][][]bool, workers)
	for w := range local {
		local[w] = make([][]bool, S)
		for i := range local[w] {
			local[w][i] = make([]bool, D)
		}
	}
	per := domain / workers
	for wk := 0; wk < workers; wk++ {
		lo := wk * per
		hi := lo + per
		if wk == workers-1 {
			hi = domain
		}
		sweepPairDomain(uint64(lo), uint64(hi), S, D, local[wk])
	}
	got := mergeSeen(local, S, D)

	// Reference: direct recompute over the same domain.
	want := make(map[int32]map[int32]bool, S)
	for h := 0; h < domain; h++ {
		s := JumpConsistentHash(uint64(h), S)
		d := JumpConsistentHash(uint64(h), D)
		if want[s] == nil {
			want[s] = make(map[int32]bool)
		}
		want[s][d] = true
	}
	if len(got) != len(want) {
		t.Fatalf("got %d s-entries, want %d", len(got), len(want))
	}
	for s, ds := range got {
		for _, d := range ds {
			if !want[s][d] {
				t.Errorf("pair (%d,%d) not in reference", s, d)
			}
		}
		for i := 1; i < len(ds); i++ {
			if ds[i-1] >= ds[i] {
				t.Errorf("s=%d not sorted: %v", s, ds)
			}
		}
	}
	for s, ds := range want {
		if len(got[s]) != len(ds) {
			t.Errorf("s=%d: got %d pairs, want %d", s, len(got[s]), len(ds))
		}
	}
}

// mergeSeen must be idempotent and total across workers: every pair marked in
// ANY worker table must appear exactly once in the merge.
func TestMergeSeenUnion(t *testing.T) {
	const S, D = int32(2), int32(3)
	local := make([][][]bool, 2)
	for w := range local {
		local[w] = make([][]bool, S)
		for i := range local[w] {
			local[w][i] = make([]bool, D)
		}
	}
	local[0][0][1] = true
	local[1][1][2] = true
	local[1][0][1] = true // duplicate of worker 0 — merge must not double it
	m := mergeSeen(local, S, D)
	if len(m[0]) != 1 || m[0][0] != 1 {
		t.Errorf("s=0: got %v, want [1]", m[0])
	}
	if len(m[1]) != 1 || m[1][0] != 2 {
		t.Errorf("s=1: got %v, want [2]", m[1])
	}
}
