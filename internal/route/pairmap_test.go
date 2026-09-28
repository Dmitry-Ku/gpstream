package route

// M3: точная таблица пар (s → {d}) и мгновенный superset-бюнд.
//
// Ключевые факты (эмпирика 27.09):
//  1. JCH из cdbhash.c НЕ монотонен по h (перенос uint64-произведения) —
//     «интервальный» метод невозможен; точная таблица = перебор домена.
//  2. Свойство consistency (arXiv:1406.2294): D>S ⇒ d>=s, D<S ⇒ d<=s,
//     S==D ⇒ d=s — проверено перебором ВСЕГО домена для {2,4,6,8}^2.
//  3. Пары РЕДКИ: (4,6) — только 10 из 24 достижимы; точная таблица даёт
//     реальное сужение (экономия пайпов), а не просто конгруэнцию.

import (
	"testing"
)

// JCH не монотонен по h — фиксируем факт (опровергает интервальный метод).
func TestJCHNotMonotonic(t *testing.T) {
	// n=2: h=6 -> 1, h=7 -> 0 (проверено; также n=2 h=0..20: 0 0 0 0 1 1 1 0 ...)
	if JumpConsistentHash(6, 2) != 1 {
		t.Fatal("JCH(6,2) changed")
	}
	if JumpConsistentHash(7, 2) != 0 {
		t.Fatal("JCH(7,2) changed")
	}
}

func TestPairBounds(t *testing.T) {
	cases := []struct {
		s, S, D int32
		lo, hi  int32
	}{
		{0, 4, 6, 0, 5}, {3, 4, 6, 3, 5},
		{0, 6, 4, 0, 0}, {5, 6, 4, 0, 5}, {3, 6, 4, 0, 3},
		{2, 4, 4, 2, 2}, {0, 8, 2, 0, 0}, {7, 8, 2, 0, 7},
	}
	for _, c := range cases {
		lo, hi := PairBounds(c.s, c.S, c.D)
		if lo != c.lo || hi != c.hi {
			t.Errorf("PairBounds(%d,%d,%d) = [%d,%d], want [%d,%d]",
				c.s, c.S, c.D, lo, hi, c.lo, c.hi)
		}
	}
}

func TestPairMapIdentityWhenEqual(t *testing.T) {
	for _, n := range []int32{2, 3, 4, 8} {
		m := PairMap(n, n)
		for s := int32(0); s < n; s++ {
			if len(m[s]) != 1 || m[s][0] != s {
				t.Fatalf("n=%d: m[%d] = %v, want [%d]", n, s, m[s], s)
			}
		}
	}
}

// Consistency-свойство на ВСЕХ точных таблицах (встроенные + brute force):
// D>S => d>=s; D<S => d<=s.
func TestPairMapConsistency(t *testing.T) {
	counts := []int32{2, 4, 6, 8}
	for _, S := range counts {
		for _, D := range counts {
			if S == D {
				continue
			}
			m := PairMap(S, D)
			for s, ds := range m {
				if s < 0 || s >= S {
					t.Fatalf("S=%d D=%d: bad s=%d", S, D, s)
				}
				for _, d := range ds {
					if d < 0 || d >= D {
						t.Fatalf("S=%d D=%d: bad d=%d", S, D, d)
					}
					if D > S && d < s {
						t.Fatalf("S=%d D=%d: consistency d>=s violated: (%d,%d)", S, D, s, d)
					}
					if D < S && d > s {
						t.Fatalf("S=%d D=%d: consistency d<=s violated: (%d,%d)", S, D, s, d)
					}
				}
			}
		}
	}
}

// PairBounds — допустимая оценка: каждая точная пара внутри [lo, hi].
func TestPairMapWithinBounds(t *testing.T) {
	counts := []int32{2, 4, 6, 8}
	for _, S := range counts {
		for _, D := range counts {
			if S == D {
				continue
			}
			m := PairMap(S, D)
			for s, ds := range m {
				lo, hi := PairBounds(s, S, D)
				for _, d := range ds {
					if d < lo || d > hi {
						t.Fatalf("S=%d D=%d: pair (%d,%d) outside bounds [%d,%d]",
							S, D, s, d, lo, hi)
					}
				}
			}
		}
	}
}

// Точная таблица РЕЖЕ, чем superset: для (4,6) — 10 пар из 24
// (полный перебор домена, 27.09).
func TestPairMapSparsity(t *testing.T) {
	m := PairMap(4, 6)
	total := 0
	for _, ds := range m {
		total += len(ds)
	}
	if total != 10 {
		t.Fatalf("S=4 D=6: %d pairs, want 10 (full-domain brute force)", total)
	}
	want := map[int32][]int32{
		0: {0, 4, 5},
		1: {1, 4, 5},
		2: {2},
		3: {3, 4, 5},
	}
	for s, ds := range want {
		if len(m[s]) != len(ds) {
			t.Fatalf("s=%d: %v, want %v", s, m[s], ds)
		}
		for i := range ds {
			if m[s][i] != ds[i] {
				t.Fatalf("s=%d: %v, want %v", s, m[s], ds)
			}
		}
	}
}

// (Full-domain brute force for a non-golden pair moved to
// pairmap_heavy_test.go under the `heavy` tag: it sweeps 2^32 and is run on
// schedule in CI, not in the default pass — see ci.yml.)

func TestPairMapEmpty(t *testing.T) {
	if m := PairMap(0, 4); len(m) != 0 {
		t.Fatal("S=0 must give empty map")
	}
	if m := PairMap(4, 0); len(m) != 0 {
		t.Fatal("D=0 must give empty map")
	}
}
