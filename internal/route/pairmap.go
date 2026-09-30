package route

// Точная таблица пар (M3, T4b): какие пары (src content, dst content)
// реально несут данные при роутинге s = JCH(h, S), d = JCH(h, D).
//
// ВАЖНО (эмпирика 27.09, pairmap_test.go): JCH из cdbhash.c НЕ монотонен по h
// (перенос uint64-произведения в формуле key_m), поэтому «интервальный»
// метод невозможен; точная таблица строится перебором домена h ∈ [0, 2^32).
//
// Свойство consistency (arXiv:1406.2294, подтверждено перебором ВСЕГО домена
// для (S,D) ∈ {2,4,6,8}^2, 0 нарушений): D > S ⇒ d >= s; D < S ⇒ d <= s;
// S == D ⇒ d = s. Это — мгновенная допустимая (superset) оценка, которой
// достаточно для отбора пайпов; точная таблица (меньше лишних пайпов) —
// PairMap.
//
// Стоимость точного перебора: ~20–50 с на ОДНУ пару (S,D) (8 воркеров, 27.09)
// — НЕ «~секунды», как предполагал план M3. Поэтому:
//   - встроенные точные таблицы для реалистичных GP6-счетов (2,4,6,8) —
//     pairmap_table.go, сгенерированы перебором всего домена;
//   - для других S/D — PairMap делает полный параллельный перебор (мемоизация);
//     вызывающий код должен учитывать, что это ~десятки секунд, и либо гонять
//     в фоне, либо ограничиваться PairBounds (мгновенный superset).

import (
	"sort"
	"sync"
)

// PairBounds — мгновенная (O(1)) допустимая оценка: диапазон d для данного s.
// D > S:  d ∈ [s, D-1] (все d >= s).
// D < S:  d ∈ [0, s] (все d <= s).
// S == D: d = s.
func PairBounds(s, S, D int32) (lo, hi int32) {
	switch {
	case D > S:
		return s, D - 1
	case D < S:
		return 0, s
	default:
		return s, s
	}
}

// PairMap — точное множество достижимых пар (s → отсортированный список d):
// {(JCH(h,S), JCH(h,D)) : h ∈ [0, 2^32)}.
//
// Порядок источников:
//  1. S == D — идентичность s->s (без вычислений);
//  2. встроенная таблица (pairmap_table.go, {2,4,6,8}) — мгновенно;
//  3. мемоизация предыдущего вычисления;
//  4. полный параллельный перебор (~20–50 с на 8 ядрах) — для остальных S/D.
func PairMap(S, D int32) map[int32][]int32 {
	if S < 1 || D < 1 {
		return map[int32][]int32{}
	}
	if S == D {
		m := make(map[int32][]int32, S)
		for s := int32(0); s < S; s++ {
			m[s] = []int32{s}
		}
		return m
	}
	if m, ok := goldenPairMap[S]; ok {
		if dm, ok := m[D]; ok {
			return dm
		}
	}
	return pairMapBruteForce(S, D)
}

// PairCount — число достижимых пар (s,d) для (S,D): S==D -> S; S,D>=1 ->
// len(PairMap); иначе 0.
func PairCount(S, D int32) int {
	if S < 1 || D < 1 {
		return 0
	}
	if S == D {
		return int(S)
	}
	n := 0
	for _, ds := range PairMap(S, D) {
		n += len(ds)
	}
	return n
}

var (
	pairMapMu sync.Mutex
	pairMapM  = map[[2]int32]map[int32][]int32{}
)

// pairMapBruteForce — полный перебор h ∈ [0, 2^32), параллельно (8 воркеров).
// ДОРОГО (~20–50 с): мемоизируется, но вызывающий код должен понимать
// стоимость и не гонять синхронно в критическом пути.
func pairMapBruteForce(S, D int32) map[int32][]int32 {
	key := [2]int32{S, D}
	pairMapMu.Lock()
	if m, ok := pairMapM[key]; ok {
		pairMapMu.Unlock()
		return m
	}
	pairMapMu.Unlock()

	const workers = 8
	// Каждый worker пишет в СВОЮ таблицу (раздельные backing-массивы —
	// race-free), затем слияние OR. Общий seen запрещён: concurrent-запись
	// в один массив = data race (проверено -race).
	local := make([][][]bool, workers)
	for w := range local {
		local[w] = make([][]bool, S)
		for i := range local[w] {
			local[w][i] = make([]bool, D)
		}
	}
	var wg sync.WaitGroup
	per := (1 << 32) / uint64(workers)
	for wk := 0; wk < workers; wk++ {
		lo := uint64(wk) * per
		hi := lo + per
		if wk == workers-1 {
			hi = 1 << 32
		}
		wg.Add(1)
		go func(wk int, lo, hi uint64) {
			defer wg.Done()
			sweepPairDomain(lo, hi, S, D, local[wk])
		}(wk, lo, hi)
	}
	wg.Wait()

	m := mergeSeen(local, S, D)
	pairMapMu.Lock()
	pairMapM[key] = m
	pairMapMu.Unlock()
	return m
}

// sweepPairDomain — one worker's share of the h-domain [lo, hi): marks the
// (s,d) pairs it sees. Split out of pairMapBruteForce so the sweep and the
// merge are unit-testable on small domains (the full 2^32 sweep stays in
// the `heavy` tag).
func sweepPairDomain(lo, hi uint64, S, D int32, seen [][]bool) {
	for h := lo; h < hi; h++ {
		seen[JumpConsistentHash(h, S)][JumpConsistentHash(h, D)] = true
	}
}

// mergeSeen — OR-merge of the per-worker seen tables into the final
// (s -> sorted [d...]) map.
func mergeSeen(local [][][]bool, S, D int32) map[int32][]int32 {
	seen := local[0]
	for w := 1; w < len(local); w++ {
		for s := int32(0); s < S; s++ {
			for d := int32(0); d < D; d++ {
				if local[w][s][d] {
					seen[s][d] = true
				}
			}
		}
	}
	m := make(map[int32][]int32, S)
	for s := int32(0); s < S; s++ {
		for d := int32(0); d < D; d++ {
			if seen[s][d] {
				m[s] = append(m[s], d)
			}
		}
		sort.Slice(m[s], func(i, j int) bool { return m[s][i] < m[s][j] })
	}
	return m
}
