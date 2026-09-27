package route

// routeMode-реши́тель (M2, T4b).
//
// Определяет, как данные из source-кластера (S primary-сегментов) попадают в
// dest-кластер (D primary-сегментов). ВАЖНО (ANSWER-T4B-FASTPATH-DISPROVEN-
// 2026-09-27): fast-path «fan-in D|S» и «congruence mod g» ОПРОВЕРГНУТЫ для
// non-legacy hash (REDUCE_JUMP_HASH) — JCH(h,S) mod D ≠ JCH(h,D) и
// JCH(h,S) mod g ≠ JCH(h,D) mod g при g>1. Поэтому в non-legacy достижимы
// ВСЕ пары (s,d), и корректных режимов ровно два:
//
//	direct_1to1 — S==D, ключи распределения равны, оба hash, та же мажорная
//	    версия ядра: segment = JCH(h, S) на обоих кластерах тождественно,
//	    s==d, потоки 1:1, 0 CPU на hash.
//	router_full — во всех остальных случаях: per-row hash по K_dest
//	    (JCH по ключу dest), eligible-сужение = ВСЕ S×D пар (без ложного
//	    конгруэнтного сужения).
//
// (Для legacy opclass (REDUCE_BITMASK/LAZYMOD) конгруэнция mod g работала бы,
// но gp_use_legacy_hashops — post-upgrade режим; v0.1 его не поддерживает —
// doctor проверит и отклонит.)

import (
	"fmt"
	"strings"
)

// Mode — режим маршрутизации source→dest.
type Mode string

const (
	// Direct1to1: S==D, ключи равны — поток с сегмента s идёт на сегмент s.
	Direct1to1 Mode = "direct_1to1"
	// RouterFull: per-row hash по K_dest; данные из s могут попасть на любой d.
	RouterFull Mode = "router_full"
)

// Pair — пара (source content, dest content), по которой идут данные.
type Pair struct {
	SrcContent int32 `json:"src_content"`
	DstContent int32 `json:"dst_content"`
}

// ModeResult — результат реши́теля.
type ModeResult struct {
	Mode  Mode
	Pairs []Pair // eligible-сужение: какие пары (s,d) реально несут данные
	G     int32  // gcd(S, D) — метаданные (для legacy); для JCH пар ВСЕ
}

// ModeInput — входы реши́теля.
type ModeInput struct {
	SrcSegments int32
	DstSegments int32
	// SrcDistribution/DstDistribution — из DDL (gp_distribution_policy):
	// "hash" (DISTRIBUTED BY), "randomly" (DISTRIBUTED RANDOMLY),
	// "replicated" (DISTRIBUTED REPLICATED).
	SrcDistribution string
	DstDistribution string
	// SrcHashKeys/DstHashKeys — ключи распределения (имена колонок в порядке
	// объявления; пустые для randomly/replicated).
	SrcHashKeys []string
	DstHashKeys []string
	// GPMajorEqual — мажорные версии ядра src/dest совпадают (hash-функция
	// non-legacy JCH одинакова внутри одной мажорной ветки).
	GPMajorEqual bool
}

// EqualKeys — ключи распределения равны (порядок значим: cdbhash обходит
// атрибуты по порядку; имена сравниваются без учёта регистра, т.к. GPDB
// складывает некавычируемые идентификаторы в нижний регистр).
func EqualKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strings.ToLower(a[i]) != strings.ToLower(b[i]) {
			return false
		}
	}
	return true
}

// ResolveMode — определяет режим маршрутизации (M2).
func ResolveMode(in ModeInput) (ModeResult, error) {
	if in.SrcSegments <= 0 {
		return ModeResult{}, fmt.Errorf("route: src segments %d must be > 0", in.SrcSegments)
	}
	if in.DstSegments <= 0 {
		return ModeResult{}, fmt.Errorf("route: dst segments %d must be > 0", in.DstSegments)
	}
	switch in.DstDistribution {
	case "hash":
		if len(in.DstHashKeys) == 0 {
			return ModeResult{}, fmt.Errorf("route: dst distribution=hash but no hash keys")
		}
	case "randomly", "replicated":
	// для randomly/replicated ключи не нужны
	default:
		return ModeResult{}, fmt.Errorf("route: unknown dst distribution %q", in.DstDistribution)
	}
	switch in.SrcDistribution {
	case "hash", "randomly", "replicated":
	default:
		return ModeResult{}, fmt.Errorf("route: unknown src distribution %q", in.SrcDistribution)
	}

	g := gcd32(uint32(in.SrcSegments), uint32(in.DstSegments))

	// direct_1to1: только когда оба кластера hash-распределены с РАВНЫМИ
	// ключами, S==D и одна мажорная версия (одинаковый JCH). Тогда s==d.
	if in.SrcSegments == in.DstSegments &&
		in.SrcDistribution == "hash" && in.DstDistribution == "hash" &&
		len(in.SrcHashKeys) > 0 && EqualKeys(in.SrcHashKeys, in.DstHashKeys) &&
		in.GPMajorEqual {
		pairs := make([]Pair, in.SrcSegments)
		for s := int32(0); s < in.SrcSegments; s++ {
			pairs[s] = Pair{SrcContent: s, DstContent: s}
		}
		return ModeResult{Mode: Direct1to1, Pairs: pairs, G: g}, nil
	}

	// Во всех остальных случаях — router_full: per-row hash по K_dest
	// (или без hash, если dest randomly/replicated — реальный приём определяет
	// план; eligible-сужение = ВСЕ S×D пар, т.к. JCH не даёт конгруэнции).
	pairs := make([]Pair, 0, in.SrcSegments*in.DstSegments)
	for s := int32(0); s < in.SrcSegments; s++ {
		for d := int32(0); d < in.DstSegments; d++ {
			pairs = append(pairs, Pair{SrcContent: s, DstContent: d})
		}
	}
	return ModeResult{Mode: RouterFull, Pairs: pairs, G: g}, nil
}

func gcd32(a, b uint32) int32 {
	for b != 0 {
		a, b = b, a%b
	}
	return int32(a)
}
