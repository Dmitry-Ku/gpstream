package route

// routeMode resolver (M2, T4b).
//
// Determines how data from the source cluster (S primary segments) lands in
// the dest cluster (D primary segments). IMPORTANT (ANSWER-T4B-FASTPATH-DISPROVEN-
// 2026-09-27): fast-path "fan-in D|S" and "congruence mod g" are DISPROVEN for
// non-legacy hash (REDUCE_JUMP_HASH) — JCH(h,S) mod D ≠ JCH(h,D) and
// JCH(h,S) mod g ≠ JCH(h,D) mod g when g>1. So in non-legacy ALL
// pairs (s,d) are reachable, and exactly two modes are valid:
//
//	direct_1to1 — S==D, distribution keys equal, both hash, same major
//	    kernel version: segment = JCH(h, S) identically on both clusters,
//	    s==d, streams 1:1, 0 CPU on hashing.
//	router_full — in all other cases: per-row hash over K_dest
//	    (JCH over the dest key), eligible narrowing = ALL S×D pairs (no false
//	    congruence narrowing).
//
// (For the legacy opclass (REDUCE_BITMASK/LAZYMOD) congruence mod g would work,
// but gp_use_legacy_hashops is a post-upgrade mode; v0.1 does not support it —
// doctor will check and reject it.)

import (
	"fmt"
	"strings"
)

// Mode — the source→dest routing mode.
type Mode string

const (
	// Direct1to1: S==D, keys equal — stream from segment s goes to segment s.
	Direct1to1 Mode = "direct_1to1"
	// RouterFull: per-row hash over K_dest; data from s can land on any d.
	RouterFull Mode = "router_full"
)

// Pair — a (source content, dest content) pair over which data flows.
type Pair struct {
	SrcContent int32 `json:"src_content"`
	DstContent int32 `json:"dst_content"`
}

// ModeResult — the resolver's result.
type ModeResult struct {
	Mode  Mode
	Pairs []Pair // eligible narrowing: which (s,d) pairs actually carry data
	G     int32  // gcd(S, D) — metadata (for legacy); for JCH all pairs
}

// ModeInput — the resolver's inputs.
type ModeInput struct {
	SrcSegments int32
	DstSegments int32
	// SrcDistribution/DstDistribution — from DDL (gp_distribution_policy):
	// "hash" (DISTRIBUTED BY), "randomly" (DISTRIBUTED RANDOMLY),
	// "replicated" (DISTRIBUTED REPLICATED).
	SrcDistribution string
	DstDistribution string
	// SrcHashKeys/DstHashKeys — distribution keys (column names in declaration
	// order; empty for randomly/replicated).
	SrcHashKeys []string
	DstHashKeys []string
	// GPMajorEqual — src/dest kernel major versions match (the hash function
	// non-legacy JCH is the same within one major branch).
	GPMajorEqual bool
}

// EqualKeys — distribution keys are equal (order matters: cdbhash walks
// attributes in order; names are compared case-insensitively, since GPDB
// folds unquoted identifiers to lowercase).
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

// ResolveMode — determines the routing mode (M2).
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
	// keys are not needed for randomly/replicated
	default:
		return ModeResult{}, fmt.Errorf("route: unknown dst distribution %q", in.DstDistribution)
	}
	switch in.SrcDistribution {
	case "hash", "randomly", "replicated":
	default:
		return ModeResult{}, fmt.Errorf("route: unknown src distribution %q", in.SrcDistribution)
	}

	g := gcd32(uint32(in.SrcSegments), uint32(in.DstSegments))

	// direct_1to1: only when both clusters are hash-distributed with EQUAL
	// keys, S==D and one major version (same JCH). Then s==d.
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

	// In all other cases — router_full: per-row hash over K_dest
	// (or without hash, if dest is randomly/replicated — actual acceptance is
	// determined by the plan; eligible narrowing = ALL S×D pairs, since JCH gives no congruence).
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
