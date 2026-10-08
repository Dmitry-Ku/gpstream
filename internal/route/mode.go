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
//	    kernel version, non-legacy hashops: segment = JCH(h, S) identically
//	    on both clusters, s==d, streams 1:1, 0 CPU on hashing.
//	router_full — both sides hash-distributed with the same major + non-legacy
//	    hashops: per-row hash over K_dest (JCH over the dest key), eligible
//	    narrowing = ALL S×D pairs (no false congruence narrowing).
//	merge — a non-hash distribution on either side, or a major / hashops
//	    generation mismatch: no deterministic segment placement, the
//	    destination coordinator places the rows (always correct).
//
// (For the legacy opclass (REDUCE_BITMASK/LAZYMOD) congruence mod g would work,
// but gp_use_legacy_hashops is a post-upgrade mode; v0.1 does not support it —
// dry-run will check and reject it.)

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
	// Merge: the table's rows cannot be placed deterministically on the
	// destination segments (a non-hash distribution on either side, a major
	// or hashops-generation mismatch between the clusters). The data is
	// merged through the destination coordinator, which places the rows
	// itself — always correct, just not segment-to-segment.
	Merge Mode = "merge"
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
	// LegacyHashops — the SOURCE cluster runs with gp_use_legacy_hashops=on
	// (a post-upgrade mode): its cdbhash differs from the non-legacy JCH the
	// destination kernel expects, so the destination cannot reproduce the
	// source's placement. Both direct_1to1 and router_full require the
	// destination kernel to place rows identically to the source kernel
	// (a faithful copy keeps the source's distribution), so a legacy source
	// degrades to Merge (the destination places the rows itself).
	LegacyHashops bool
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

	// Segment-to-segment placement (direct_1to1 or router_full) requires the
	// DESTINATION kernel to place each row where the SOURCE kernel placed it:
	// the destination table is a faithful copy (the source's distribution
	// policy), and the data is written directly to the named segments
	// (utility COPY) — the destination kernel never re-hashes it. That holds
	// only when both kernels hash identically: the same major branch AND the
	// non-legacy hash generation (JCH). A major mismatch or a legacy source
	// degrades to Merge (the destination places the rows itself).
	sameHash := in.GPMajorEqual && !in.LegacyHashops
	// direct_1to1: only when both clusters are hash-distributed with EQUAL
	// keys, S==D and identical hashing. Then s==d.
	if sameHash &&
		in.SrcSegments == in.DstSegments &&
		in.SrcDistribution == "hash" && in.DstDistribution == "hash" &&
		len(in.SrcHashKeys) > 0 && EqualKeys(in.SrcHashKeys, in.DstHashKeys) {
		pairs := make([]Pair, in.SrcSegments)
		for s := int32(0); s < in.SrcSegments; s++ {
			pairs[s] = Pair{SrcContent: s, DstContent: s}
		}
		return ModeResult{Mode: Direct1to1, Pairs: pairs, G: g}, nil
	}
	// router_full: both sides hash-distributed (a routable source key) and
	// identical hashing: per-row hash over K_dest, eligible narrowing = ALL
	// S×D pairs (JCH gives no congruence narrowing).
	if sameHash &&
		in.SrcDistribution == "hash" && len(in.SrcHashKeys) > 0 &&
		in.DstDistribution == "hash" && len(in.DstHashKeys) > 0 {
		pairs := make([]Pair, 0, in.SrcSegments*in.DstSegments)
		for s := int32(0); s < in.SrcSegments; s++ {
			for d := int32(0); d < in.DstSegments; d++ {
				pairs = append(pairs, Pair{SrcContent: s, DstContent: d})
			}
		}
		return ModeResult{Mode: RouterFull, Pairs: pairs, G: g}, nil
	}
	// Merge: no deterministic segment-to-segment placement (non-hash
	// distribution on either side, or the kernels hash differently). The
	// destination coordinator places the rows; the S×D pair list is metadata
	// (nothing is routed per pair).
	pairs := make([]Pair, 0, in.SrcSegments*in.DstSegments)
	for s := int32(0); s < in.SrcSegments; s++ {
		for d := int32(0); d < in.DstSegments; d++ {
			pairs = append(pairs, Pair{SrcContent: s, DstContent: d})
		}
	}
	return ModeResult{Mode: Merge, Pairs: pairs, G: g}, nil
}

func gcd32(a, b uint32) int32 {
	for b != 0 {
		a, b = b, a%b
	}
	return int32(a)
}
