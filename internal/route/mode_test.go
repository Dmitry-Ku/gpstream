package route

import (
	"strings"
	"testing"
)

func TestResolveModeTable(t *testing.T) {
	hash1 := []string{"id"}
	hash2 := []string{"a", "b"}
	cases := []struct {
		name       string
		in         ModeInput
		wantMode   Mode
		wantPairs  int
		wantErr    string
		wantG      int32
		identPairs bool // пары = идентичность s->s
	}{
		{
			name:     "direct_1to1: S==D, keys equal, same major",
			in:       ModeInput{2, 2, "hash", "hash", hash1, hash1, true},
			wantMode: Direct1to1, wantPairs: 2, wantG: 2, identPairs: true,
		},
		{
			name:     "direct_1to1: keys case-insensitive",
			in:       ModeInput{4, 4, "hash", "hash", []string{"ID"}, []string{"id"}, true},
			wantMode: Direct1to1, wantPairs: 4, wantG: 4, identPairs: true,
		},
		{
			name:     "router_full: S==D but keys differ",
			in:       ModeInput{2, 2, "hash", "hash", hash1, []string{"other"}, true},
			wantMode: RouterFull, wantPairs: 4, wantG: 2,
		},
		{
			name:     "router_full: S!=D (4->2)",
			in:       ModeInput{4, 2, "hash", "hash", hash1, hash1, true},
			wantMode: RouterFull, wantPairs: 8, wantG: 2,
		},
		{
			name: "router_full: DISPROVEN fan-in (10->5) — no fast-path",
			in:   ModeInput{10, 5, "hash", "hash", hash1, hash1, true},
			wantMode: RouterFull, wantPairs: 50, wantG: 5,
		},
		{
			name: "router_full: DISPROVEN congruence (4->6, g=2) — no fast-path",
			in:   ModeInput{4, 6, "hash", "hash", hash1, hash1, true},
			wantMode: RouterFull, wantPairs: 24, wantG: 2,
		},
		{
			name:     "router_full: different major versions",
			in:       ModeInput{2, 2, "hash", "hash", hash1, hash1, false},
			wantMode: RouterFull, wantPairs: 4, wantG: 2,
		},
		{
			name:     "router_full: src randomly",
			in:       ModeInput{2, 2, "randomly", "hash", nil, hash1, true},
			wantMode: RouterFull, wantPairs: 4, wantG: 2,
		},
		{
			name:     "router_full: dst randomly",
			in:       ModeInput{2, 2, "hash", "randomly", hash1, nil, true},
			wantMode: RouterFull, wantPairs: 4, wantG: 2,
		},
		{
			name:     "router_full: multi-attr keys differ in order",
			in:       ModeInput{2, 2, "hash", "hash", hash2, []string{"b", "a"}, true},
			wantMode: RouterFull, wantPairs: 4, wantG: 2,
		},
		{
			name:    "err: zero src segments",
			in:      ModeInput{0, 2, "hash", "hash", hash1, hash1, true},
			wantErr: "src segments",
		},
		{
			name:    "err: zero dst segments",
			in:      ModeInput{2, 0, "hash", "hash", hash1, hash1, true},
			wantErr: "dst segments",
		},
		{
			name:    "err: dst hash without keys",
			in:      ModeInput{2, 2, "hash", "hash", hash1, nil, true},
			wantErr: "no hash keys",
		},
		{
			name:    "err: unknown dst distribution",
			in:      ModeInput{2, 2, "hash", "weird", hash1, hash1, true},
			wantErr: "unknown dst distribution",
		},
		{
			name:    "err: unknown src distribution",
			in:      ModeInput{2, 2, "weird", "hash", hash1, hash1, true},
			wantErr: "unknown src distribution",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ResolveMode(c.in)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want err containing %q, got %v", c.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got.Mode != c.wantMode {
				t.Fatalf("mode = %v, want %v", got.Mode, c.wantMode)
			}
			if len(got.Pairs) != c.wantPairs {
				t.Fatalf("pairs = %d, want %d", len(got.Pairs), c.wantPairs)
			}
			if got.G != c.wantG {
				t.Fatalf("g = %d, want %d", got.G, c.wantG)
			}
			if c.identPairs {
				for i, p := range got.Pairs {
					if p.SrcContent != int32(i) || p.DstContent != int32(i) {
						t.Fatalf("pair %d = %+v, want identity", i, p)
					}
				}
			}
			if c.wantMode == RouterFull {
				// все S×D пар ровно по одному разу
				seen := map[Pair]bool{}
				for _, p := range got.Pairs {
					if seen[p] {
						t.Fatalf("duplicate pair %+v", p)
					}
					seen[p] = true
					if p.SrcContent < 0 || p.SrcContent >= c.in.SrcSegments ||
						p.DstContent < 0 || p.DstContent >= c.in.DstSegments {
						t.Fatalf("pair out of range %+v", p)
					}
				}
			}
		})
	}
}

func TestResolveModeAllPairsCovered(t *testing.T) {
	// router_full при любом (S,D): покрытие = ровно S×D уникальных пар.
	for _, s := range []int32{1, 2, 3, 4, 6, 8, 10} {
		for _, d := range []int32{1, 2, 3, 4, 6, 8, 12} {
			got, err := ResolveMode(ModeInput{s, d, "hash", "hash", []string{"k"}, []string{"k"}, true})
			if err != nil {
				t.Fatalf("S=%d D=%d: %v", s, d, err)
			}
			if s == d && got.Mode != Direct1to1 {
				t.Fatalf("S=D=%d: mode %v, want direct_1to1", s, got.Mode)
			}
			if s != d && got.Mode != RouterFull {
				t.Fatalf("S=%d D=%d: mode %v, want router_full", s, d, got.Mode)
			}
			// direct_1to1: S идентичных пар (s->s); router_full: все S×D.
			wantPairs := int(s * d)
			if s == d {
				wantPairs = int(s)
			}
			if len(got.Pairs) != wantPairs {
				t.Fatalf("S=%d D=%d: pairs %d, want %d", s, d, len(got.Pairs), wantPairs)
			}
		}
	}
}

func TestEqualKeys(t *testing.T) {
	if !EqualKeys(nil, nil) {
		t.Fatal("nil==nil")
	}
	if EqualKeys([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("len differ")
	}
	if !EqualKeys([]string{"ID", "Name"}, []string{"id", "name"}) {
		t.Fatal("case-insensitive multi")
	}
	if EqualKeys([]string{"a", "b"}, []string{"b", "a"}) {
		t.Fatal("order matters")
	}
}
