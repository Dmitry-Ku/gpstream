package manifest

// Behavior coverage for the flag decision points that previously had no test
// at all:
//
//   - wantsRoutedWrite: the P9 routed segment-to-segment data path vs the
//     merge fallback (the --no-routed-write escape hatch, manager.go).
//   - resolve: the conflict-action -> (in-place, truncate, verify) mapping
//     (the --truncate / --overwrite / --verify semantics, manager.go).
//
// Both are pure control-plane logic: no Run, no data path, no live cluster.

import (
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestWantsRoutedWrite: the routed sub-path is taken only when the plan proved
// router_full, the table is not a partition parent, the ON SEGMENT dispatch is
// deployed (SegBinary + SegListen set), and --no-routed-write is not passed.
// Every other combination degrades to the merge path.
func TestWantsRoutedWrite(t *testing.T) {
	cases := []struct {
		name   string
		mode   route.Mode
		part   pg.PartitionSpec
		segOK  bool // SegBinary + SegListen set
		noRW   bool // NoRoutedWrite
		want   bool
	}{
		{"routed by default (router_full, seg deployed)", route.RouterFull, pg.PartitionSpec{}, true, false, true},
		{"--no-routed-write forces the merge path", route.RouterFull, pg.PartitionSpec{}, true, true, false},
		{"partition parent never routes", route.RouterFull, pg.PartitionSpec{Spec: "PARTITION BY RANGE (d)"}, true, false, false},
		{"seg dispatch not deployed (no seg-listen)", route.RouterFull, pg.PartitionSpec{}, false, false, false},
		{"direct_1to1 is not a routed table", route.Direct1to1, pg.PartitionSpec{}, true, false, false},
		{"merge route is not a routed table", route.Merge, pg.PartitionSpec{}, true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newMgr(t, nil, nil, func(c *config.Config) {
				c.SegBinary = "/usr/bin/gpstream-segreceiver"
				c.SegListen = "10.0.0.1:9432"
				if !tc.segOK {
					c.SegListen = ""
				}
				c.NoRoutedWrite = tc.noRW
			})
			u := &Unit{RouteMode: tc.mode}
			if got := m.wantsRoutedWrite(u, tc.part); got != tc.want {
				t.Errorf("wantsRoutedWrite = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolveConflictAction: the conflict policy maps to the copy target.
// Only --truncate copies in-place into the final table (clearing it first);
// stop/skip/overwrite use the staging+swap path; --verify is orthogonal.
func TestResolveConflictAction(t *testing.T) {
	cases := []struct {
		name    string
		action  config.ConflictAction
		verify  config.VerifyMode
		inPlace bool
		trunc   bool
		wantVer bool
	}{
		{"stop (default): staging+swap, no verify", config.Stop, config.VerifyOff, false, false, false},
		{"skip: staging+swap", config.Skip, config.VerifyOff, false, false, false},
		{"overwrite: staging+swap (in-place copy target rejected)", config.Overwrite, config.VerifyOff, false, false, false},
		{"truncate: in-place + clear the final table", config.Truncate, config.VerifyOff, true, true, false},
		{"truncate + verify on: verify is orthogonal", config.Truncate, config.VerifyOn, true, true, true},
		{"stop + verify on: verify still orthogonal", config.Stop, config.VerifyOn, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newMgr(t, nil, nil, func(c *config.Config) {
				c.ConflictAction = tc.action
				c.Verify = tc.verify
			})
			inPlace, trunc, ver := m.resolve(&Unit{})
			if inPlace != tc.inPlace || trunc != tc.trunc || ver != tc.wantVer {
				t.Errorf("resolve = (inPlace=%v, truncate=%v, verify=%v), want (%v, %v, %v)",
					inPlace, trunc, ver, tc.inPlace, tc.trunc, tc.wantVer)
			}
		})
	}
}
