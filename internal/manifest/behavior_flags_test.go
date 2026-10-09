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
		name  string
		mode  route.Mode
		part  pg.PartitionSpec
		segOK bool // SegBinary + SegListen set
		noRW  bool // NoRoutedWrite
		want  bool
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

// TestResolveVerify: resolve() reports the post-copy content-verify decision
// (FR-02, --verify on). The two-agent data path fixes the copy target
// (staging + atomic swap) — there is no in-place / truncate split to decide,
// so resolve is a pure function of the operator's --verify flag, independent
// of the conflict action (2B-7).
func TestResolveVerify(t *testing.T) {
	cases := []struct {
		name    string
		action  config.ConflictAction
		verify  config.VerifyMode
		wantVer bool
	}{
		{"stop (default), verify off", config.Stop, config.VerifyOff, false},
		{"skip, verify off", config.Skip, config.VerifyOff, false},
		{"overwrite, verify off", config.Overwrite, config.VerifyOff, false},
		{"truncate, verify off", config.Truncate, config.VerifyOff, false},
		{"stop, verify on", config.Stop, config.VerifyOn, true},
		{"overwrite, verify on", config.Overwrite, config.VerifyOn, true},
		{"truncate, verify on", config.Truncate, config.VerifyOn, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newMgr(t, nil, nil, func(c *config.Config) {
				c.ConflictAction = tc.action
				c.Verify = tc.verify
			})
			ver := m.resolve(&Unit{})
			if ver != tc.wantVer {
				t.Errorf("resolve = %v, want %v", ver, tc.wantVer)
			}
		})
	}
}
