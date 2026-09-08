package topology

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// fullCatalog is a 3-content cluster with mirrors plus a coordinator row.
// Columns: content, role, preferred_role, mode, status, hostname, port, datname.
var fullCatalog = [][]any{
	{-1, "p", "p", "s", "u", "coordinator.local", int64(5432), "gpstream"},
	{0, "p", "p", "s", "u", "seg0.local", int64(6000), "gpstream"},
	{0, "m", "m", "s", "u", "seg0m.local", int64(7000), "gpstream"},
	{1, "p", "p", "s", "u", "seg1.local", int64(6000), "gpstream"},
	{1, "m", "m", "n", "u", "seg1m.local", int64(7000), "gpstream"},
	{2, "p", "p", "s", "u", "seg2.local", int64(6000), "gpstream"},
}

// gpConn wires a FakeConn that answers both the probe and the catalog read.
// The probe must be registered under its full SQL: Query() resolves the
// longest matching substring, and the probe contains the catalog name.
func gpConn(t *testing.T) *pg.FakeConn {
	t.Helper()
	q := pg.NewFakeConn()
	q.On("to_regclass('gp_toolkit.gp_segment_configuration')", [][]any{{int32(16384)}})
	q.On("gp_segment_configuration", fullCatalog)
	return q
}

func TestReadFull(t *testing.T) {
	tt, err := Read(context.Background(), gpConn(t))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !tt.IsGreenplum() {
		t.Fatal("expected Greenplum topology")
	}
	if tt.Coordinator == nil || tt.Coordinator.Content != -1 {
		t.Fatalf("coordinator = %+v, want content=-1", tt.Coordinator)
	}
	if got := tt.SegmentCount(); got != 3 {
		t.Errorf("SegmentCount = %d, want 3", got)
	}
	if got := tt.UpCount(); got != 3 {
		t.Errorf("UpCount = %d, want 3", got)
	}
	// Segments sorted by (content, role): 0p, 0m, 1p, 1m, 2p.
	want := []string{"0p", "0m", "1p", "1m", "2p"}
	if len(tt.Segments) != len(want) {
		t.Fatalf("segments = %d, want %d", len(tt.Segments), len(want))
	}
	for i, w := range want {
		g := tt.Segments[i]
		if g.Content != int(w[0]-'0') || g.Role != string(w[1]) {
			t.Errorf("segment[%d] = %d/%s, want %s", i, g.Content, g.Role, w)
		}
	}
}

func TestReadNotGreenplum(t *testing.T) {
	q := pg.NewFakeConn()
	// NULL OID: plain PostgreSQL.
	q.On("to_regclass('gp_toolkit.gp_segment_configuration')", [][]any{{nil}})
	tt, err := Read(context.Background(), q)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if tt != nil {
		t.Fatalf("Read on plain postgres = %+v, want nil", tt)
	}
	if IsGreenplum(context.Background(), q) {
		t.Error("IsGreenplum = true, want false for NULL OID")
	}
}

func TestReadProbeError(t *testing.T) {
	q := pg.NewFakeConn() // nothing registered: probe query errors
	if IsGreenplum(context.Background(), q) {
		t.Error("IsGreenplum must be false on probe error")
	}
	tt, err := Read(context.Background(), q)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if tt != nil {
		t.Fatalf("Read on probe error = %+v, want nil", tt)
	}
}

func TestReadCatalogError(t *testing.T) {
	q := pg.NewFakeConn()
	q.On("to_regclass('gp_toolkit.gp_segment_configuration')", [][]any{{int32(16384)}})
	// catalog read unregistered -> query error
	tt, err := Read(context.Background(), q)
	if err == nil {
		t.Fatalf("Read with failing catalog query: want error, got %+v", tt)
	}
}

func TestUpCountPartialSync(t *testing.T) {
	tt := &Topology{Segments: []Segment{
		{Content: 0, Role: "p", Status: "u"},
		{Content: 1, Role: "p", Status: "d"}, // primary down
		{Content: 2, Role: "p", Status: "u"},
		{Content: 2, Role: "m", Status: "u"}, // mirror must not count
	}}
	if got := tt.UpCount(); got != 2 {
		t.Errorf("UpCount = %d, want 2", got)
	}
	if got := tt.SegmentCount(); got != 3 {
		t.Errorf("SegmentCount = %d, want 3", got)
	}
}

func TestSegmentDescription(t *testing.T) {
	for _, tc := range []struct {
		s    Segment
		want string
	}{{
		Segment{Content: 1, Role: "p", Mode: "s", Address: "10.0.0.3", Port: 6000},
		"content=1 primary synced 10.0.0.3:6000",
	}, {
		Segment{Content: 1, Role: "m", Mode: "n", Address: "10.0.0.9", Port: 7000},
		"content=1 mirror not-in-sync 10.0.0.9:7000",
	}} {
		if got := tc.s.Description(); got != tc.want {
			t.Errorf("Description = %q, want %q", got, tc.want)
		}
	}
}

func TestToIntCoercions(t *testing.T) {
	for _, tc := range []struct {
		v    any
		want int
	}{{int(5), 5}, {int32(6), 6}, {int64(7), 7}, {float64(8), 8},
		{true, 1}, {false, 0}, {"42", 42}, {"0", 0}, {"x", 0}, {nil, 0}, {byte('3'), 0}} {
		if got := toInt(tc.v); got != tc.want {
			t.Errorf("toInt(%v) = %d, want %d", tc.v, got, tc.want)
		}
	}
}

func TestToStrCoercions(t *testing.T) {
	for _, tc := range []struct {
		v    any
		want string
	}{{"abc", "abc"}, {[]byte("xyz"), "xyz"}, {nil, ""}, {7, "7"}} {
		if got := toStr(tc.v); got != tc.want {
			t.Errorf("toStr(%v) = %q, want %q", tc.v, got, tc.want)
		}
	}
}
