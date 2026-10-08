package topology

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// fullCatalog is a 3-content cluster with mirrors plus a coordinator row.
// Columns: content, role, preferred_role, mode, status, hostname, port —
// matching the open-gpdb 6.x gp_segment_configuration (no datname column).
var fullCatalog = [][]any{
	{-1, "p", "p", "s", "u", "coordinator.local", int64(5432)},
	{0, "p", "p", "s", "u", "seg0.local", int64(6000)},
	{0, "m", "m", "s", "u", "seg0m.local", int64(7000)},
	{1, "p", "p", "s", "u", "seg1.local", int64(6000)},
	{1, "m", "m", "n", "u", "seg1m.local", int64(7000)},
	{2, "p", "p", "s", "u", "seg2.local", int64(6000)},
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

// TestReadProbeReturnsRegclassString — regression for the live GP6 dry-run
// "could not enumerate segments" FAIL. pgx decodes a bare to_regclass as its
// TEXT form (the type name) in the text protocol, NOT a numeric OID. The old
// IsGreenplum did toInt(oid) > 0, so toInt("gp_segment_configuration") == 0
// mis-detected a real cluster as plain-PG and Read returned (nil, nil).
func TestReadProbeReturnsRegclassString(t *testing.T) {
	q := pg.NewFakeConn()
	// Real pgx text-protocol decode: the regclass comes back as its name.
	q.On("to_regclass('gp_toolkit.gp_segment_configuration')",
		[][]any{{"gp_segment_configuration"}})
	q.On("gp_segment_configuration", fullCatalog)

	if !IsGreenplum(context.Background(), q) {
		t.Fatal("IsGreenplum = false, want true (probe decoded as a regclass string)")
	}
	tt, err := Read(context.Background(), q)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if tt == nil || !tt.IsGreenplum() {
		t.Fatalf("Read = %+v, want a Greenplum topology", tt)
	}
	if got := tt.SegmentCount(); got != 3 {
		t.Errorf("SegmentCount = %d, want 3", got)
	}
}

// TestCatalogSQLColumnsExistOnTargetKernel — regression guard: every column
// in catalogSQL must exist in the open-gpdb 6.x gp_segment_configuration.
// A stray column (the historical `datname`, absent on this build) makes the
// whole query error and dry-run reports "could not enumerate segments".
func TestCatalogSQLColumnsExistOnTargetKernel(t *testing.T) {
	// Columns present on open-gpdb 6.x gp_segment_configuration (verified
	// live): address, content, datadir, dbid, hostname, mode, port,
	// preferred_role, role, status. datname is NOT among them.
	present := map[string]bool{
		"address": true, "content": true, "datadir": true, "dbid": true,
		"hostname": true, "mode": true, "port": true,
		"preferred_role": true, "role": true, "status": true,
	}
	// Pull the column list out of catalogSQL (the SELECT ... FROM part).
	head := catalogSQL[:strings.Index(catalogSQL, "FROM")]
	for _, col := range strings.FieldsFunc(head,
		func(r rune) bool { return r == ',' || r == ' ' || r == '\n' }) {
		c := strings.ToLower(strings.TrimSuffix(col, "::text"))
		if c == "select" || c == "" {
			continue
		}
		if !present[c] {
			t.Errorf("catalogSQL references %q, which is not a gp_segment_configuration column on the target kernel", c)
		}
	}
}

// TestIsEmptyOID pins the NULL-vs-present decision: NULL (nil) and blank
// strings are "absent"; a non-empty name or a numeric OID are "present".
func TestIsEmptyOID(t *testing.T) {
	cases := []struct {
		v    any
		want bool
	}{
		{nil, true},
		{"", true},
		{"   ", true},
		{"gp_segment_configuration", false},
		{int32(16384), false},
		{uint32(5036), false},
	}
	for i, c := range cases {
		if got := isEmptyOID(c.v); got != c.want {
			t.Errorf("case %d: isEmptyOID(%v) = %v, want %v", i, c.v, got, c.want)
		}
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
