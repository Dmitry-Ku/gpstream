package doctor

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestSeverityString covers every label, including the unknown fallback.
func TestSeverityString(t *testing.T) {
	for _, tc := range []struct {
		s    Severity
		want string
	}{{Pass, "PASS"}, {Warn, "WARN"}, {Fail, "FAIL"}, {Severity(99), "?"}} {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("Severity(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}

// TestReportSorted verifies Fail-first grouping with name tie-break and that
// the original slice is not mutated.
func TestReportSorted(t *testing.T) {
	r := &Report{Healthy: false, Checks: []Check{
		{Name: "zeta", Side: "dest", Sev: Pass},
		{Name: "alpha", Side: "source", Sev: Fail},
		{Name: "mid", Side: "dest", Sev: Warn},
		{Name: "beta", Side: "source", Sev: Fail},
	}}
	got := r.Sorted()
	want := []string{"alpha", "beta", "mid", "zeta"}
	for i, name := range want {
		if got[i].Name != name {
			t.Fatalf("sorted order = %v, want %v", names(got), want)
		}
	}
	// Original order must be preserved on the receiver.
	if r.Checks[0].Name != "zeta" {
		t.Errorf("Sorted() mutated the receiver: first = %q, want zeta", r.Checks[0].Name)
	}
}

func names(cs []Check) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	return out
}

// TestFirstAsInt covers every coercion branch: int64, int, bool, libpq text
// booleans, and unrecognised cells (which must not panic and map to 0).
func TestFirstAsInt(t *testing.T) {
	for _, tc := range []struct {
		rows [][]any
		want int
	}{{
		nil, 0,
	}, {
		[][]any{{}}, 0,
	}, {
		[][]any{{int64(42)}}, 42,
	}, {
		[][]any{{int(7)}}, 7,
	}, {
		[][]any{{true}}, 1,
	}, {
		[][]any{{false}}, 0,
	}, {
		[][]any{{"t"}}, 1,
	}, {
		[][]any{{"true"}}, 1,
	}, {
		[][]any{{"1"}}, 1,
	}, {
		[][]any{{"f"}}, 0,
	}, {
		[][]any{{"0"}}, 0,
	}, {
		[][]any{{float64(3.0)}}, 0, // unsupported type -> 0, no panic
	}} {
		if got := firstAsInt(tc.rows); got != tc.want {
			t.Errorf("firstAsInt(%v) = %d, want %d", tc.rows, got, tc.want)
		}
	}
}

// TestCheckDiskEmptyRows covers the "probe returned no data" branch: the
// sentinel query succeeds but yields zero rows.
func TestCheckDiskEmptyRows(t *testing.T) {
	r := &Report{Healthy: true}
	q := pg.NewFakeConn()
	q.On("SELECT 1", [][]any{})
	r.checkDisk("dest", q)
	if got := find(r, "dest", "disk"); got != Fail {
		t.Errorf("disk with empty rows: got %v, want FAIL", got)
	}
}

// TestCheckPermissionsProbeError covers the privilege-probe failure branch
// (query account cannot answer the has_database_privilege call): WARN, not a
// crash and not a silent PASS.
func TestCheckPermissionsProbeError(t *testing.T) {
	r := &Report{Healthy: true}
	q := pg.NewFakeConn()
	q.Off("has_database_privilege") // unregistered -> query error
	r.checkPermissions("source", q)
	if got := find(r, "source", "permissions"); got != Warn {
		t.Errorf("permissions probe error: got %v, want WARN", got)
	}
}

// TestCheckSegmentsZeroUp is the CR-7 fault "no up segments": the catalog is
// present (probe resolves to an OID) but the count of up segments is zero —
// must FAIL, never a silent PASS.
func TestCheckSegmentsZeroUp(t *testing.T) {
	r := &Report{Healthy: true}
	q := pg.NewFakeConn()
	q.On("to_regclass", [][]any{{int32(16384)}}) // OID -> Greenplum
	// Catalog present but all primaries down -> UpCount() = 0.
	q.On("FROM gp_segment_configuration", [][]any{
		{int64(0), "p", "p", "n", "d", "host0", 6000, "dwh"},
		{int64(1), "p", "p", "n", "d", "host1", 6000, "dwh"},
	})
	r.checkSegments("dest", q)
	if got := find(r, "dest", "segments"); got != Fail {
		t.Errorf("zero up segments: got %v, want FAIL", got)
	}
}

// TestCheckSegmentsProbeError covers the "cannot determine Greenplum status"
// branch (catalog query errors): FAIL with a recommendation.
func TestCheckSegmentsProbeError(t *testing.T) {
	r := &Report{Healthy: true}
	q := pg.NewFakeConn()
	q.Off("to_regclass") // unregistered -> query error
	r.checkSegments("source", q)
	if got := find(r, "source", "segments"); got != Fail {
		t.Errorf("segments probe error: got %v, want FAIL", got)
	}
}

var _ = context.Background // keep import stable across branch edits
