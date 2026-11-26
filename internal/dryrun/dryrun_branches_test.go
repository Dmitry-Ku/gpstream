package dryrun

import "testing"

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
