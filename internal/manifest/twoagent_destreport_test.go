package manifest

import "testing"

// TestParseDestReport is a regression guard for the binary (process-runner)
// two-agent COORDINATOR path: the coordinator dest-agent report is four
// fields (unit, rows, logical, verdict) with NO wire= — the on-wire volume is
// the source agent's, reported separately. The parser must accept n==4.
// (A prior version required n==5, which made the binary coordinator path
// fail every unit with "no dest report line found"; the in-proc runner and
// the segment/direct paths each parse differently, so the bug was latent
// until the crash-recovery matrix exercised the binary coordinator path.)
func TestParseDestReport(t *testing.T) {
	cases := []struct {
		name    string
		stderr  string
		wantR   int64
		wantL   uint64
		wantErr bool
	}{
		{
			name:   "coordinator loaded",
			stderr: "gpstream-dest-agent: unit=1 rows=500 logical=28413 loaded ok\n",
			wantR:  500, wantL: 28413,
		},
		{
			name:   "coordinator swapped",
			stderr: "gpstream-dest-agent: unit=2 rows=20000 logical=1064915 swapped ok\n",
			wantR:  20000, wantL: 1064915,
		},
		{
			// Other stderr lines before the report must not break the parse.
			name:   "report after noise",
			stderr: "some other line\ngpstream-dest-agent: unit=3 rows=10 logical=100 loaded ok\n",
			wantR:  10, wantL: 100,
		},
		{
			name:    "no report line",
			stderr:  "nothing useful here\n",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, l, err := parseDestReport(tc.stderr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got rows=%d logical=%d", r, l)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r != tc.wantR || l != tc.wantL {
				t.Fatalf("got rows=%d logical=%d, want rows=%d logical=%d", r, l, tc.wantR, tc.wantL)
			}
		})
	}
}
