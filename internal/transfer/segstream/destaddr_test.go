package segstream

import (
	"testing"
)

// TestParseDestAddrs covers the segreceiver's --dest-addr address book:
// the single-address broadcast form (P9, destination AGENT) and the
// D-address mesh form (P4, one per destination segment's mini-agent),
// plus the hard-error shapes.
func TestParseDestAddrs(t *testing.T) {
	tests := []struct {
		name     string
		destAddr string
		D        int
		want     []string
		wantErr  bool
	}{
		{
			name:     "broadcast single address",
			destAddr: "10.0.0.1:4000",
			D:        3,
			want:     []string{"10.0.0.1:4000", "10.0.0.1:4000", "10.0.0.1:4000"},
		},
		{
			name:     "broadcast D=1",
			destAddr: "127.0.0.1:5000",
			D:        1,
			want:     []string{"127.0.0.1:5000"},
		},
		{
			name:     "mesh D addresses (one per segment)",
			destAddr: "10.0.0.2:4100,10.0.0.3:4101,10.0.0.4:4102",
			D:        3,
			want:     []string{"10.0.0.2:4100", "10.0.0.3:4101", "10.0.0.4:4102"},
		},
		{
			name:     "mesh D addresses with spaces",
			destAddr: " 10.0.0.2:4100 , 10.0.0.3:4101 , 10.0.0.4:4102 ",
			D:        3,
			want:     []string{"10.0.0.2:4100", "10.0.0.3:4101", "10.0.0.4:4102"},
		},
		{
			name:     "list length neither 1 nor D",
			destAddr: "10.0.0.2:4100,10.0.0.3:4101",
			D:        3,
			wantErr:  true,
		},
		{
			name:     "empty entry",
			destAddr: "10.0.0.2:4100,,10.0.0.4:4102",
			D:        3,
			wantErr:  true,
		},
		{
			name:     "malformed address",
			destAddr: "no-port-here",
			D:        1,
			wantErr:  true,
		},
		{
			name:     "bad D",
			destAddr: "10.0.0.1:4000",
			D:        0,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDestAddrs(tt.destAddr, tt.D)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseDestAddrs(%q, %d) = %v, want error", tt.destAddr, tt.D, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDestAddrs(%q, %d): %v", tt.destAddr, tt.D, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ParseDestAddrs(%q, %d) len = %d, want %d", tt.destAddr, tt.D, len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("grid[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
