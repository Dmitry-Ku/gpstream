package manifest

import "testing"

// TestRewritePeerAddr: the dest agent's BOUND peer-listen address must be
// rewritten to the destination coordinator host when the source agent dials
// it cross-cluster (a remote dest binds 0.0.0.0 -> hello reports "[::]:port",
// a loopback alias; a cross-cluster source would dial its OWN localhost).
func TestRewritePeerAddr(t *testing.T) {
	tests := []struct {
		name      string
		boundPeer string
		destSSH   string
		want      string
	}{
		{"remote any6 rewrite", "[::]:46738", "gp@10.0.0.16", "10.0.0.16:46738"},
		{"remote any4 rewrite", "0.0.0.0:46738", "gp@10.0.0.16", "10.0.0.16:46738"},
		{"remote loopback rewrite", "127.0.0.1:46738", "gp@vm6", "vm6:46738"},
		{"local dest untouched (loopback host)", "127.0.0.1:46738", "gp@localhost", "127.0.0.1:46738"},
		{"local dest untouched (no ssh)", "127.0.0.1:46738", "", "127.0.0.1:46738"},
		{"explicit non-loopback untouched", "10.0.0.16:46738", "gp@10.0.0.16", "10.0.0.16:46738"},
		{"empty untouched", "", "gp@10.0.0.16", ""},
		{"garbage untouched", "not-a-port", "gp@10.0.0.16", "not-a-port"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rewritePeerAddr(tt.boundPeer, tt.destSSH); got != tt.want {
				t.Fatalf("rewritePeerAddr(%q, %q) = %q, want %q", tt.boundPeer, tt.destSSH, got, tt.want)
			}
		})
	}
}
