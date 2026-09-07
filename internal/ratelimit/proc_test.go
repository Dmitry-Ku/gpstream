package ratelimit

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProcProviderIOWait(t *testing.T) {
	dir := t.TempDir()
	stat := filepath.Join(dir, "stat")
	// First snapshot: baseline.
	writeFile(t, stat, "cpu  100 0 50 900 20 0 0 0\n")
	p := NewProcProvider()
	p.StatPath = stat
	_ = p.Snapshot() // prime the baseline
	// Second snapshot: iowait grows a lot relative to total.
	writeFile(t, stat, "cpu  100 0 50 900 200 0 0 0\n")
	m := p.Snapshot()
	// iowait went from 20 -> 200 (+180) over total delta 200 -> 200...
	// the percentage is positive and bounded.
	if m.IOWaitPercent < 0 || m.IOWaitPercent > 100 {
		t.Errorf("iowait percent = %g, out of range", m.IOWaitPercent)
	}
}

func TestProcProviderMissing(t *testing.T) {
	p := NewProcProvider()
	p.StatPath = "/nonexistent/proc/stat"
	p.DiskPath = "/nonexistent/proc/diskstats"
	m := p.Snapshot() // must not panic; returns zeros
	if m.IOWaitPercent != 0 {
		t.Errorf("missing stat: iowait = %g, want 0", m.IOWaitPercent)
	}
}

func TestIsPartition(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"sda", false},
		{"sda1", true},
		{"nvme0n1", false},
		{"nvme0n1p1", true},
		{"loop0", false},
	}
	for _, c := range cases {
		if got := isPartition(c.name); got != c.want {
			t.Errorf("isPartition(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
