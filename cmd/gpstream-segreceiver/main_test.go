package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
)

// statsPipe is a transfer.Pipe that also reports a fixed sender-side Stats,
// so writeSenderStats's aggregation can be exercised without a live pipe.
type statsPipe struct {
	st transfer.Stats
}

func (p *statsPipe) Read([]byte) (int, error)  { return 0, io.EOF }
func (p *statsPipe) Write([]byte) (int, error) { return 0, nil }
func (p *statsPipe) Close() error              { return nil }
func (p *statsPipe) Stats() transfer.Stats     { return p.st }

// plainPipe is a transfer.Pipe WITHOUT a Stats() accessor (the plain-TCP
// case when frame-ack is off): writeSenderStats must skip it, not panic.
type plainPipe struct{}

func (p plainPipe) Read([]byte) (int, error)  { return 0, io.EOF }
func (p plainPipe) Write([]byte) (int, error) { return 0, nil }
func (p plainPipe) Close() error              { return nil }

func TestWriteSenderStatsAggregates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.json")
	pipes := []transfer.Pipe{
		&statsPipe{st: transfer.Stats{Retransmitted: 3, Reconnects: 1, AckLag: 12 * time.Millisecond}},
		&statsPipe{st: transfer.Stats{Retransmitted: 5, Reconnects: 2, AckLag: 40 * time.Millisecond}},
		plainPipe{}, // no Stats(): must be skipped
	}
	writeSenderStats(path, 7, 9, 2, 1234, pipes...)

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("stats file not written: %v", err)
	}
	var rep statsFileReport
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rep.Job != 7 || rep.Unit != 9 || rep.Content != 2 || rep.Rows != 1234 {
		t.Errorf("identity fields = %+v, want job=7 unit=9 content=2 rows=1234", rep)
	}
	if rep.Retransmitted != 8 {
		t.Errorf("retransmitted = %d, want 8 (3+5)", rep.Retransmitted)
	}
	if rep.Reconnects != 3 {
		t.Errorf("reconnects = %d, want 3 (1+2)", rep.Reconnects)
	}
	if rep.AckLagMaxMs != 40 {
		t.Errorf("ack_lag_max_ms = %d, want 40 (max of 12,40)", rep.AckLagMaxMs)
	}
}

func TestWriteSenderStatsEmptyPathNoop(t *testing.T) {
	// path == "" must be a silent no-op (frame-ack off / flag unset).
	writeSenderStats("", 1, 2, 3, 4, &statsPipe{st: transfer.Stats{Retransmitted: 99}})
	if _, err := os.Stat(""); err == nil {
		t.Fatalf("expected no file for empty path")
	}
}

func TestWriteSenderStatsNoPipes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	writeSenderStats(path, 1, 1, 0, 50)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("stats file not written: %v", err)
	}
	var rep statsFileReport
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rep.Retransmitted != 0 || rep.Reconnects != 0 || rep.AckLagMaxMs != 0 {
		t.Errorf("zero-pipe report = %+v, want all-zero counters", rep)
	}
	if rep.Rows != 50 {
		t.Errorf("rows = %d, want 50", rep.Rows)
	}
}

func TestWriteSenderStatsBadDirNeverPanic(t *testing.T) {
	// A non-writable path warns on stderr but must not panic or fail the
	// stream (observability side-channel only).
	writeSenderStats(filepath.Join(t.TempDir(), "no", "such", "dir", "s.json"), 1, 1, 0, 1)
}

// --stats-dir (relay increment 2): the per-segment filename is built from
// this segreceiver's own --content, so parallel segments never collide.
func TestStatsDirPerSegmentFileName(t *testing.T) {
	dir := t.TempDir()
	p0 := filepath.Join(dir, "sender-7-8-seg0.json")
	p1 := filepath.Join(dir, "sender-7-8-seg1.json")
	writeSenderStats(p0, 7, 8, 0, 11)
	writeSenderStats(p1, 7, 8, 1, 22)
	var r0, r1 statsFileReport
	for _, p := range []struct {
		path string
		rep  *statsFileReport
	}{
		{p0, &r0}, {p1, &r1},
	} {
		b, err := os.ReadFile(p.path)
		if err != nil {
			t.Fatalf("read %s: %v", p.path, err)
		}
		if err := json.Unmarshal(b, p.rep); err != nil {
			t.Fatalf("parse %s: %v", p.path, err)
		}
	}
	if r0.Content != 0 || r0.Rows != 11 || r0.Job != 7 || r0.Unit != 8 {
		t.Errorf("seg0 report = %+v", r0)
	}
	if r1.Content != 1 || r1.Rows != 22 {
		t.Errorf("seg1 report = %+v", r1)
	}
}

// --stats-dir creates its own directory (the segreceiver runs as the
// database user; no cross-user setup needed).
func TestStatsDirMkdirAll(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "dir")
	writeSenderStats(filepath.Join(dir, "sender-1-1-seg0.json"), 1, 1, 0, 3)
	if b, err := os.ReadFile(filepath.Join(dir, "sender-1-1-seg0.json")); err != nil || len(b) == 0 {
		t.Errorf("file after MkdirAll: %v (len %d)", err, len(b))
	}
}

// The kernel launches the segreceiver with umask 077: the created stats
// directory and file must still land 0755/0644 — the relay reader (the
// source agent) runs as a DIFFERENT user.
func TestStatsDirUmask077(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "sender-1-1-seg0.json")
	writeSenderStats(p, 1, 1, 0, 5)
	if fi, err := os.Stat(filepath.Join(dir, "sub")); err != nil {
		t.Fatalf("dir: %v", err)
	} else if fi.Mode().Perm() != 0o755 {
		t.Errorf("dir mode = %o, want 755 (umask 077 must not win)", fi.Mode().Perm())
	}
	if fi, err := os.Stat(p); err != nil {
		t.Fatalf("file: %v", err)
	} else if fi.Mode().Perm() != 0o644 {
		t.Errorf("file mode = %o, want 644", fi.Mode().Perm())
	}
}
