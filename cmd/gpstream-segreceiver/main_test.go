package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
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
