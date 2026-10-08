package manifest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// mkBenchUnits returns a couple of units for the benchmark test.
func mkBenchUnits(n int) []plan.Table {
	var out []plan.Table
	for i := 0; i < n; i++ {
		out = append(out, plan.Table{
			DB:     "srcdb",
			Schema: "s",
			Name:   "t",
			Bytes:  4096,
		})
	}
	return out
}

// fakeBenchRunner is an in-memory TwoAgentRunner for the unit tests. Its Bench
// returns a canned (rows, wire, logical) triple standing in for the source
// agent's report; the other ops are stubs (the benchmark only calls Bench).
// When block is set, Bench blocks until the context is done (modeling a stream
// still producing when the --timeout deadline fires).
type fakeBenchRunner struct {
	rows    int64
	wire    uint64
	logical uint64
	block   bool
}

func (f *fakeBenchRunner) Start(context.Context) error { return nil }
func (f *fakeBenchRunner) Stream(context.Context, unitStreamRequest) (int64, uint64, uint64, string, string, error) {
	return 0, 0, 0, "", "", fmt.Errorf("fakeBenchRunner: Stream unused in bench")
}
func (f *fakeBenchRunner) StreamSegment(context.Context, unitSegmentRequest) (int64, uint64, uint64, error) {
	return 0, 0, 0, fmt.Errorf("fakeBenchRunner: StreamSegment unused in bench")
}
func (f *fakeBenchRunner) StreamRouted(context.Context, unitRoutedRequest) (int64, uint64, uint64, error) {
	return 0, 0, 0, fmt.Errorf("fakeBenchRunner: StreamRouted unused in bench")
}
func (f *fakeBenchRunner) StreamMini(context.Context, unitMiniRequest) (int64, uint64, uint64, string, string, error) {
	return 0, 0, 0, "", "", fmt.Errorf("fakeBenchRunner: StreamMini unused in bench")
}
func (f *fakeBenchRunner) Checksum(context.Context, string, string) (string, error) {
	return "", fmt.Errorf("fakeBenchRunner: Checksum unused in bench")
}
func (f *fakeBenchRunner) Release(context.Context) error { return nil }
func (f *fakeBenchRunner) Bench(ctx context.Context, unitID, jobID uint32, schema, table, compress string) (int64, uint64, uint64, error) {
	if f.block {
		<-ctx.Done()
		return 0, 0, 0, ctx.Err()
	}
	return f.rows, f.wire, f.logical, nil
}

func TestBenchAllStreamsAndDiscards(t *testing.T) {
	// The runner's canned report stands in for the source agent's (rows +
	// on-wire + logical volume); there is NO destination (dstQ nil).
	m := newMgr(t, pg.NewFakeConn(), nil, nil)
	runner := &fakeBenchRunner{rows: 1, wire: 100, logical: 40}

	units := mkBenchUnits(3)
	st, err := m.BenchAll(context.Background(), units, runner)
	if err != nil {
		t.Fatalf("benchall: %v", err)
	}
	if st == nil {
		t.Fatal("benchall returned nil stats")
	}
	if len(st.Results) != 3 {
		t.Fatalf("results = %d, want 3", len(st.Results))
	}
	for i, r := range st.Results {
		if !r.OK {
			t.Errorf("unit %d: not OK (err=%v)", i, r.Err)
		}
		if r.Rows != 1 {
			t.Errorf("unit %d: rows = %d, want 1", i, r.Rows)
		}
		if r.Logical == 0 {
			t.Errorf("unit %d: logical = 0, want >0 (source reported the payload)", i)
		}
		if r.Wire == 0 {
			t.Errorf("unit %d: wire = 0, want >0 (source reported the framed stream)", i)
		}
	}
	if st.TotalRows != 3 {
		t.Errorf("total rows = %d, want 3", st.TotalRows)
	}
	if st.TotalLogical == 0 || st.TotalWire == 0 {
		t.Errorf("totals: logical=%d wire=%d, want both >0", st.TotalLogical, st.TotalWire)
	}
	if st.TimedOut {
		t.Error("timed out, want a clean run")
	}
}

func TestBenchAllTimeoutCutsShort(t *testing.T) {
	// block=true: each unit's source read blocks until the deadline fires.
	m := newMgr(t, pg.NewFakeConn(), nil, nil)
	runner := &fakeBenchRunner{block: true}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	st, _ := m.BenchAll(ctx, mkBenchUnits(3), runner)
	if st == nil {
		t.Fatal("benchall returned nil under timeout")
	}
	if !st.TimedOut {
		t.Error("TimedOut = false, want the deadline to cut the run short")
	}
	cut := 0
	for _, r := range st.Results {
		if r.TimedOut {
			cut++
		}
	}
	if cut == 0 {
		t.Error("no unit marked TimedOut")
	}
}

func TestBenchAllEmptyUnits(t *testing.T) {
	m := newMgr(t, pg.NewFakeConn(), nil, nil)
	if _, err := m.BenchAll(context.Background(), nil, &fakeBenchRunner{}); err == nil {
		t.Fatal("benchall(empty) = nil error, want an error")
	}
}

// TestBenchAllRejectsSegmentModes: P7 — on_segment measures the
// per-segment source read, which needs a real Greenplum source (gp_segment_id
// + ON SEGMENT / routing); on a non-GP source they are refused with a clear
// message pointing at the lab.
func TestBenchAllRejectsSegmentModes(t *testing.T) {
	m := newMgr(t, pg.NewFakeConn(), nil, func(c *config.Config) { c.CopyMode = config.CopyMesh })
	if _, err := m.BenchAll(context.Background(), mkBenchUnits(1), &fakeBenchRunner{}); err == nil {
		t.Fatal("benchall(on_segment): want the real-GP error")
	}
	m2 := newMgr(t, pg.NewFakeConn(), nil, func(c *config.Config) { c.CopyMode = config.CopyMesh })
	if _, err := m2.BenchAll(context.Background(), mkBenchUnits(1), &fakeBenchRunner{}); err == nil {
		t.Fatal("benchall(on_segment): want the real-GP error")
	}
}
