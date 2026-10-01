package manifest

import (
	"context"
	"io"
	"testing"
	"time"

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

func TestBenchAllStreamsAndDiscards(t *testing.T) {
	// Source fake streams a canned COPY payload per table; there is NO
	// destination at all (dstQ nil) — the benchmark must not touch it.
	src := pg.NewFakeConn().OnReadCopy("TO STDOUT", []byte("binary-copy-payload"))
	m := newMgr(t, src, nil, nil)
	m.Bench()

	units := mkBenchUnits(3)
	st, err := m.BenchAll(context.Background(), units)
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
		// The fake ReadCopy returns 1 row for a non-empty payload.
		if r.Rows != 1 {
			t.Errorf("unit %d: rows = %d, want 1", i, r.Rows)
		}
		// The framed pipe must have counted the (compressed) wire bytes and
		// the (decompressed) logical bytes the drain served.
		if r.Logical == 0 {
			t.Errorf("unit %d: logical = 0, want >0 (payload was drained)", i)
		}
		if r.Wire == 0 {
			t.Errorf("unit %d: wire = 0, want >0 (frame was pushed)", i)
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

// slowCopy blocks the source COPY until the context is done, modeling a
// stream that is still producing when the --timeout deadline fires.
type slowCopy struct {
	*pg.FakeConn
}

func (s *slowCopy) ReadCopy(ctx context.Context, w io.Writer, sql string) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestBenchAllTimeoutCutsShort(t *testing.T) {
	src := &slowCopy{FakeConn: pg.NewFakeConn()}
	m := newMgr(t, src, nil, nil)
	m.Bench()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	st, _ := m.BenchAll(ctx, mkBenchUnits(3))
	if st == nil {
		t.Fatal("benchall returned nil under timeout")
	}
	if !st.TimedOut {
		t.Error("TimedOut = false, want the deadline to cut the run short")
	}
	// At least one unit must be marked as cut off.
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
	m.Bench()
	if _, err := m.BenchAll(context.Background(), nil); err == nil {
		t.Fatal("benchall(empty) = nil error, want an error")
	}
}
