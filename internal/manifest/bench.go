package manifest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
)

// BenchResult is the per-table measurement of the input-stream benchmark
// (F-14, `gpstream bench`): rows, wire bytes (compressed + framed), logical
// bytes (decompressed COPY payload), and the wall time of that unit's stream.
type BenchResult struct {
	Schema   string
	Name     string
	Rows     int64
	Wire     uint64
	Logical  uint64
	Elapsed  time.Duration
	CompOff  bool // F-12 small-table auto-disable applied to this unit
	OK       bool
	Err      error
	TimedOut bool // unit was cut off by the --timeout deadline
}

// BenchStats aggregates the benchmark run.
type BenchStats struct {
	Results      []BenchResult
	Workers      int
	TotalRows    int64
	TotalWire    uint64
	TotalLogical uint64
	Elapsed      time.Duration
	TimedOut     bool // the --timeout deadline fired mid-benchmark
}

// LogicalRate returns the aggregate input-stream rate (decompressed bytes /
// wall) — the headline "how fast does the source feed the pipeline" number.
func (s *BenchStats) LogicalRate() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.TotalLogical) / s.Elapsed.Seconds()
}

// WireRate returns the aggregate wire (compressed + framed) rate.
func (s *BenchStats) WireRate() float64 {
	if s.Elapsed <= 0 {
		return 0
	}
	return float64(s.TotalWire) / s.Elapsed.Seconds()
}

// BenchAll runs the input-stream benchmark over the selected units (F-14, P7).
// Each unit is read through the two-agent source path: the source agent runs
// the real COPY ... TO STDOUT -> frame v2 -> compression -> socket pipeline and
// reports the row count + on-wire + logical volume; the runner receives the
// stream on a drain sink (it reads the UnitSpec header, decompresses and
// discards the frames) so the measured wall time is the source's production
// rate and no destination database is involved. Units run on a WorkersPerPair
// pool, exactly like a run; the --timeout deadline (ctx) cuts the benchmark
// short and the partial result is returned with TimedOut set.
//
// When runner is nil a process runner is started for the benchmark and
// released on return (cmdBench); the unit tests inject a fake runner.
func (m *Manager) BenchAll(ctx context.Context, units []plan.Table, runner TwoAgentRunner) (*BenchStats, error) {
	if len(units) == 0 {
		return nil, fmt.Errorf("bench: no units to measure")
	}
	released := false
	if runner == nil {
		runner = newProcessTwoAgentRunner(m.cfg)
		if err := runner.Start(ctx); err != nil {
			return nil, fmt.Errorf("bench: start source agent: %w", err)
		}
		released = true
	}
	defer func() {
		if released {
			_ = runner.Release(context.Background())
		}
	}()
	switch m.cfg.CopyMode {
	case config.CopyOnSegment:
		return nil, fmt.Errorf("bench: --copy-mode on_segment measures the per-segment source read (COPY ON SEGMENT), which requires a real Greenplum source; run it on the lab (gp6lab build tag)")
	case config.CopyDirect:
		return nil, fmt.Errorf("bench: --copy-mode direct measures the per-segment routed source read, which requires a real Greenplum source; run it on the lab (gp6lab build tag)")
	}
	workers := m.cfg.WorkersPerPair
	if workers < 1 {
		workers = 1
	}

	start := time.Now()
	results := make([]BenchResult, len(units))

	var (
		wg       sync.WaitGroup
		work     = make(chan int, len(units))
		timedOut atomic.Bool
	)
	go func() {
		<-ctx.Done()
		timedOut.Store(true)
	}()
	for i := range units {
		work <- i
	}
	close(work)

	// Worker pool (same shape as Run): each worker pulls unit indices and
	// streams them through the two-agent source read. A unit error (or the
	// deadline) is recorded per unit; the benchmark keeps going (like the
	// default on-unit-failure=continue) so the operator sees the full
	// measurement picture.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				t := units[i]
				res := BenchResult{Schema: t.Schema, Name: t.Name}
				u := &Unit{
					UnitID: int64(i + 1),
					DB:     t.DB,
					Schema: t.Schema,
					Table:  t.Name,
					Kind:   "table",
				}
				t0 := time.Now()
				uc, _ := m.unitCompressors(t)
				res.CompOff = m.compressionOffFor(t)
				rows, wire, logical, err := runner.Bench(ctx, uint32(u.UnitID), jobIDFromHash(u.JobID), t.Schema, t.Name, m.twoAgentCompressString(uc))
				res.Elapsed = time.Since(t0)
				res.Rows = rows
				res.Wire = wire
				res.Logical = logical
				if err != nil {
					res.Err = err
					res.TimedOut = ctx.Err() != nil
				} else {
					res.OK = true
				}
				results[i] = res
			}
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		timedOut.Store(true)
	}

	s := &BenchStats{Results: results, Workers: workers, Elapsed: time.Since(start)}
	for _, r := range results {
		s.TotalRows += r.Rows
		s.TotalWire += r.Wire
		s.TotalLogical += r.Logical
		if r.TimedOut {
			s.TimedOut = true
		}
	}
	return s, nil
}
