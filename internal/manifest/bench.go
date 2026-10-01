package manifest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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

// BenchAll runs the input-stream benchmark over the selected units (F-14).
// Each unit streams through the real data path (COPY ... TO STDOUT -> frame
// v2 -> compression -> adaptive rate limiter) but the payload is discarded
// instead of written to a destination, so the measured wall time is the
// source's production rate. Units run on a WorkersPerPair pool, exactly like
// a run; the --timeout deadline (ctx) cuts the benchmark short and the
// partial result is returned with TimedOut set.
func (m *Manager) BenchAll(ctx context.Context, units []plan.Table) (*BenchStats, error) {
	if len(units) == 0 {
		return nil, fmt.Errorf("bench: no units to measure")
	}
	workers := m.cfg.WorkersPerPair
	if workers < 1 {
		workers = 1
	}

	// The benchmark honors the operator's consistency mode exactly like a run:
	// the default --consistent-db pins one REPEATABLE READ snapshot so every
	// worker's COPY reads the same point of the source.
	m.establishSnapshot(ctx)
	defer m.unpinSource()

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
	// streams them. A unit error (or the deadline) is recorded per unit; the
	// benchmark keeps going (like the default on-unit-failure=continue) so
	// the operator sees the full measurement picture.
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
				us, end := m.unitSource(ctx)
				t0 := time.Now()
				uc, ud := m.unitCompressors(t)
				res.CompOff = m.compressionOffFor(t)
				fp := m.framedPipe(uint32(u.UnitID), uc, ud)
				rows, wire, logical, err := m.copyStream(ctx, t, u, "", fp, us)
				end()
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
