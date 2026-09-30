package manifest

// Unit tests for the FR-02 snapshot machinery and the direct-path degrade
// helpers. The snapshot functions are the consistency core: establishSnapshot
// pins the source connection (run-wide db/cluster modes), perTableSnapshotConn
// draws a per-table RR connection, healSnapshot re-establishes an aborted
// snapshot so one failed unit does not cascade 25P02 to the rest (F-06).
// Fakes stand in for the pool connections; the live lab e2e covers the real
// RR semantics.

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/ratelimit"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// newMgr builds a Manager with the given fake source/dest and config overrides.
func newMgr(t *testing.T, srcQ, dstQ pg.StreamingQuerier, mutate func(*config.Config)) *Manager {
	t.Helper()
	c := configDefault()
	if mutate != nil {
		mutate(c)
	}
	return NewManager(c, srcQ, dstQ, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
}

func queriedContains(qs []string, substr string) bool {
	for _, s := range qs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

func TestEstablishSnapshotModes(t *testing.T) {
	ctx := context.Background()

	// No consistency mode: no snapshot at all. (ConsistentDB defaults true, so
	// clear it explicitly.)
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), func(c *config.Config) { c.ConsistentDB = false })
	m.establishSnapshot(ctx)
	if m.snapshotMode != "" || m.snapshotTx {
		t.Errorf("no mode: snapshotMode=%q tx=%v, want empty/false", m.snapshotMode, m.snapshotTx)
	}

	// consistent-db: the fake Exec succeeds, so the RR transaction opens and
	// the mode is recorded.
	src := pg.NewFakeConn()
	m = newMgr(t, src, pg.NewFakeConn(), func(c *config.Config) { c.ConsistentDB = true })
	m.establishSnapshot(ctx)
	if m.snapshotMode != "db" || !m.snapshotTx {
		t.Errorf("consistent-db: mode=%q tx=%v, want db/true", m.snapshotMode, m.snapshotTx)
	}
	if !queriedContains(src.Queried(), "BEGIN ISOLATION LEVEL REPEATABLE READ") {
		t.Error("consistent-db: BEGIN RR not issued")
	}
	m.releaseSnapshot(ctx)
	if m.snapshotTx {
		t.Error("releaseSnapshot: snapshotTx still true")
	}
	if !queriedContains(src.Queried(), "COMMIT") {
		t.Error("releaseSnapshot: COMMIT not issued")
	}

	// consistent-cluster: the source freeze runs first, then the BEGIN RR.
	// (DB and Cluster are mutually exclusive, so clear DB.)
	src = pg.NewFakeConn()
	m = newMgr(t, src, pg.NewFakeConn(), func(c *config.Config) {
		c.ConsistentDB = false
		c.ConsistentCluster = true
	})
	m.establishSnapshot(ctx)
	if m.snapshotMode != "cluster" || !m.snapshotTx {
		t.Errorf("consistent-cluster: mode=%q tx=%v, want cluster/true", m.snapshotMode, m.snapshotTx)
	}
	if !queriedContains(src.Queried(), "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY") {
		t.Error("consistent-cluster: freeze statement not issued")
	}
	m.releaseSnapshot(ctx)
}

func TestBeginReleaseSnapshot(t *testing.T) {
	ctx := context.Background()
	src := pg.NewFakeConn()
	m := newMgr(t, src, pg.NewFakeConn(), nil)

	// db mode: just the BEGIN RR.
	if err := m.beginSnapshotTx(ctx, "db"); err != nil {
		t.Fatalf("beginSnapshotTx db: %v", err)
	}
	if !queriedContains(src.Queried(), "BEGIN ISOLATION LEVEL REPEATABLE READ") {
		t.Error("db: BEGIN RR not issued")
	}

	// cluster mode: freeze first.
	if err := m.beginSnapshotTx(ctx, "cluster"); err != nil {
		t.Fatalf("beginSnapshotTx cluster: %v", err)
	}
	if !queriedContains(src.Queried(), "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY") {
		t.Error("cluster: freeze not issued")
	}

	// releaseSnapshot without a tx: a no-op (no COMMIT issued).
	before := len(src.Queried())
	m.releaseSnapshot(ctx)
	if len(src.Queried()) != before {
		t.Error("releaseSnapshot (no tx): must not issue statements")
	}

	// With a tx: COMMIT is issued and the state clears.
	m.snapshotTx = true
	m.releaseSnapshot(ctx)
	if m.snapshotTx {
		t.Error("releaseSnapshot: snapshotTx still true")
	}
	if !queriedContains(src.Queried(), "COMMIT") {
		t.Error("releaseSnapshot: COMMIT not issued")
	}
}

func TestHealSnapshot(t *testing.T) {
	ctx := context.Background()

	// Not in snapshot mode: a no-op.
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	m.healSnapshot(ctx) // must not panic

	// Cancelled ctx: the run is tearing down; no heal, state preserved.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	m.snapshotTx = true
	m.healSnapshot(cctx)
	if !m.snapshotTx {
		t.Error("cancelled ctx: heal must not clear the snapshot state")
	}

	// Healthy transaction: the probe succeeds, nothing is healed.
	src := pg.NewFakeConn()
	m = newMgr(t, src, pg.NewFakeConn(), nil)
	m.snapshotTx = true
	m.snapshotMode = "db"
	m.healSnapshot(ctx)
	if !m.snapshotTx || m.snapshotMode != "db" {
		t.Errorf("healthy: state changed (tx=%v mode=%q)", m.snapshotTx, m.snapshotMode)
	}
	if !queriedContains(src.Queried(), "SELECT 1") {
		t.Error("healthy: probe (SELECT 1) not issued")
	}

	// Aborted transaction on a dead connection: the probe fails, the
	// ROLLBACK fails too (the exec error persists on the fake), so the
	// snapshot degrades to best-effort (mode cleared, no cascade).
	dead := pg.NewFakeConn().SetExecError(errors.New("25P02: current transaction is aborted"))
	m = newMgr(t, dead, pg.NewFakeConn(), nil)
	m.snapshotTx = true
	m.snapshotMode = "db"
	m.healSnapshot(ctx)
	if m.snapshotMode != "" {
		t.Errorf("dead conn: snapshotMode=%q, want degraded empty", m.snapshotMode)
	}
}

func TestPerTableSnapshotConn(t *testing.T) {
	ctx := context.Background()

	// Outside per-table mode: (nil, no-op).
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	q, end := m.perTableSnapshotConn(ctx)
	if q != nil {
		t.Errorf("no mode: q=%v, want nil", q)
	}
	end() // no-op must not panic

	// Per-table mode with a fake (non-pool) source: degrade to base.
	m = newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), func(c *config.Config) { c.ConsistentTables = true })
	q, end = m.perTableSnapshotConn(ctx)
	if q != nil {
		t.Errorf("fake source: q=%v, want nil (no pool to draw from)", q)
	}
	end()
}

func TestSrcReadUnpin(t *testing.T) {
	base := pg.NewFakeConn()
	m := newMgr(t, base, pg.NewFakeConn(), nil)
	if m.srcRead() != base {
		t.Error("srcRead: want the base querier without a pin")
	}
	// Pin a different conn and verify it wins, then unpin restores the base.
	pinned := pg.NewFakeConn()
	m.pinnedSrc = pinned
	m.pinRelease = func() {}
	if m.srcRead() != pinned {
		t.Error("srcRead: want the pinned conn")
	}
	m.unpinSource()
	if m.srcRead() != base {
		t.Error("unpinSource: srcRead must return the base querier")
	}
	m.unpinSource() // idempotent
	if m.pinnedSrc != nil || m.pinRelease != nil {
		t.Error("unpinSource: state not cleared")
	}
}

func TestUnitSource(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	us, end := m.unitSource(ctx)
	if us.agent == nil || us.q == nil {
		t.Fatal("unitSource: agent/q nil")
	}
	end() // no-op

	// Per-table mode, fake source: falls back to the base agent.
	m = newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), func(c *config.Config) { c.ConsistentTables = true })
	us, end = m.unitSource(ctx)
	if us.agent == nil || us.q == nil {
		t.Fatal("unitSource (per-table fake): agent/q nil")
	}
	end()
}

// TestCopyStreamDirectDegrades covers copyStreamDirect's pre-flight degrade
// paths (the R5 direct route must fall back to the coordinator path, not
// fail, when it cannot be satisfied).
func TestCopyStreamDirectDegrades(t *testing.T) {
	ctx := context.Background()
	src := pg.NewFakeConn().On("gp_segment_configuration", [][]any{{int64(4)}})
	dst := pg.NewFakeConn().On("gp_segment_configuration", [][]any{
		{int32(0), "seg0", int32(20000)},
		{int32(1), "seg1", int32(20001)},
	})
	m := newMgr(t, src, dst, nil)

	// A partitioned parent degrades (utility COPY FROM is not supported on
	// partition-hierarchy roots).
	u := &Unit{UnitID: 1, Schema: "public", Table: "t", DstSchema: "public", DstName: "t"}
	u.Partition.Spec = "PARTITION BY RANGE (d)"
	_, _, _, err := m.copyStreamDirect(ctx, plan.Table{Name: "t"}, u, "stg", nil, unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("partitioned: want directDegradedError, got %v", err)
	}

	// A non-hash target distribution degrades (the fake has no dist catalog).
	u = &Unit{UnitID: 2, Schema: "public", Table: "t2", DstSchema: "public", DstName: "t2"}
	_, _, _, err = m.copyStreamDirect(ctx, plan.Table{Name: "t2"}, u, "stg", nil, unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("no hash key: want directDegradedError, got %v", err)
	}

	// A plain-PostgreSQL destination (no segment catalog) degrades too.
	m2 := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	_, _, _, err = m2.copyStreamDirect(ctx, plan.Table{Name: "t3"}, &Unit{UnitID: 3, Schema: "public", Table: "t3", DstSchema: "public", DstName: "t3"}, "stg", nil, unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("plain PG dest: want directDegradedError, got %v", err)
	}
}

// TestCopyStreamSegmentDirectDegrades covers copyStreamSegmentDirect's early
// guards: no segments, and a bad listen address.
func TestCopyStreamSegmentDirectDegrades(t *testing.T) {
	ctx := context.Background()

	// No up primaries (plain-PG source): the segment count query fails.
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	_, _, _, err := m.copyStreamSegmentDirect(ctx, plan.Table{Name: "t"}, &Unit{UnitID: 1, Schema: "public", Table: "t", DstSchema: "public", DstName: "t"}, "stg", unitSource{})
	if err == nil {
		t.Error("no segments: want error")
	}

	// A bad seg-listen address degrades before any network work.
	m = newMgr(t,
		pg.NewFakeConn().On("gp_segment_configuration", [][]any{{int64(4)}}),
		pg.NewFakeConn(),
		func(c *config.Config) { c.SegListen = "no-port-here" })
	_, _, _, err = m.copyStreamSegmentDirect(ctx, plan.Table{Name: "t"}, &Unit{UnitID: 1, Schema: "public", Table: "t", DstSchema: "public", DstName: "t"}, "stg", unitSource{})
	if err == nil {
		t.Error("bad listen addr: want error")
	}
}

// fakeCloser is an io.Closer that records whether Close was called.
type fakeCloser struct {
	err    error
	closed bool
}

func (c *fakeCloser) Close() error {
	c.closed = true
	return c.err
}

func TestClosePipesIsDirectDegradedQuote(t *testing.T) {
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)

	// closePipes: nils are skipped, the first error wins, all are closed.
	var c1, c2 fakeCloser
	c2.err = errors.New("boom")
	if err := m.closePipes([]io.Closer{nil, &c1, &c2}); err == nil || err.Error() != "boom" {
		t.Errorf("closePipes: err=%v, want boom", err)
	}
	if !c1.closed || !c2.closed {
		t.Error("closePipes: not all closers closed")
	}
	if err := m.closePipes([]io.Closer{nil}); err != nil {
		t.Errorf("closePipes (nils): %v", err)
	}
	if err := m.closePipes(nil); err != nil {
		t.Errorf("closePipes (nil slice): %v", err)
	}

	// isDirectDegraded / Error.
	var d = &directDegradedError{reason: "x"}
	if !isDirectDegraded(d) || isDirectDegraded(errors.New("other")) {
		t.Error("isDirectDegraded misfires")
	}
	if d.Error() != "direct mode: x" {
		t.Errorf("directDegradedError.Error = %q", d.Error())
	}

	// quoteIdentDSN: quotes and escapes embedded quotes.
	if got := quoteIdentDSN("a b"); got != `"a b"` {
		t.Errorf("quoteIdentDSN = %q", got)
	}
	if got := quoteIdentDSN(`a"b`); got != `"a""b"` {
		t.Errorf("quoteIdentDSN escape = %q", got)
	}
}

// TestPacedWriter covers the FR-14 pacer: nil pacer -> transparent
// passthrough; a real pacer throttles writes through the framed pipe.
func TestPacedWriter(t *testing.T) {
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	w, r := net.Pipe()
	fp := transfer.NewFramedPipe(w, r, 1, 1, compress.New(compress.ModeNone, 0), compress.NewDec(compress.ModeNone))
	defer fp.Close()

	// No limiter: pacedWriter returns the pipe itself (passthrough).
	if pw := m.pacedWriter(fp); pw != io.Writer(fp) {
		t.Error("nil pacer: want the pipe itself (passthrough)")
	}

	// With a limiter: the wrapper throttles to the rate and still delivers.
	m.lim = ratelimit.New(ratelimit.Options{Baseline: 1})
	m.pacer = ratelimit.NewPacer(m.lim)
	pw := m.pacedWriter(fp)
	if pw == nil {
		t.Fatal("pacedWriter with limiter: want non-nil wrapper")
	}
	data := make([]byte, 1<<20) // 1 MiB, well under the 1 Gbps burst
	werr := make(chan error, 1)
	go func() {
		_, err := pw.Write(data)
		werr <- err
	}()
	// Drain the full payload on the read side (net.Pipe is unbuffered).
	total := 0
	buf := make([]byte, 32<<10)
	for total < len(data) {
		n, err := fp.Read(buf)
		total += n
		if err != nil {
			t.Fatalf("fp.Read: %v", err)
		}
	}
	if err := <-werr; err != nil {
		t.Fatalf("pacedWriter.Write: %v", err)
	}
}

func TestStartLimiterSampler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No limiter: nil stop func.
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	if stop := m.startLimiterSampler(ctx); stop != nil {
		t.Error("no limiter: want nil stop")
	}

	// With a limiter: the sampler runs and the stop func is non-nil.
	m = newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	m.lim = ratelimit.New(ratelimit.Options{Baseline: 1})
	stop := m.startLimiterSampler(ctx)
	if stop == nil {
		t.Fatal("limiter present: want non-nil stop")
	}
	stop()
	cancel()
	time.Sleep(20 * time.Millisecond) // let the goroutine observe ctx.Done
}

// TestProcessUnitSafePanic: a panicking unit is recorded as a failure (FR-06/
// CR-7) and the worker survives; the panic is surfaced as an error.
func TestProcessUnitSafePanic(t *testing.T) {
	ctx := context.Background()
	// A source whose schema read panics.
	src := pg.NewFakeConn().SetPanicQuery("pg_attribute")
	m := newMgr(t, src, pg.NewFakeConn(), nil)
	u := &Unit{UnitID: 1, Schema: "public", Table: "pt", DstSchema: "public", DstName: "pt"}
	err := m.processUnitSafe(ctx, u)
	if err == nil {
		t.Fatal("panicking unit: want error")
	}
	if m.Failures == 0 {
		t.Error("panicking unit: Failures not incremented")
	}
	if u.State != StateFailed {
		t.Errorf("panicking unit: state=%s, want failed", u.State)
	}
}
