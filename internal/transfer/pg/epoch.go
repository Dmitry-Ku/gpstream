package pg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Epoch is the P1d snapshot-epoch: N concurrent REPEATABLE READ transactions,
// each pinned to its snapshot point within a small ε-window at run start
// (NewEpoch). The whole run reads through the epoch — each pipeline borrows a
// pooled connection (Acquire), runs its data COPY / checksum re-read on it (so
// it sees that connection's pinned snapshot), and returns it (Release; the
// transaction is NOT committed). Close commits (rolls back) all N transactions
// and returns the connections to the pool.
//
// Consistency: the N snapshot points differ by Δ = max(t_fix) − min(t_fix)
// (a few ms). ValidateDelta bounds Δ against --max-snapshot-delta; a run whose
// pins are more than that apart must not start (the source is "not in one
// point"). strict mode (--workers 1) never uses the epoch: it is the single
// pinned connection, one point exactly.
//
// This is the coordinator data path's N>1 generalization of Pool.Pin (which is
// single-connection and mutually exclusive): many connections may be pinned at
// once, each in its own REPEATABLE READ transaction, so N pipelines stream N
// tables concurrently under N near-identical snapshots.
type Epoch struct {
	pool      *Pool
	mu        sync.Mutex
	raw       []*pgxpool.Conn // the N reserved pool connections (index-aligned)
	conns     []*EpochConn    // the N borrowed-conn wrappers (index-aligned)
	inUse     []bool          // Acquire/Release free-list (index-aligned)
	divergent []bool          // conns excluded after a divergent heal (fail -> resume)
	fix       []time.Time     // t_fix[i]: wall-clock moment connection i's snapshot fixed
	// baseMin is the earliest ORIGINAL pin (min of fix at NewEpoch). It is the
	// reference a heal re-pin is validated against (P1d heal): a re-pin that
	// lags baseMin by more than --max-snapshot-delta must not restart the unit.
	baseMin time.Time
	closed  bool
}

// EpochConn is a borrowed connection from an Epoch: it carries one pipeline's
// data COPY (or checksum re-read) and knows its index in the epoch, so a unit
// that fails on it can trigger a Heal of exactly that connection. Release
// returns it to the epoch's free-list (the snapshot transaction stays open).
type EpochConn struct {
	StreamingQuerier
	epoch *Epoch
	index int
}

// Index is the connection's position in the epoch (for Epoch.Heal by index).
func (ec *EpochConn) Index() int { return ec.index }

// Release returns the connection to the epoch (the transaction is NOT
// committed — the run reuses the same snapshot). It is safe to call more than
// once; a double release is a no-op after the first.
func (ec *EpochConn) Release() { ec.epoch.Release(ec) }

// NewEpoch reserves n dedicated pool connections and pins a REPEATABLE READ
// snapshot on each, in one ε-window (the N pins happen back-to-back, a few ms
// apart). It returns the epoch; the caller must Close it. If the pool cannot
// supply n connections it returns an error and reserves nothing.
//
// The pin on each connection is `BEGIN ISOLATION LEVEL REPEATABLE READ` +
// `SELECT clock_timestamp()` — in Postgres a REPEATABLE READ snapshot is
// established at the FIRST query in the transaction, not at BEGIN, so the
// SELECT both pins the snapshot point and is the query that fixes it. t_fix[i]
// is the client wall-clock moment right after that query (the ε-window clock).
func NewEpoch(ctx context.Context, pool *Pool, n int) (*Epoch, error) {
	if n < 1 {
		return nil, fmt.Errorf("epoch: n must be >= 1, got %d", n)
	}
	raw, err := pool.reserveEpoch(ctx, n)
	if err != nil {
		return nil, err
	}
	e := &Epoch{
		pool:      pool,
		raw:       raw,
		conns:     make([]*EpochConn, n),
		inUse:     make([]bool, n),
		divergent: make([]bool, n),
		fix:       make([]time.Time, n),
	}
	for i, c := range raw {
		if err := pinEpochConn(ctx, c); err != nil {
			// Roll back the connections already pinned, release all, fail.
			for j := 0; j < i; j++ {
				_, _ = raw[j].Conn().Exec(context.Background(), "ROLLBACK")
			}
			pool.releaseEpoch(raw)
			return nil, fmt.Errorf("epoch: pin connection %d: %w", i, err)
		}
		e.conns[i] = &EpochConn{StreamingQuerier: &pinnedConn{c: c, pool: pool}, epoch: e, index: i}
		e.fix[i] = time.Now()
	}
	e.baseMin = minFix(e.fix)
	return e, nil
}

// pinEpochConn begins the REPEATABLE READ transaction on c and pins its
// snapshot point with a first query (the clock_timestamp() probe — see
// NewEpoch). It does not COMMIT; the connection holds the open transaction for
// the epoch's lifetime.
func pinEpochConn(ctx context.Context, c *pgxpool.Conn) error {
	pc := c.Conn()
	if _, err := pc.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ`); err != nil {
		return fmt.Errorf("begin snapshot tx: %w", err)
	}
	if _, err := pc.Exec(ctx, `SELECT clock_timestamp()`); err != nil {
		return fmt.Errorf("pin snapshot point: %w", err)
	}
	return nil
}

// Acquire hands out a free pooled connection (first free wins) as an
// EpochConn. The caller MUST Release it when the pipeline's read is done. It
// errors if all N connections are in use (the caller has more concurrent
// pipelines than the epoch has connections — a sizing fault).
func (e *Epoch) Acquire() (*EpochConn, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fmt.Errorf("epoch: closed")
	}
	for i := range e.conns {
		if !e.inUse[i] && !e.divergent[i] {
			e.inUse[i] = true
			return e.conns[i], nil
		}
	}
	return nil, fmt.Errorf("epoch: all %d connections in use or divergent", len(e.conns))
}

// Release returns a borrowed connection to the epoch's free-list (the
// snapshot transaction stays open — NOT committed). A nil or unknown
// connection is ignored (idempotent / defensive).
func (e *Epoch) Release(ec *EpochConn) {
	if ec == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.conns {
		if e.conns[i] == ec {
			e.inUse[i] = false
			return
		}
	}
}

// Delta returns max(t_fix) − min(t_fix) over the N pins (the ε-window width).
func (e *Epoch) Delta() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	return spreadOf(e.fix)
}

// ValidateDelta reports an error if the observed Δ exceeds limit (P1d
// --max-snapshot-delta, in seconds). limit <= 0 disables the check. A run
// whose N pins are more than `limit` apart must not start: the source is "not
// in one point", and starting would copy a silently-inconsistent snapshot.
func (e *Epoch) ValidateDelta(limit time.Duration) error {
	if limit <= 0 {
		return nil
	}
	e.mu.Lock()
	d := spreadOf(e.fix)
	e.mu.Unlock()
	return validateSpread(d, limit)
}

// MinFix returns the earliest original pin time (baseMin) — the reference a
// heal re-pin is validated against.
func (e *Epoch) MinFix() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.baseMin
}

// FixTimes returns a copy of the N pin times (diagnostics / runstats).
func (e *Epoch) FixTimes() []time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]time.Time, len(e.fix))
	copy(out, e.fix)
	return out
}

// ConnCount is the number of pinned connections in the epoch.
func (e *Epoch) ConnCount() int { return len(e.raw) }

// Heal re-pins connection i after its snapshot transaction was aborted (a
// failed unit's SQL fault, P1d heal): ROLLBACK + a fresh REPEATABLE READ
// snapshot on the same connection, recording a new t_fix[i]. The new pin is
// validated against baseMin: if it lags the epoch's earliest pin by more than
// `limit` (P1d --max-snapshot-delta), the caller must NOT restart the unit on
// this connection — it fails the unit → resume (an honest divergence, not a
// silent snapshot drift). limit <= 0 skips the validation. Returns the new
// t_fix[i]. It holds the epoch lock for the (fast) ROLLBACK+BEGIN+SELECT so a
// concurrent Acquire cannot hand connection i out mid-heal.
func (e *Epoch) Heal(i int, limit time.Duration) (time.Time, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if i < 0 || i >= len(e.raw) {
		return time.Time{}, fmt.Errorf("epoch: bad connection index %d", i)
	}
	pc := e.raw[i].Conn()
	_, _ = pc.Exec(context.Background(), "ROLLBACK")
	if err := pinEpochConn(context.Background(), e.raw[i]); err != nil {
		return time.Time{}, fmt.Errorf("epoch: heal pin connection %d: %w", i, err)
	}
	nf := time.Now()
	e.fix[i] = nf
	if limit > 0 && !healLagOKTimes(e.baseMin, nf, limit) {
		return nf, fmt.Errorf("epoch: heal connection %d lags the epoch base by %s > --max-snapshot-delta %s (do not restart the unit; fail -> resume)", i, nf.Sub(e.baseMin), limit)
	}
	return nf, nil
}

// Close rolls back all N open snapshot transactions and returns the N
// connections to the pool (the run's abort/error path). It is idempotent.
func (e *Epoch) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.end("ROLLBACK")
}

// CommitAll commits all N open snapshot transactions and returns the N
// connections to the pool (the run's successful Release). For read-only
// snapshot transactions this cleanly ends each and releases its snapshot
// (and its xmin, which pins the vacuum horizon). It is idempotent.
func (e *Epoch) CommitAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.end("COMMIT")
}

// end terminates all N transactions with stmt (COMMIT or ROLLBACK) and
// returns the connections to the pool. Caller holds e.mu.
func (e *Epoch) end(stmt string) {
	if e.closed {
		return
	}
	e.closed = true
	for _, c := range e.raw {
		if c == nil {
			continue
		}
		_, _ = c.Conn().Exec(context.Background(), stmt)
		c.Release()
	}
	e.raw = nil
	e.conns = nil
}

// ProbeAndHeal checks connection i's snapshot transaction and, only if it is
// aborted (a failed unit's SQL fault), re-pins it with a fresh REPEATABLE READ
// snapshot (a new t_fix[i], validated against baseMin). A healthy connection is
// left untouched (its snapshot point is preserved, so the epoch's ε-window is
// not widened). It returns nil if the connection is healthy or was healed
// within the delta limit; an error if the re-pin lags baseMin by more than
// `limit` (divergent — the caller must NOT restart the unit; fail -> resume).
// limit <= 0 skips the validation.
func (e *Epoch) ProbeAndHeal(i int, limit time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if i < 0 || i >= len(e.raw) {
		return fmt.Errorf("epoch: bad connection index %d", i)
	}
	pc := e.raw[i].Conn()
	// Probe: a healthy transaction answers SELECT 1. Exec (not Query) so the
	// probe leaves no open rows on the connection (open rows would keep the
	// conn busy for the next operation).
	if _, err := pc.Exec(context.Background(), "SELECT 1"); err == nil {
		return nil // healthy, leave untouched
	}
	// Aborted: roll back and re-pin (fresh REPEATABLE READ + clock probe).
	_, _ = pc.Exec(context.Background(), "ROLLBACK")
	if err := pinEpochConn(context.Background(), e.raw[i]); err != nil {
		return fmt.Errorf("epoch: heal pin connection %d: %w", i, err)
	}
	nf := time.Now()
	e.fix[i] = nf
	if limit > 0 && !healLagOKTimes(e.baseMin, nf, limit) {
		return fmt.Errorf("epoch: heal connection %d lags the epoch base by %s > --max-snapshot-delta %s (do not restart the unit; fail -> resume)", i, nf.Sub(e.baseMin), limit)
	}
	return nil
}

// MarkDivergent excludes connection i from the free-list after a divergent
// heal (its re-pin lags the epoch base by more than --max-snapshot-delta, so it
// must not carry a unit — that would copy a snapshot inconsistent with the rest
// of the run). The open transaction is left in place; Close/CommitAll ends it.
// It is not re-included.
func (e *Epoch) MarkDivergent(i int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if i >= 0 && i < len(e.divergent) {
		e.divergent[i] = true
		e.inUse[i] = true // keep it out of the free-list
	}
}

// reserveEpoch acquires n dedicated pool connections for a snapshot-epoch
// (P1d). They are removed from the pool's normal availability for the epoch's
// lifetime and returned by releaseEpoch. If the pool cannot supply all n, the
// ones already acquired are released and an error is returned (nothing is
// reserved). The pool must be sized >= n (the controller sizes it
// --workers + 4) so the acquires do not block on a too-small pool.
func (p *Pool) reserveEpoch(ctx context.Context, n int) ([]*pgxpool.Conn, error) {
	out := make([]*pgxpool.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := p.pool.Acquire(ctx)
		if err != nil {
			for _, rc := range out {
				rc.Release()
			}
			return nil, fmt.Errorf("pool: reserve epoch connection %d/%d: %w", i, n, err)
		}
		out = append(out, c)
	}
	return out, nil
}

// releaseEpoch returns the epoch's reserved connections to the pool.
func (p *Pool) releaseEpoch(cs []*pgxpool.Conn) {
	for _, c := range cs {
		if c != nil {
			c.Release()
		}
	}
}

// minFix is the earliest pin time (baseMin) in the epoch.
func minFix(fix []time.Time) time.Time {
	if len(fix) == 0 {
		return time.Time{}
	}
	mn := fix[0]
	for _, f := range fix[1:] {
		if f.Before(mn) {
			mn = f
		}
	}
	return mn
}

// spreadOf returns max(fix) − min(fix): the ε-window width of the N pins.
// A nil/short slice spreads zero.
func spreadOf(fix []time.Time) time.Duration {
	if len(fix) == 0 {
		return 0
	}
	mn, mx := fix[0], fix[0]
	for _, f := range fix[1:] {
		if f.Before(mn) {
			mn = f
		}
		if f.After(mx) {
			mx = f
		}
	}
	return mx.Sub(mn)
}

// validateSpread is the P1d --max-snapshot-delta gate: a pin spread d wider
// than limit (limit > 0) is rejected (the run must not start). limit <= 0
// disables the check.
func validateSpread(d, limit time.Duration) error {
	if limit <= 0 {
		return nil
	}
	if d > limit {
		return fmt.Errorf("snapshot-epoch: pin spread %s exceeds --max-snapshot-delta %s (the N pins are not in one point; the run must not start)", d, limit)
	}
	return nil
}

func healLagOKTimes(baseMin, newFix time.Time, limit time.Duration) bool {
	if limit <= 0 {
		return true
	}
	return newFix.Sub(baseMin) <= limit
}

// Compile-time assertion: an EpochConn is a StreamingQuerier (it carries a
// pipeline's data COPY / checksum re-read on its borrowed connection).
var _ StreamingQuerier = (*EpochConn)(nil)
