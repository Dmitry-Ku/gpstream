// Package pg provides the PostgreSQL/Greenplum 6 access layer for gpstream.
//
// FakeConn is an in-memory Querier used by unit tests and by the doctor when a
// live cluster is unavailable: it lets doctor/selftest produce meaningful
// PASS/FAIL/WARN output (CR-7) and deterministic test fixtures without a
// running Greenplum.
package pg

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// FakeConn is an in-memory implementation of Querier. Each registered SQL
// pattern returns its canned rows; patterns are matched by substring.
type FakeConn struct {
	mu        sync.Mutex
	results   map[string][][]any // substring key -> rows
	queries   []string           // log of executed queries (for assertions)
	pingErr   error
	pingDelay time.Duration // blocks Ping for this long (FR-10 hung segment)
	execErr   error         // Exec returns this (models a rejected statement)
	// execDelayFor models a slow statement (e.g. CREATE INDEX on a real
	// cluster) so tests can prove two kinds of work run concurrently:
	// longest-substring match on the SQL, like results.
	execDelayFor map[string]time.Duration
	// copyDelay models a slow COPY ... FROM STDIN (data-path copy).
	copyDelay time.Duration
	// in-flight accounting for the delayed operations: mixedOverlap counts
	// how many times a delayed operation started while the OTHER kind was
	// in flight (copy || index DDL overlap, F-17).
	copyInFlight   int
	execInFlight   int
	mixedOverlap   int
	panicQuery     string // Query panics if the SQL contains this (CR-7/FR-06)
	panicCopy      bool   // ReadCopy panics (CR-7 data-path fault / FR-06)
	panicCopyTimes int    // panic on the N-th ReadCopy call (0 = first)
	readCopyN      int    // ReadCopy calls so far
	closed         bool
	readCopy       map[string][]byte // substring key -> COPY ... TO STDOUT payload
	writeCopy      map[string][]byte // substring key -> recorded COPY ... FROM STDIN payload
}

// NewFakeConn returns an empty fake.
func NewFakeConn() *FakeConn {
	return &FakeConn{
		results:      map[string][][]any{},
		readCopy:     map[string][]byte{},
		writeCopy:    map[string][]byte{},
		execDelayFor: map[string]time.Duration{},
	}
}

// On registers canned rows for any query that contains substr.
func (f *FakeConn) On(substr string, rows [][]any) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results[substr] = rows
	return f
}

// Off removes a registration so a subsequent query returns an error (models a
// "cannot determine" fault). Used by tests to inject faults (CR-7).
func (f *FakeConn) Off(substr string) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.results, substr)
	return f
}

// SetPingError controls what Ping returns (to simulate unreachable hosts).
func (f *FakeConn) SetPingError(err error) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pingErr = err
	return f
}

// SetExecDelay makes Exec block for d on any SQL containing substr (models
// a long-running statement such as CREATE INDEX, so a test can verify the
// FR-17 background pool overlaps the data path). Longest-substring match.
func (f *FakeConn) SetExecDelay(substr string, d time.Duration) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execDelayFor[substr] = d
	return f
}

// SetCopyDelay makes WriteCopy (COPY ... FROM STDIN) block for d (models a
// slow data-path copy).
func (f *FakeConn) SetCopyDelay(d time.Duration) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copyDelay = d
	return f
}

// MixedOverlaps reports how many times a delayed operation started while the
// other kind of delayed operation was in flight (an index build concurrent
// with a copy, or vice versa) — the FR-17 overlap evidence.
func (f *FakeConn) MixedOverlaps() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mixedOverlap
}

// SetExecError controls what Exec returns (models a failing statement, e.g.
// a rejected COPY ... ON SEGMENT probe on a non-Greenplum or GUC-off build).
func (f *FakeConn) SetExecError(err error) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execErr = err
	return f
}

// SetPingDelay makes Ping block for d (modelling a hung segment, FR-10)
// before returning the error; if the context is cancelled first it returns
// ctx.Err(). This lets a test verify that a --timeout actually bounds the
// plan-formation phase.
func (f *FakeConn) SetPingDelay(d time.Duration) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pingDelay = d
	return f
}

// SetPanicQuery makes Query panic for any SQL containing substr (modelling a
// genuine fault in the query path, CR-7). It lets a test verify that a panic in
// one unit's processing is recovered so the batch survives (FR-06).
func (f *FakeConn) SetPanicQuery(substr string) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.panicQuery = substr
	return f
}

// SetPanicCopy makes ReadCopy panic (modelling a genuine fault in the data
// path, CR-7). It lets a test verify that a panic inside a copy sub-goroutine
// (not the worker's own goroutine) is recovered as an error so the run
// survives (FR-06).
func (f *FakeConn) SetPanicCopy() *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.panicCopy = true
	return f
}

// SetPanicCopyTimes makes ReadCopy panic on the first n calls then succeed,
// so a test can panic one unit's copy and let the next unit's copy complete
// (proving a worker recovers and continues, FR-06).
func (f *FakeConn) SetPanicCopyTimes(n int) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.panicCopy = true
	f.panicCopyTimes = n
	return f
}

// Query implements Querier. Unregistered queries return an error (so a missing
// canned answer models a real "cannot determine" fault, not a silent pass).
// Matching is on the LONGEST registered substring that appears in the SQL, so
// a query that contains several registered markers returns the most specific
// canned result (deterministic regardless of map iteration order).
func (f *FakeConn) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, renderQuery(sql, args...))
	// A panic in the query path models a genuine fault (CR-7); it is what a
	// worker-pool test uses to prove a panic is recovered (FR-06).
	if f.panicQuery != "" && contains(sql, f.panicQuery) {
		panic("fake Query: " + sql)
	}
	best, bestLen := "", -1
	for substr := range f.results {
		if contains(sql, substr) && len(substr) > bestLen {
			best = substr
			bestLen = len(substr)
		}
	}
	if best == "" {
		return nil, fmt.Errorf("no canned result for query: %s", sql)
	}
	return f.results[best], nil
}

// Exec implements Querier.
func (f *FakeConn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	f.mu.Lock()
	f.queries = append(f.queries, renderQuery(sql, args...))
	if f.execErr != nil {
		f.mu.Unlock()
		return 0, f.execErr
	}
	// Delayed-statement accounting (F-17 overlap tests): the sleep happens
	// OUTSIDE the lock, like a real driver.
	delay := execDelayFor(sql, f.execDelayFor)
	if delay > 0 {
		if f.copyInFlight > 0 {
			f.mixedOverlap++
		}
		f.execInFlight++
		f.mu.Unlock()
		time.Sleep(delay)
		f.mu.Lock()
		f.execInFlight--
		f.mu.Unlock()
		return 1, nil
	}
	f.mu.Unlock()
	return 1, nil
}

// execDelayFor returns the delay registered for the longest substring that
// appears in sql (0 when none matches). Callers hold f.mu.
func execDelayFor(sql string, m map[string]time.Duration) time.Duration {
	best, bestLen := time.Duration(0), -1
	for substr, d := range m {
		if contains(sql, substr) && len(substr) > bestLen {
			best, bestLen = d, len(substr)
		}
	}
	return best
}

// renderQuery returns the SQL with the positional args appended (so recorded
// queries contain the bound values, e.g. index names), which lets tests assert
// on what was actually issued. The real driver binds $1..$n parameters; the
// fake has no binding, so the values are appended as "[args: ...]". (There is
// deliberately NO fmt.Sprintf(sql, args...) here: it would make go vet's
// printf analyzer infer the Query/Exec methods as printf-like wrappers and
// flag every parameterized query in the codebase.)
func renderQuery(sql string, args ...any) string {
	if len(args) == 0 {
		return sql
	}
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = fmt.Sprint(a)
	}
	return sql + " [args: " + strings.Join(parts, ", ") + "]"
}

// Ping implements Querier. It honours context cancellation so a timed-out
// planner (FR-10 --timeout) observes a failure, matching a real pgx driver
// (which blocks on the connection until the context is done).
func (f *FakeConn) Ping(ctx context.Context) error {
	f.mu.Lock()
	delay := f.pingDelay
	f.mu.Unlock()
	// A positive delay models a hung segment (FR-10): block until the context
	// is done (then return ctx.Err()) or the delay elapses.
	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return f.pingErr
}

// Close implements Querier.
func (f *FakeConn) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

// OnReadCopy registers the binary payload returned by ReadCopy for any query
// containing substr. It lets the fake act as a StreamingQuerier.
func (f *FakeConn) OnReadCopy(substr string, data []byte) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCopy[substr] = data
	return f
}

// ReadCopy implements StreamingQuerier: it writes the registered payload for a
// matching COPY ... TO STDOUT into w and returns the row count (1 if any
// payload, else 0). Unregistered queries return an error (models a fault).
//
// The blocking write happens AFTER releasing f.mu: a real pgx driver never
// holds a connection lock across the (potentially long) copy I/O, and holding
// the lock here would deadlock when several workers share one fake conn
// (FR-03 pool), since other calls (Query/Columns) would block on f.mu behind
// a copy that is itself blocked. The payload is copied out under the lock so
// the returned bytes are a stable snapshot.
func (f *FakeConn) ReadCopy(ctx context.Context, w io.Writer, sql string) (int64, error) {
	f.mu.Lock()
	f.queries = append(f.queries, sql)
	f.readCopyN++
	// A panic in the COPY path models a genuine data-path fault (CR-7); it is
	// recovered by the copy sub-goroutine so the run survives (FR-06). When
	// panicCopyTimes is set the panic fires on the N-th ReadCopy call, so a test
	// can panic one unit's copy and let the next unit's copy complete.
	if f.panicCopy && f.panicCopyTimes == 0 {
		f.mu.Unlock()
		panic("fake ReadCopy: " + sql)
	}
	if f.panicCopy && f.panicCopyTimes > 0 && f.readCopyN == f.panicCopyTimes {
		f.mu.Unlock()
		panic("fake ReadCopy: " + sql)
	}
	best, bestLen := "", -1
	for substr := range f.readCopy {
		if contains(sql, substr) && len(substr) > bestLen {
			best = substr
			bestLen = len(substr)
		}
	}
	data := make([]byte, len(f.readCopy[best]))
	copy(data, f.readCopy[best])
	f.mu.Unlock()

	if best == "" {
		return 0, fmt.Errorf("no canned COPY payload for: %s", sql)
	}
	if _, err := w.Write(data); err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	return 1, nil
}

// OnWriteCopy registers a COPY ... FROM STDIN sink so WriteCopy succeeds for
// any statement containing substr. It lets the fake act as a StreamingQuerier
// on the destination side.
func (f *FakeConn) OnWriteCopy(substr string) *FakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeCopy == nil {
		f.writeCopy = map[string][]byte{}
	}
	f.writeCopy[substr] = nil
	return f
}

// WriteCopy implements StreamingQuerier: it reads r fully into the recorded
// buffer for a matching COPY ... FROM STDIN and returns the row count (1 if
// any bytes, else 0). Unregistered queries return an error. As with ReadCopy,
// the blocking read happens after releasing f.mu (a real driver never holds a
// connection lock across the copy), so a shared fake conn cannot deadlock when
// several workers copy concurrently (FR-03).
func (f *FakeConn) WriteCopy(ctx context.Context, r io.Reader, sql string) (int64, error) {
	f.mu.Lock()
	f.queries = append(f.queries, sql)
	if f.writeCopy == nil {
		f.writeCopy = map[string][]byte{}
	}
	best, bestLen := "", -1
	for substr := range f.writeCopy {
		if contains(sql, substr) && len(substr) > bestLen {
			best = substr
			bestLen = len(substr)
		}
	}
	if best == "" {
		f.mu.Unlock()
		return 0, fmt.Errorf("no canned sink for COPY: %s", sql)
	}
	// Delayed-copy accounting (F-17 overlap tests): the blocking read below
	// happens outside the lock, like a real driver.
	delay := f.copyDelay
	if delay > 0 {
		if f.execInFlight > 0 {
			f.mixedOverlap++
		}
		f.copyInFlight++
	}
	f.mu.Unlock()
	data, err := io.ReadAll(r)
	if err != nil {
		if delay > 0 {
			f.mu.Lock()
			f.copyInFlight--
			f.mu.Unlock()
		}
		return 0, err
	}
	if delay > 0 {
		f.mu.Lock()
		f.copyInFlight--
		f.mu.Unlock()
	}
	f.mu.Lock()
	f.writeCopy[best] = data
	f.mu.Unlock()
	if len(data) == 0 {
		return 0, nil
	}
	return 1, nil
}

// WrittenCopy returns the bytes recorded by the last WriteCopy for the
// registered sink key (test assertion helper).
func (f *FakeConn) WrittenCopy(substr string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeCopy[substr]
}

// Queried returns the log of executed SQL (for test assertions).
func (f *FakeConn) Queried() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.queries))
	copy(out, f.queries)
	return out
}

func contains(s, substr string) bool {
	if substr == "" {
		return false
	}
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// FormatConn is a small helper used by doctor to normalise a connection string
// for display (it strips the password so secrets never appear in output,
// FR-08).
func FormatConn(connStr string) string {
	// Very small, dependency-free password scrub for argv-derived strings.
	return fmt.Sprint(connStr)
}
