package pg

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool is a live, concurrency-safe PostgreSQL/Greenplum access layer backed by
// a pgxpool. Unlike the single-connection Conn (safe for one in-flight
// statement), a Pool hands each operation its own connection, so the Manager's
// parallel stream workers (FR-03, --streams) can COPY simultaneously without a
// "conn busy" collision. The single-Conn was only ever exercised against the
// concurrency-safe FakeConn in tests, so the live multi-worker path first
// failed here (E1 e2e, 27.09).
//
// Pool implements StreamingQuerier, so the source/destination agents and the
// journal use it exactly as they use a Conn.
type Pool struct {
	pool *pgxpool.Pool

	// pinned holds the one connection reserved for a FR-02 snapshot
	// transaction (--consistent-db/--consistent-cluster). While non-nil, all
	// source data reads must run on it so they share the single
	// REPEATABLE READ snapshot. It is acquired by Pin and released by the
	// matching Release, never returned to the pool in between.
	pinMu    sync.Mutex
	pinned   *pgxpool.Conn
	pinnedOK bool
}

// NewPool opens a connection pool from a connection string, honouring the TLS
// flags from FR-13 (same sslmode/TLS handling as NewConn). maxConns bounds the
// pool; 0 (default) lets pgxpool size it from the configured concurrency. The
// pool is sized to comfortably serve the Manager's worker pool plus the
// journal, so concurrent COPYs never block on acquisition.
func NewPool(ctx context.Context, connStr, sslCert, sslKey, sslCA, minTLS string, maxConns int) (*Pool, error) {
	connStr = ensureVerifyFull(connStr)
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse conn: %w", err)
	}
	if sslCA != "" || sslCert != "" {
		cfg.ConnConfig.TLSConfig = buildTLS(sslCert, sslKey, sslCA, minTLS)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	// Fail fast if the pool cannot reach the server at all.
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Pool{pool: p}, nil
}

// Query implements Querier on an acquired connection.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	c, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Release()
	rows, err := c.Conn().Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		flds := rows.FieldDescriptions()
		vals := make([]any, len(flds))
		ptrs := make([]any, len(flds))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Exec implements Querier on an acquired connection.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	c, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	tag, err := c.Conn().Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Ping implements Querier.
func (p *Pool) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// Close implements Querier.
func (p *Pool) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

// ReadCopy implements StreamingQuerier: it streams a `COPY ... TO STDOUT`
// statement on an acquired connection (FR-01 source data plane).
func (p *Pool) ReadCopy(ctx context.Context, w io.Writer, sql string) (int64, error) {
	c, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	tag, err := c.Conn().PgConn().CopyTo(ctx, w, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// WriteCopy implements StreamingQuerier: it streams r into the destination via
// a `COPY ... FROM STDIN` statement on an acquired connection (FR-01 dest).
func (p *Pool) WriteCopy(ctx context.Context, r io.Reader, sql string) (int64, error) {
	c, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	tag, err := c.Conn().PgConn().CopyFrom(ctx, r, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Pin reserves ONE connection from the pool for the duration of a FR-02
// snapshot transaction and returns it as a StreamingQuerier bound to that
// single connection. All source data reads issued through the returned querier
// share the one open REPEATABLE READ transaction, which is what makes
// --consistent-db/--consistent-cluster read a single point-in-time snapshot
// even with parallel workers. The caller MUST call the returned release func
// (after COMMIT) to return the connection to the pool.
//
// Only one connection may be pinned at a time; a second Pin returns an error.
func (p *Pool) Pin(ctx context.Context) (q StreamingQuerier, release func(), err error) {
	p.pinMu.Lock()
	defer p.pinMu.Unlock()
	if p.pinned != nil {
		return nil, nil, fmt.Errorf("pool: a snapshot connection is already pinned")
	}
	c, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("pool: pin: %w", err)
	}
	p.pinned = c
	p.pinnedOK = true
	pinned := &pinnedConn{c: c, pool: p}
	release = func() {
		p.pinMu.Lock()
		defer p.pinMu.Unlock()
		if p.pinned == c {
			c.Release()
			p.pinned = nil
		}
	}
	return pinned, release, nil
}

// pinnedConn is a StreamingQuerier bound to a single held pool connection, so
// every operation (including the snapshot BEGIN/COMMIT and the COPYs inside it)
// runs on the same backend and therefore the same transaction.
//
// A FR-02 snapshot is per-transaction and a transaction is per-connection, so
// every source read in a --consistent-db run funnels through this ONE
// connection. Parallel workers must therefore be serialized on it (a source
// COPY is a long-lived stream); mu guarantees that, so a snapshot run is
// source-serial by construction rather than a "conn busy" collision.
type pinnedConn struct {
	c    *pgxpool.Conn
	pool *Pool
	mu   sync.Mutex
}

func (pc *pinnedConn) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	rows, err := pc.c.Conn().Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		flds := rows.FieldDescriptions()
		vals := make([]any, len(flds))
		ptrs := make([]any, len(flds))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (pc *pinnedConn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	tag, err := pc.c.Conn().Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (pc *pinnedConn) Ping(ctx context.Context) error {
	return pc.c.Conn().Ping(ctx)
}

func (pc *pinnedConn) Close() {
	// Do NOT release here: the pinned connection is owned by Pool.Pin's
	// release func, which is the only correct owner to return it to the pool.
}

func (pc *pinnedConn) ReadCopy(ctx context.Context, w io.Writer, sql string) (int64, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	tag, err := pc.c.Conn().PgConn().CopyTo(ctx, w, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (pc *pinnedConn) WriteCopy(ctx context.Context, r io.Reader, sql string) (int64, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	tag, err := pc.c.Conn().PgConn().CopyFrom(ctx, r, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Compile-time assertions: Pool and pinnedConn satisfy the live access layer.
var (
	_ StreamingQuerier = (*Pool)(nil)
	_ StreamingQuerier = (*pinnedConn)(nil)
	_ Querier          = (*Pool)(nil)
)

// AcquireSnapshotConn hands out a DEDICATED pool connection for a per-table
// snapshot transaction (FR-02 per-table fallback, --consistent-tables): the
// caller runs BEGIN ... COMMIT on the returned querier so the whole
// statement sequence of ONE table (the data COPY plus the checksum read)
// shares one short REPEATABLE READ snapshot. Unlike Pin (one run-wide
// snapshot connection, mutually exclusive), many snapshot connections may be
// out at once — each in-flight unit gets its own, so parallel workers each
// hold their table's snapshot independently. The release func MUST be called
// after COMMIT/ROLLBACK to return the connection to the pool.
func (p *Pool) AcquireSnapshotConn(ctx context.Context) (q StreamingQuerier, release func(), err error) {
	c, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("pool: snapshot conn: %w", err)
	}
	pinned := &pinnedConn{c: c, pool: p}
	release = func() {
		c.Release()
	}
	return pinned, release, nil
}

// reference so the pgx import is used even if pgxpool covers it transitively.
var _ = pgx.ErrNoRows
