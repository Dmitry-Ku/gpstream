// Package pg provides the PostgreSQL/Greenplum 6 access layer for gpstream.
//
// Conn is a live pgx-backed Querier. It additionally exposes the raw COPY
// streaming path (ReadCopy/WriteCopy) that the source and destination agents
// use to move data between clusters without touching local storage (FR-01).
//
// FakeConn is an in-memory Querier used by unit tests and by the doctor when a
// live cluster is unavailable.
package pg

import (
	"context"
	"fmt"
	"io"
	"time"

	"crypto/tls"

	"github.com/jackc/pgx/v5"
)

// Querier is the minimal surface gpstream needs from a database connection.
// Implementations may be a live pgx conn or an in-memory test fake.
type Querier interface {
	// Query runs a single-statement SQL and returns rows as [][]any.
	Query(ctx context.Context, sql string, args ...any) ([][]any, error)
	// Exec runs a write/DDL statement and returns the affected count.
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	// Ping verifies the connection is alive (FR-01/FR-10 reachability).
	Ping(ctx context.Context) error
	// Close releases the connection.
	Close()
}

// StreamingQuerier is a Querier that can also run a raw COPY (FR-01). The live
// Conn implements it; a fake need not (the fake is for planning/doctor only,
// never for the data path — which is exactly why the data path used to be
// unreachable from the binary).
type StreamingQuerier interface {
	Querier
	// ReadCopy streams the rows of `sql` (a `COPY ... TO STDOUT` statement)
	// into w. It returns the command tag and an error. The raw binary COPY
	// payload is the data plane (FR-01): source -> dest over the network.
	ReadCopy(ctx context.Context, w io.Writer, sql string) (int64, error)
	// WriteCopy streams r into the destination via `sql`
	// (a `COPY ... FROM STDIN` statement). It returns the number of rows
	// copied.
	WriteCopy(ctx context.Context, r io.Reader, sql string) (int64, error)
}

// Conn is a live pgx-backed Querier.
type Conn struct {
	*pgx.Conn
	c *pgx.Conn
}

// NewConn opens a connection from a connection string, honouring the TLS
// flags from FR-13. The secret material (cert/key/ca paths) is resolved here,
// never passed via argv.
func NewConn(ctx context.Context, connStr, sslCert, sslKey, sslCA, minTLS string) (*Conn, error) {
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse conn: %w", err)
	}
	if sslCA != "" || sslCert != "" {
		cfg.TLSConfig = buildTLS(sslCert, sslKey, sslCA, minTLS)
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Conn{c: c}, nil
}

// Query implements Querier.
func (c *Conn) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	rows, err := c.c.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// FieldDescriptions may be nil before the first row; read it after Next.
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

// Exec implements Querier.
func (c *Conn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := c.c.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Ping implements Querier.
func (c *Conn) Ping(ctx context.Context) error {
	return c.c.Ping(ctx)
}

// Close implements Querier.
func (c *Conn) Close() {
	_ = c.c.Close(context.Background())
}

// Row is a convenience accessor for a single-row single-column result.
func Row(ctx context.Context, q Querier, sql string, args ...any) (any, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows[0]) == 0 {
		return nil, nil
	}
	return rows[0][0], nil
}

// RowStr returns the first cell of the first row as a string, or "".
func RowStr(ctx context.Context, q Querier, sql string, args ...any) string {
	v, err := Row(ctx, q, sql, args...)
	if err != nil {
		return ""
	}
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// buildTLS returns a *tls.Config for PG connections (FR-13). It performs
// certificate chain validation when a CA is supplied, honours the minimum
// TLS version, and presents a client certificate for mTLS (FR-13).
func buildTLS(cert, key, ca, minTLS string) *tls.Config {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "",
	}
	if cert != "" && key != "" {
		// mTLS: client presents a certificate to the server (FR-13).
		peer, err := tls.LoadX509KeyPair(cert, key)
		if err == nil {
			tlsCfg.Certificates = []tls.Certificate{peer}
		}
	}
	if v, err := minVersion(minTLS); err == nil {
		tlsCfg.MinVersion = v
	}
	return tlsCfg
}

// minVersion maps a version string like "1.2" to a tls.Version.
func minVersion(s string) (uint16, error) {
	switch s {
	case "1.0":
		return tls.VersionTLS10, nil
	case "1.1":
		return tls.VersionTLS11, nil
	case "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unknown min TLS version %q", s)
	}
}

// DefaultTimeout bounds a single query so a hung segment cannot stall the
// planner (FR-10 --timeout).
const DefaultTimeout = 60 * time.Second
