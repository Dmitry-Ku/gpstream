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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

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

// ensureVerifyFull enforces FR-13: when the user's DSN does not specify
// sslmode explicitly, the connection is made with sslmode=verify-full.
// An explicit sslmode (any value) is respected as-is.
func ensureVerifyFull(connStr string) string {
	if strings.Contains(connStr, "sslmode=") {
		return connStr
	}
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		if strings.Contains(connStr, "?") {
			return connStr + "&sslmode=verify-full"
		}
		return connStr + "?sslmode=verify-full"
	}
	// Keyword/value form: "host=... port=..."
	if connStr != "" && !strings.HasSuffix(connStr, " ") {
		connStr += " "
	}
	return connStr + "sslmode=verify-full"
}

// hostOf extracts the connection host from a DSN (URL or keyword/value
// form) for TLS hostname verification (FR-13): the host must be known
// before the TLS config is built, so it cannot come from pgx.ParseConfig.
func hostOf(connStr string) (string, error) {
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		u, err := url.Parse(connStr)
		if err != nil {
			return "", fmt.Errorf("conn: %w", err)
		}
		return u.Hostname(), nil
	}
	for _, kv := range strings.Fields(connStr) {
		if i := strings.IndexByte(kv, '='); i > 0 && kv[:i] == "host" {
			return kv[i+1:], nil
		}
	}
	return "", nil
}

// NewConn opens a connection from a connection string, honouring the TLS
// flags from FR-13. The secret material (cert/key/ca paths) is resolved here,
// never passed via argv.
func NewConn(ctx context.Context, connStr, sslCert, sslKey, sslCA, minTLS string) (*Conn, error) {
	connStr = ensureVerifyFull(connStr)
	host, err := hostOf(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse conn: %w", err)
	}
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse conn: %w", err)
	}
	if sslCA != "" || sslCert != "" {
		cfg.TLSConfig, err = buildTLS(host, sslCert, sslKey, sslCA, minTLS)
		if err != nil {
			return nil, err
		}
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

// buildTLS returns a *tls.Config for PG connections (FR-13). With
// sslmode=verify-full (the enforced default, ensureVerifyFull) the
// connection MUST validate the server's hostname: host (the DSN host) is
// set as the TLS ServerName so crypto/tls checks the certificate's
// SAN/CN against it — a wrong certificate is a hard, diagnosed failure
// (F-13 acceptance: «при неверном сертификате — отказ с диагностикой»).
// An IP-literal host cannot carry a verifiable name, so ServerName stays
// empty there (verify-full degrades to verify-ca; the hostname check
// applies to named hosts). A supplied CA bundle is loaded into RootCAs
// for chain validation. Errors (unreadable CA, malformed client keypair)
// are returned instead of swallowed: a silently-downgraded TLS config
// would defeat the guarantee.
func buildTLS(host, cert, key, ca, minTLS string) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if host != "" && net.ParseIP(host) == nil {
		// F-13: verify-full hostname check. pgx sets the same field for
		// sslmode=verify-full, but this config REPLACES pgx's, so it must
		// carry the name itself.
		tlsCfg.ServerName = host
	}
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("ssl-ca %s: %w", ca, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ssl-ca %s: no valid certificates found", ca)
		}
		tlsCfg.RootCAs = pool
	}
	if cert != "" && key != "" {
		// mTLS: client presents a certificate to the server (FR-13).
		peer, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("ssl-cert/ssl-key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{peer}
	}
	if v, err := minVersion(minTLS); err == nil {
		tlsCfg.MinVersion = v
	}
	return tlsCfg, nil
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

// DatabaseName returns the database encoded in a libpq-style connection
// string — i.e. the database a pool on that string connects to. The bool is
// false when the string does not parse or carries no database. The planner
// uses it to compare collations against the database the runtime actually
// writes to: every table is copied through the single destination pool, so
// the data lands in the --dest connection's database, not in a per-table
// "DstDB" label (FR-16).
func DatabaseName(connStr string) (string, bool) {
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil || cfg == nil {
		return "", false
	}
	if cfg.Database == "" {
		return "", false
	}
	return cfg.Database, true
}
