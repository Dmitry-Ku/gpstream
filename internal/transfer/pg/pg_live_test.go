//go:build integration

package pg

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fergusstrange/embedded-postgres"
)

// startPGLive starts a real (embedded) PostgreSQL server and returns a pgx
// connection to it. This exercises the *live* Conn (not the fake) — the
// pgx/pgxpool wrappers in pg.go and the COPY paths in copy.go, which the
// fake-backed tests cannot reach.
func startPGLive(t *testing.T, port uint32) *Conn {
	t.Helper()
	dataDir := "/tmp/pg_live_" + t.Name()
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatalf("rm data dir: %v", err)
	}
	cfg := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Username("gp").
		Password("secret").
		Database("gp_test").
		Port(port).
		DataPath(dataDir).
		StartTimeout(60 * time.Second)
	srv := embeddedpostgres.NewDatabase(cfg)
	if err := srv.Start(); err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	ctx := context.Background()
	c, err := NewConn(ctx, cfg.GetConnectionURL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect %s: %v", t.Name(), err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// TestLiveConnQueryExecPing covers the pgx-backed Query/Exec/Ping/Close paths
// of *Conn (pg.go), which the fake-backed unit tests cannot reach.
func TestLiveConnQueryExecPing(t *testing.T) {
	c := startPGLive(t, 5601)

	// Ping must succeed against a live server.
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Query: a SELECT returns the first cell of the first row.
	if got, err := c.Query(context.Background(), `SELECT 1`); err != nil || len(got) == 0 || len(got[0]) == 0 {
		t.Fatalf("Query(SELECT 1): got=%v err=%v", got, err)
	}

	// Exec: DDL/DML succeeds against a live server (one statement per call).
	if _, err := c.Exec(context.Background(), `CREATE TABLE t (id int)`); err != nil {
		t.Fatalf("exec DDL: %v", err)
	}
	for _, row := range []int{1, 2} {
		if _, err := c.Exec(context.Background(), `INSERT INTO t VALUES ($1)`, row); err != nil {
			t.Fatalf("exec insert %d: %v", row, err)
		}
	}

	// A COUNT query round-trips through QueryRow.
	if got, err := c.Query(context.Background(), `SELECT count(*) FROM t`); err != nil || len(got) == 0 || got[0][0] == nil {
		t.Fatalf("count query: got=%v err=%v", got, err)
	}
}

// TestLiveCopyRoundTrip exercises the live COPY ... TO STDOUT / FROM STDIN
// paths (copy.go) end-to-end against a real server: rows are read out via
// ReadCopy and the exact bytes are written back via WriteCopy (FR-01).
func TestLiveCopyRoundTrip(t *testing.T) {
	c := startPGLive(t, 5602)
	ctx := context.Background()

	if _, err := c.Exec(ctx, `CREATE TABLE orders (id int, name text)`); err != nil {
		t.Fatalf("seed DDL: %v", err)
	}
	for _, row := range [][]any{{1, "a"}, {2, "b"}, {3, "c"}} {
		if _, err := c.Exec(ctx, `INSERT INTO orders VALUES ($1,$2)`, row[0], row[1]); err != nil {
			t.Fatalf("seed insert %v: %v", row, err)
		}
	}

	// ReadCopy: COPY ... TO STDOUT streams the rows into a buffer (FR-01).
	var buf strings.Builder
	n, err := c.ReadCopy(ctx, &buf, `COPY orders TO STDOUT`)
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if n != 3 {
		t.Errorf("read copy rows = %d, want 3", n)
	}
	if strings.Count(buf.String(), "\n") < 1 {
		t.Errorf("read copy produced no rows: %q", buf.String())
	}

	// WriteCopy: feed the exact bytes back in (FR-01 destination side).
	m, err := c.WriteCopy(ctx, strings.NewReader(buf.String()), `COPY orders FROM STDIN`)
	if err != nil {
		t.Fatalf("write copy: %v", err)
	}
	if m != 3 {
		t.Errorf("write copy rows = %d, want 3", m)
	}
}
