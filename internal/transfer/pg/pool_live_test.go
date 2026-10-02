//go:build gp6lab

package pg

// Live coverage for Pool (pool.go): the pgxpool-backed access layer the
// Manager's parallel workers use (FR-03). The unit tests run against the
// concurrency-safe FakeConn and cannot reach this code; the live E1 e2e
// (27.09) is what first caught the "conn busy" defect here, so the pool's
// full surface (Query/Exec/Ping/Close/ReadCopy/WriteCopy/Pin/
// AcquireSnapshotConn, plus the pinnedConn wrapper) is exercised against a
// real PG 9.4.26.

import (
	"bytes"
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/pgtest"
)

func startPoolLive(t *testing.T, port uint32, maxConns int32) *Pool {
	t.Helper()
	cl, err := pgtest.Start(port, "gp_pool")
	if err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(func() { cl.Stop() })

	ctx := context.Background()
	p, err := NewPool(ctx, cl.URL(), "", "", "", "", int(maxConns))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestLivePoolQueryExecPingClose(t *testing.T) {
	p := startPoolLive(t, 5611, 4)
	ctx := context.Background()

	if err := p.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if _, err := p.Exec(ctx, `CREATE TABLE pt (id int, v text)`); err != nil {
		t.Fatalf("Exec DDL: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := p.Exec(ctx, `INSERT INTO pt VALUES ($1, $2)`, i, "r"+string(rune('a'+i))); err != nil {
			t.Fatalf("Exec insert: %v", err)
		}
	}
	rows, err := p.Query(ctx, `SELECT count(*) FROM pt`)
	if err != nil || len(rows) == 0 {
		t.Fatalf("Query count: rows=%v err=%v", rows, err)
	}
	if n, ok := rows[0][0].(int64); !ok || n != 5 {
		t.Errorf("count = %v, want 5", rows[0][0])
	}

	// Row/RowStr (pg.go) through the pool.
	if got := RowStr(ctx, p, `SELECT v FROM pt WHERE id = 3`); got != "rd" {
		t.Errorf("RowStr = %q, want rd", got)
	}
	if got, err := Row(ctx, p, `SELECT count(*) FROM pt`); err != nil || got == nil {
		t.Errorf("Row count: %v err %v", got, err)
	}

	// Error paths: a bad statement must surface, not panic.
	if _, err := p.Query(ctx, `SELECT * FROM no_such_table`); err == nil {
		t.Error("Query on missing table: want error")
	}
	if _, err := p.Exec(ctx, `INSERT INTO pt BAD SYNTAX`); err == nil {
		t.Error("Exec bad syntax: want error")
	}

	// DatabaseName (F-16 fix: the planner needs the dest DB from the DSN).
	// The pool's DSN is keyword form with dbname=gp_pool.
	if got, ok := DatabaseName(`host=127.0.0.1 port=5432 user=gp dbname=srcdb sslmode=disable`); !ok || got != "srcdb" {
		t.Errorf("DatabaseName keyword = %q/%v, want srcdb/true", got, ok)
	}
	if got, ok := DatabaseName(`postgres://u@h:5432/srcdb?sslmode=disable`); !ok || got != "srcdb" {
		t.Errorf("DatabaseName URL = %q/%v, want srcdb/true", got, ok)
	}
	if _, ok := DatabaseName(`postgres://u@h:5432?sslmode=disable`); ok {
		t.Error("DatabaseName without db must be ok=false")
	}
	if _, ok := DatabaseName(`postgres://u@h:5432/%zz not a url`); ok {
		t.Error("DatabaseName unparseable must be ok=false")
	}
}

func TestLivePoolCopyRoundTrip(t *testing.T) {
	p := startPoolLive(t, 5612, 4)
	ctx := context.Background()

	if _, err := p.Exec(ctx, `CREATE TABLE src (id int, v text)`); err != nil {
		t.Fatalf("create src: %v", err)
	}
	for i := 1; i <= 100; i++ {
		if _, err := p.Exec(ctx, `INSERT INTO src VALUES ($1, $2)`, i, "payload-"+string(rune('0'+i/10))+string(rune('0'+i%10))); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	var buf bytes.Buffer
	n, err := p.ReadCopy(ctx, &buf, `COPY src TO STDOUT (FORMAT binary)`)
	if err != nil || n != 100 {
		t.Fatalf("ReadCopy = %d rows err %v, want 100", n, err)
	}
	if buf.Len() == 0 {
		t.Fatal("ReadCopy produced no bytes")
	}

	if _, err := p.Exec(ctx, `CREATE TABLE dst (id int, v text)`); err != nil {
		t.Fatalf("create dst: %v", err)
	}
	m, err := p.WriteCopy(ctx, &buf, `COPY dst FROM STDIN (FORMAT binary)`)
	if err != nil || m != 100 {
		t.Fatalf("WriteCopy = %d rows err %v, want 100", m, err)
	}
	if rows, _ := p.Query(ctx, `SELECT count(*) FROM dst`); rows[0][0] == nil {
		t.Fatal("dst count missing")
	} else if got, _ := rows[0][0].(int64); got != 100 {
		t.Errorf("dst count = %d, want 100", got)
	}
}

// TestLivePoolPinSnapshot: Pin hands out ONE connection for a FR-02 snapshot
// transaction; a second Pin is rejected; release returns the connection. The
// pinned querier must share the one transaction (a writer on ANOTHER pool
// connection is invisible to the pinned snapshot).
func TestLivePoolPinSnapshot(t *testing.T) {
	p := startPoolLive(t, 5613, 4)
	ctx := context.Background()

	if _, err := p.Exec(ctx, `CREATE TABLE snap (id int)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.Exec(ctx, `INSERT INTO snap VALUES (1)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	q, release, err := p.Pin(ctx)
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	// A second Pin while one is out must fail.
	if _, _, err := p.Pin(ctx); err == nil {
		t.Fatal("second Pin: want error (already pinned)")
	}
	defer release()

	// Open the snapshot transaction on the pinned connection (the manager's
	// exact form: BEGIN with the isolation level in one statement).
	if _, err := q.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ`); err != nil {
		t.Fatalf("BEGIN RR: %v", err)
	}
	count, err := q.Query(ctx, `SELECT count(*) FROM snap`)
	if err != nil || count[0][0] == nil {
		t.Fatalf("pinned Query: %v %v", count, err)
	}
	if got, _ := count[0][0].(int64); got != 1 {
		t.Fatalf("pinned count = %d, want 1", got)
	}

	// A writer on a DIFFERENT pool connection is invisible to the snapshot.
	if _, err := p.Exec(ctx, `INSERT INTO snap VALUES (2)`); err != nil {
		t.Fatalf("writer insert: %v", err)
	}
	count2, err := q.Query(ctx, `SELECT count(*) FROM snap`)
	if err != nil {
		t.Fatalf("pinned requery: %v", err)
	}
	if got, _ := count2[0][0].(int64); got != 1 {
		t.Errorf("snapshot count = %d, want 1 (repeatable read must ignore the concurrent writer)", got)
	}

	// The pinned querier's COPY paths run on the same connection/transaction.
	var buf bytes.Buffer
	if _, err := q.ReadCopy(ctx, &buf, `COPY snap TO STDOUT (FORMAT binary)`); err != nil {
		t.Fatalf("pinned ReadCopy: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("pinned ReadCopy produced no bytes")
	}

	if _, err := q.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
	release()

	// After release, a fresh Pin must succeed again.
	q2, release2, err := p.Pin(ctx)
	if err != nil {
		t.Fatalf("re-Pin: %v", err)
	}
	if _, err := q2.Query(ctx, `SELECT 1`); err != nil {
		t.Fatalf("re-Pin Query: %v", err)
	}
	release2()
}

// TestLivePoolAcquireSnapshotConn: the per-table fallback (FR-02) hands out
// MANY independent snapshot connections at once — each in-flight unit owns
// its own, so parallel workers hold their table's snapshot independently.
func TestLivePoolAcquireSnapshotConn(t *testing.T) {
	p := startPoolLive(t, 5614, 8)
	ctx := context.Background()

	if _, err := p.Exec(ctx, `CREATE TABLE ptab (id int)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Two snapshot connections out at once (the point: not mutually
	// exclusive like Pin).
	q1, rel1, err := p.AcquireSnapshotConn(ctx)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	defer rel1()
	q2, rel2, err := p.AcquireSnapshotConn(ctx)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	defer rel2()

	if _, err := q1.Exec(ctx, `BEGIN`); err != nil {
		t.Fatalf("q1 begin: %v", err)
	}
	if _, err := q2.Exec(ctx, `BEGIN`); err != nil {
		t.Fatalf("q2 begin: %v", err)
	}
	if _, err := q1.Exec(ctx, `INSERT INTO ptab VALUES (1)`); err != nil {
		t.Fatalf("q1 insert: %v", err)
	}
	// q2's snapshot (opened before q1's insert committed... it hasn't) —
	// both are in uncommitted transactions; q2 cannot see q1's row.
	rows, err := q2.Query(ctx, `SELECT count(*) FROM ptab`)
	if err != nil {
		t.Fatalf("q2 count: %v", err)
	}
	if got, _ := rows[0][0].(int64); got != 0 {
		t.Errorf("q2 sees %d rows, want 0 (q1's insert uncommitted)", got)
	}
	if _, err := q1.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatalf("q1 rollback: %v", err)
	}
	if _, err := q2.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatalf("q2 rollback: %v", err)
	}

	// The snapshot connection also serves a COPY.
	if _, err := q1.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := q1.ReadCopy(ctx, &buf, `COPY ptab TO STDOUT (FORMAT binary)`); err != nil {
		t.Fatalf("snapshot ReadCopy: %v", err)
	}
	if _, err := q1.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}
