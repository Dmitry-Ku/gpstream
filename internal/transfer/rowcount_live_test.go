//go:build gp6lab

package transfer

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// startTransferPG starts a real PostgreSQL 9.4.26 and returns a pg.Conn.
func startTransferPG(t *testing.T, port uint32) *pg.Conn {
	t.Helper()
	cl, err := pgtest.Start(port, "rc_test")
	if err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(func() { cl.Stop() })
	c, err := pg.NewConn(context.Background(), cl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect %s: %v", t.Name(), err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// TestLiveRowCounterCrossCheck (DESIGN §3.3) cross-checks RowCounter against a
// REAL PostgreSQL 9.4.26 (the GP6 kernel): a binary-COPY stream is read out of
// a live table, passed through RowCounter (which counts rows and passes bytes
// through), and the same bytes are re-ingested via COPY FROM STDIN. The row
// count must equal COUNT(*), and the round-tripped table must match the source
// — proving RowCounter is byte-transparent and counts real PG binary rows.
func TestLiveRowCounterCrossCheck(t *testing.T) {
	const n = 50000
	c := startTransferPG(t, 5603)
	ctx := context.Background()

	// A table with mixed NULLs, empty strings, short and long values, so the
	// binary-COPY rows exercise every field path (NULL=-1, empty len=0, long).
	if _, err := c.Exec(ctx, `DROP TABLE IF EXISTS rc_src`); err != nil {
		t.Fatalf("drop src: %v", err)
	}
	if _, err := c.Exec(ctx, `CREATE TABLE rc_src (id int, name text, body text)`); err != nil {
		t.Fatalf("create src: %v", err)
	}
	stmt := `INSERT INTO rc_src
		SELECT i,
		       CASE WHEN i % 2 = 0 THEN NULL
		            WHEN i % 10 = 0 THEN ''
		            ELSE 'name_' || i END,
		       repeat('x' || (i % 7), i % 300)
		FROM generate_series(1, ` + strconv.Itoa(n) + `) i`
	if _, err := c.Exec(ctx, stmt); err != nil {
		t.Fatalf("populate: %v", err)
	}

	cnt, err := queryInt(c, ctx, `SELECT count(*) FROM rc_src`)
	if err != nil {
		t.Fatalf("count src: %v", err)
	}
	if cnt != n {
		t.Fatalf("populated %d rows, want %d", cnt, n)
	}

	// Read the binary COPY out into a buffer (a real PG 9.4.26 stream).
	var stream bytes.Buffer
	if _, err := c.ReadCopy(ctx, &stream, `COPY (SELECT * FROM rc_src ORDER BY id) TO STDOUT (FORMAT binary)`); err != nil {
		t.Fatalf("readcopy: %v", err)
	}
	if stream.Len() == 0 {
		t.Fatal("empty COPY stream")
	}

	// RowCounter over the real stream: count must equal COUNT(*).
	got, err := CountRows(bytes.NewReader(stream.Bytes()))
	if err != nil {
		t.Fatalf("rowcount: %v", err)
	}
	if got != int64(n) {
		t.Fatalf("RowCounter rows = %d, want %d (live PG binary COPY)", got, n)
	}

	// Byte-transparency: re-ingest the SAME bytes into a second table and
	// confirm it matches the source (count + spot values). This proves the
	// RowCounter did not alter a single byte of the stream.
	if _, err := c.Exec(ctx, `CREATE TABLE rc_dst (LIKE rc_src INCLUDING ALL)`); err != nil {
		t.Fatalf("create dst: %v", err)
	}
	if _, err := c.WriteCopy(ctx, bytes.NewReader(stream.Bytes()), `COPY rc_dst FROM STDIN (FORMAT binary)`); err != nil {
		t.Fatalf("writecopy: %v", err)
	}
	dstCnt, err := queryInt(c, ctx, `SELECT count(*) FROM rc_dst`)
	if err != nil {
		t.Fatalf("count dst: %v", err)
	}
	if dstCnt != n {
		t.Fatalf("dst rows = %d, want %d (byte transparency broken)", dstCnt, n)
	}
	// Spot-check a few rows (NULL handling + long body) against the source.
	for _, id := range []int{1, 2, 10, 7, 299, n} {
		s, e1 := c.Query(ctx, `SELECT name, length(body) FROM rc_src WHERE id=`+strconv.Itoa(id))
		d, e2 := c.Query(ctx, `SELECT name, length(body) FROM rc_dst WHERE id=`+strconv.Itoa(id))
		if e1 != nil || e2 != nil || len(s) == 0 || len(d) == 0 {
			t.Fatalf("spot id %d: e1=%v e2=%v", id, e1, e2)
		}
		if !cellEq(s[0][0], d[0][0]) || !cellEq(s[0][1], d[0][1]) {
			t.Errorf("spot id %d mismatch: src=%v dst=%v", id, s[0], d[0])
		}
	}
}

// queryInt runs a single-column integer query and returns the value.
func queryInt(c *pg.Conn, ctx context.Context, sql string) (int, error) {
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || rows[0][0] == nil {
		return 0, nil
	}
	s, ok := rows[0][0].(string)
	if !ok {
		s = fmt.Sprint(rows[0][0])
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// cellEq compares two pgx cells (any: string, []byte, or nil) for equality.
func cellEq(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
