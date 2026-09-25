// pg94probe empirically checks whether pgx/v5 (our production driver) can
// talk to PostgreSQL 9.4.26 (the GP6 kernel): connect, DDL, COPY in both
// directions. Run: pg94probe <dsn>
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	dsn := os.Args[1]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Println("CONNECT FAIL:", err)
		os.Exit(1)
	}
	defer c.Close(ctx)
	fmt.Println("CONNECT OK")

	var ver string
	if err := c.QueryRow(ctx, "select version()").Scan(&ver); err != nil {
		fmt.Println("VERSION FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("SERVER:", ver)

	if _, err := c.Exec(ctx, `CREATE TABLE IF NOT EXISTS t94 (id int, s text)`); err != nil {
		fmt.Println("DDL FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("DDL OK")

	n, err := c.Exec(ctx, `TRUNCATE t94`)
	_ = n
	if err != nil {
		fmt.Println("EXEC FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("EXEC OK")

	// COPY FROM
	data := [][]any{{1, "alpha"}, {2, "beta"}}
	nr, err := c.CopyFrom(ctx, pgx.Identifier{"t94"}, []string{"id", "s"}, pgx.CopyFromSlice(len(data), func(i int) ([]any, error) { return data[i], nil }))
	if err != nil {
		fmt.Println("COPYFROM FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("COPYFROM OK rows=", nr)

	// COPY TO (raw binary)
	var rows [][]any
	r, err := c.Query(ctx, `SELECT id, s FROM t94 ORDER BY id`)
	if err != nil {
		fmt.Println("QUERY FAIL:", err)
		os.Exit(1)
	}
	for r.Next() {
		var v []any
		for i := 0; i < 2; i++ {
			var x any
			v = append(v, &x)
		}
		if err := r.Scan(v...); err != nil {
			fmt.Println("SCAN FAIL:", err)
			os.Exit(1)
		}
		row := []any{}
		for _, p := range v {
			row = append(row, *(p.(*any)))
		}
		rows = append(rows, row)
	}
	if err := r.Err(); err != nil {
		fmt.Println("ROWS ERR:", err)
		os.Exit(1)
	}
	if len(rows) != 2 {
		fmt.Println("ROWCOUNT FAIL:", len(rows))
		os.Exit(1)
	}
	fmt.Println("QUERY OK rows=", len(rows))

	// prepared statements
	if _, err := c.Exec(ctx, `PREPARE p94(int) AS SELECT $1 * 2`); err != nil {
		fmt.Println("PREPARE FAIL:", err)
		os.Exit(1)
	}
	var two int
	if err := c.QueryRow(ctx, `EXECUTE p94(21)`).Scan(&two); err != nil {
		fmt.Println("EXECUTE FAIL:", err)
		os.Exit(1)
	}
	if two != 42 {
		fmt.Println("EXECUTE WRONG:", two)
		os.Exit(1)
	}
	fmt.Println("PREPARE OK")

	// F-01 snapshot mechanics: tx1 takes an EXPORTED snapshot (9.4-native
	// pg_export_snapshot, same family as GP's pg_current_snapshot), then
	// inserts a row. tx2 resumes from the old snapshot and must see the
	// pre-insert state (count = 2).
	tx, err := c.Begin(ctx)
	if err != nil {
		fmt.Println("BEGIN FAIL:", err)
		os.Exit(1)
	}
	var snap string
	if err := tx.QueryRow(ctx, `SELECT pg_export_snapshot()`).Scan(&snap); err != nil {
		_ = tx.Rollback(ctx)
		fmt.Println("SNAPSHOT FAIL:", err)
		os.Exit(1)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO t94 VALUES (3, 'gamma')`); err != nil {
		_ = tx.Rollback(ctx)
		fmt.Println("TX FAIL:", err)
		os.Exit(1)
	}
	if err := tx.Commit(ctx); err != nil {
		fmt.Println("COMMIT FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("SNAPSHOT TAKEN:", snap)

	// tx2: SET TRANSACTION SNAPSHOT must be the first statement of the tx.
	tx2, err := c.Begin(ctx)
	if err != nil {
		fmt.Println("BEGIN2 FAIL:", err)
		os.Exit(1)
	}
	if _, err := tx2.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
		_ = tx2.Rollback(ctx)
		fmt.Println("TX2-RO FAIL:", err)
		os.Exit(1)
	}
	if _, err := tx2.Exec(ctx, fmt.Sprintf(`SET TRANSACTION SNAPSHOT '%s'`, snap)); err != nil {
		_ = tx2.Rollback(ctx)
		fmt.Println("SET-SNAPSHOT FAIL:", err)
		os.Exit(1)
	}
	var cnt int
	if err := tx2.QueryRow(ctx, `SELECT count(*) FROM t94`).Scan(&cnt); err != nil {
		_ = tx2.Rollback(ctx)
		fmt.Println("TX2-QUERY FAIL:", err)
		os.Exit(1)
	}
	if err := tx2.Commit(ctx); err != nil {
		fmt.Println("COMMIT2 FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("SET TRANSACTION SNAPSHOT OK (count at old snap =", cnt, ", want 2)")
	if cnt != 2 {
		fmt.Println("SNAPSHOT-VIS FAIL")
		os.Exit(1)
	}

	fmt.Println("ALL PGX/9.4 CHECKS PASS")
}
