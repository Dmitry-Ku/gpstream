// Command distprobe prints the source Agent.Distribution result for a table,
// against a live connection, to diagnose why the F-11 clause is not applied.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: distprobe <connstr> <schema> <table>")
		os.Exit(2)
	}
	connStr, schema, table := os.Args[1], os.Args[2], os.Args[3]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Mirror the manager: source is a pg.Pool (cmdRun uses pg.NewPool).
	pool, err := pg.NewPool(ctx, connStr, "", "", "", "", 8)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()
	a := source.New(pool)

	// Raw probe 1: COALESCE to_regclass.
	oid, err := pg.Row(ctx, pool,
		`SELECT COALESCE(to_regclass('gp_toolkit.gp_distribution_policy'), to_regclass('gp_distribution_policy'))`)
	fmt.Printf("probe1 COALESCE to_regclass = %v err=%v\n", oid, err)

	// Raw probe 2: the exact policytype query.
	rows, err := pool.Query(ctx,
		`SELECT dp.policytype,
		        (SELECT string_agg(a.attname, ',' ORDER BY k.pos)
		         FROM unnest(dp.distkey::int2[]) WITH ORDINALITY AS k(attnum, pos)
		         JOIN pg_attribute a ON a.attrelid = dp.localoid AND a.attnum = k.attnum)
		 FROM gp_distribution_policy dp
		 JOIN pg_class c ON c.oid = dp.localoid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relname = $1 AND n.nspname = $2`,
		table, schema)
	fmt.Printf("probe2 rows=%#v err=%v\n", rows, err)

	d := a.Distribution(ctx, schema, table)
	fmt.Printf("Distribution = %+v\n", d)
	fmt.Printf("SQLClause = %q\n", d.SQLClause())
}
