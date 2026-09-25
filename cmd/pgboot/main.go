package main

// pgboot starts a REAL PostgreSQL 9.4.26 cluster (the Greenplum 6 kernel),
// seeds it, prints its connection URL to stdout, and keeps running until
// killed. It is used by the end-to-end smoke test to provide two real
// clusters (source + destination) for `gpstream run` to copy between.
//
// WHY 9.4.26: Greenplum 6 is built on PostgreSQL 9.4.26, so the test
// engine must match the target database's kernel, not a newer release.
import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/gpstream/gpstream/internal/pgtest"
)

func main() {
	port := os.Args[1] // port
	db := os.Args[2]   // database name
	seed := os.Args[3] // seed SQL (optional)

	cl, err := pgtest.Start(uint32(mustInt(port)), db)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer cl.Stop()

	c, err := pgx.Connect(context.Background(), cl.URL())
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer c.Close(context.Background())

	if seed != "" {
		for _, stmt := range strings.Split(seed, ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if _, err := c.Exec(context.Background(), stmt); err != nil {
				fmt.Fprintln(os.Stderr, "seed:", err)
				os.Exit(1)
			}
		}
	}

	// Print the URL and block so the binary can copy against it.
	fmt.Println(cl.URL())
	os.Stdout.Sync()
	// Block forever until killed.
	<-make(chan struct{})
}

func mustInt(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}
