package main

// pgboot starts an embedded (real) PostgreSQL cluster, seeds it, prints its
// connection URL to stdout, and keeps running until killed. It is used by the
// end-to-end smoke test to provide two real clusters (source + destination)
// for `gpstream run` to copy between — proving the binary moves real bytes.
import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
)

func main() {
	port := os.Args[1] // port
	db := os.Args[2]   // database name
	seed := os.Args[3] // seed SQL (optional)
	dataDir := "/tmp/pgdata_" + db
	os.RemoveAll(dataDir)

	cfg := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Username("gp").
		Password("secret").
		Database(db).
		Port(uint32(mustInt(port))).
		DataPath(dataDir).
		StartTimeout(60000000000). // 60s
		Logger(os.Stderr)          // keep stdout clean so the URL can be grepped

	srv := embeddedpostgres.NewDatabase(cfg)
	if err := srv.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer srv.Stop()

	c, err := pgx.Connect(context.Background(), cfg.GetConnectionURL())
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
	fmt.Println(cfg.GetConnectionURL())
	os.Stdout.Sync()
	// Block forever until killed.
	keepAlive()
}

func keepAlive() {
	// Block until the process is killed.
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
