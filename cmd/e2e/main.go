package main

// e2e is an end-to-end smoke test that starts two REAL PostgreSQL clusters,
// seeds the source, runs the actual `bin/gpstream run` binary between them,
// and verifies the data arrived in the destination. It proves the binary
// moves real bytes (not a fake / "no bytes transferred").
import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
)

func main() {
	// --- source cluster (seeded) ---
	src, srcURL, err := startCluster("srcdb", 5601)
	if src == nil {
		os.Exit(1)
	}
	if err != nil {
		os.Exit(1)
	}
	defer src.Stop()
	if _, err := seedCluster(srcURL, `CREATE TABLE public.orders (id integer, customer text, amount numeric);
INSERT INTO public.orders VALUES (1,'alice',10.50),(2,'bob',20.25),(3,'carol',30.00),(4,'dave',40.75),(5,'erin',50.00);`); err != nil {
		fmt.Println("seed src:", err)
		os.Exit(1)
	}

	// --- destination cluster (empty) ---
	dst, dstURL, err := startCluster("dstdb", 5602)
	if dst == nil {
		os.Exit(1)
	}
	if err != nil {
		os.Exit(1)
	}
	defer dst.Stop()
	if _, err := seedCluster(dstURL, ""); err != nil {
		fmt.Println("seed dst:", err)
		os.Exit(1)
	}

	fmt.Printf("SRC = %s\n", srcURL)
	fmt.Printf("DST = %s\n", dstURL)

	// --- run the actual binary ---
	fmt.Println("\n### running: ./bin/gpstream run --source <src> --dest <dst> --tables public.orders --auto-resume 3 --compression zstd")
	fmt.Println("------------------------------------------------------------------------")
	stateDir, err := os.MkdirTemp("", "gpstream-e2e-state-")
	if err != nil {
		fmt.Println("state dir:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(stateDir)

	out := exec.Command("./bin/gpstream", "run",
		"--source", srcURL, "--dest", dstURL,
		"--tables", "public.orders", "--auto-resume", "3", "--compression", "zstd",
		"--state-dir", stateDir)
	out.Dir = "."
	out.Stdout = os.Stdout
	out.Stderr = os.Stderr
	if err := out.Run(); err != nil {
		fmt.Println("gpstream run exit:", err)
	}
	fmt.Println("------------------------------------------------------------------------")

	// --- verify the destination ---
	time.Sleep(500 * time.Millisecond)
	c, err := pgx.Connect(context.Background(), dstURL)
	if err != nil {
		fmt.Println("verify connect:", err)
		os.Exit(1)
	}
	defer c.Close(context.Background())

	var n int
	r, _ := c.Query(context.Background(), `SELECT count(*) FROM public.orders`)
	for r.Next() {
		r.Scan(&n)
	}
	fmt.Printf("DEST rows = %d (want 5)\n", n)

	var cust string
	r2, _ := c.Query(context.Background(), `SELECT customer FROM public.orders WHERE id=3`)
	for r2.Next() {
		r2.Scan(&cust)
	}
	fmt.Printf("DEST id=3 customer = %q (want carol)\n", cust)

	if n == 5 && cust == "carol" {
		fmt.Println("\nE2E PASS: real bytes copied between two live PostgreSQL clusters by the binary")
	} else {
		fmt.Println("\nE2E FAIL")
		os.Exit(1)
	}
}

func startCluster(db string, port uint32) (*embeddedpostgres.EmbeddedPostgres, string, error) {
	dataDir := "/tmp/pgdata_" + db
	_ = os.RemoveAll(dataDir)
	cfg := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Username("gp").
		Password("secret").
		Database(db).
		Port(port).
		DataPath(dataDir).
		StartTimeout(60000000000).
		Logger(os.Stderr)
	srv := embeddedpostgres.NewDatabase(cfg)
	if err := srv.Start(); err != nil {
		return nil, "", fmt.Errorf("start %s: %w", db, err)
	}
	url := cfg.GetConnectionURL() + "?sslmode=disable" // local test cluster: no TLS (FR-13 default verify-full would not apply)
	// Wait until it accepts connections.
	for i := 0; i < 40; i++ {
		c, err := pgx.Connect(context.Background(), url)
		if err == nil {
			c.Close(context.Background())
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return srv, url, nil
}

func seedCluster(url, seed string) (string, error) {
	c, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return "", err
	}
	defer c.Close(context.Background())
	if seed != "" {
		for _, stmt := range strings.Split(seed, ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if _, err := c.Exec(context.Background(), stmt); err != nil {
				return "", err
			}
		}
	}
	return url, nil
}

// ensure io is referenced (kept for future extensions).
var _ = io.Discard
var _ = bufio.NewScanner
