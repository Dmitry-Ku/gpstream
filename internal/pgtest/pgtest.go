// Package pgtest boots real PostgreSQL 9.4.26 clusters from the local
// toolchain ( /opt/toolchain/pg9426) for live tests and e2e runs.
//
// WHY 9.4.26: Greenplum 6 is built on PostgreSQL 9.4.26. Testing gpstream
// against a newer engine (16) would validate behavior the target database
// does not exhibit. The toolchain was compiled from the official 9.4.26
// source tarball (see  pg-proxy-status.md).
//
// The embedded-postgres library only ships V13+ binaries, so we drive
// initdb/pg_ctl directly. Clusters run as the current (non-root) user;
// data dirs are ephemeral and removed on Stop.
package pgtest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// DefaultBinDir is the PG 9.4.26 toolchain location on this build host.
// Override with the GPTEST_PGBIN env var for other hosts.
const DefaultBinDir = " /opt/toolchain/pg9426/bin"

// BinDir resolves the toolchain directory.
func BinDir() string {
	if b := os.Getenv("GPTEST_PGBIN"); b != "" {
		return b
	}
	return DefaultBinDir
}

// Cluster is a running ephemeral PostgreSQL 9.4.26 instance.
type Cluster struct {
	DataDir string
	Port    uint32
	DB      string
	// Live is true when the "cluster" is a database on a real (GP6)
	// coordinator instead of a local pgtest process. Stop() then drops the
	// database (via LiveDrop) rather than stopping a local server.
	Live    bool
	LiveDSN string // full connection string to the live database
}

// Live pool: when non-empty, pgtest routes Start/Stop to real GP6
// coordinators instead of booting local clusters. The harness sets this from
// the GP6_LAB_CLUSTERS env var ("host:port,host:port,..."). Each test gets an
// isolated live database (named <db>_<port>) so sequential tests never see
// each other's tables, and source/destination always map to different
// clusters (parity + round-robin), so all four clusters (A-D) are exercised.
var livePool []liveCluster

type liveCluster struct {
	Host string
	Port int
}

// LiveCreate / LiveDrop, when set by the harness, create/drop a live database
// (identified by its full DSN). pgtest stays driver-free: it only computes the
// DSN and calls these.
var LiveCreate func(dsn string) error
var LiveDrop func(dsn string)

func liveEnabled() bool { return len(livePool) > 0 }

// LiveMode reports whether pgtest is routing to live GP6 coordinators (set via
// SetLivePool). The integration harness uses this to pick the process runner
// (deployed agents on the cluster hosts) instead of the in-process runner.
func LiveMode() bool { return liveEnabled() }

// LiveClusterFor returns the live coordinator (host, port) that a local test
// port maps to, so the harness can build the agent SSH config (source agent on
// the source host, dest agent on the dest host). ok is false when live mode is
// off.
func LiveClusterFor(port uint32) (host string, cport int, ok bool) {
	cl, o := liveClusterFor(port)
	if !o {
		return "", 0, false
	}
	return cl.Host, cl.Port, true
}

// setLivePool parses "host:port,host:port,..." (the GP6_LAB_CLUSTERS value).
func SetLivePool(spec string) {
	livePool = nil
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		host, portStr, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			continue
		}
		livePool = append(livePool, liveCluster{Host: host, Port: port})
	}
}

// liveClusterFor maps a local test port to a live cluster. ODD ports are
// sources, EVEN ports destinations (the harness convention). The cluster index
// is (port/10)%len(pool) so distinct tests spread across the pool; the
// destination is always the NEXT cluster (mod pool size) so src != dst.
func liveClusterFor(port uint32) (liveCluster, bool) {
	if !liveEnabled() {
		return liveCluster{}, false
	}
	idx := int(port/10) % len(livePool)
	if port%2 == 0 {
		idx = (idx + 1) % len(livePool)
	}
	return livePool[idx], true
}

// liveDBName gives each (port, db) its own isolated live database so sequential
// tests that reuse the same local db name never collide.
func liveDBName(db string, port uint32) string {
	return fmt.Sprintf("%s_%d", db, port)
}

// Start initializes a fresh cluster in a temp dir, starts it, and creates
// the named database. It returns once the server accepts connections
// (bounded to ~60s by pg_ctl -w). In live mode (GP6_LAB_CLUSTERS set) it
// instead creates an isolated database on the mapped live coordinator and
// returns a Live Cluster whose URL() points at it.
func Start(port uint32, db string) (*Cluster, error) {
	if liveEnabled() {
		return liveStart(port, db)
	}
	return startCluster(port, db, "", "")
}

// StartWithLocale is like Start but creates the database with an explicit
// LC_COLLATE / LC_CTYPE (both must be locales the cluster's initdb locale
// supports; on a --no-locale C cluster, "C.utf8" is available). An empty
// locale falls back to the cluster default. Used by the F-10 dry-run
// acceptance test to seed a genuine source/dest collation difference.
//
// In live mode the locale is IGNORED (a live GP6 database cannot be
// re-initialized with a different collation; the collation tests that depend
// on a genuine src/dst difference must skip in live mode).
func StartWithLocale(port uint32, db, lcCollate, lcCtype string) (*Cluster, error) {
	if liveEnabled() {
		return liveStart(port, db)
	}
	return startCluster(port, db, lcCollate, lcCtype)
}

// liveStart creates an isolated live database on the coordinator mapped to
// port and returns a Live Cluster. The harness must have set LiveCreate.
func liveStart(port uint32, db string) (*Cluster, error) {
	cl, ok := liveClusterFor(port)
	if !ok {
		return nil, fmt.Errorf("pgtest: live mode but no cluster maps to port %d", port)
	}
	liveDB := liveDBName(db, port)
	dsn := fmt.Sprintf("host=%s port=%d user=gpadmin dbname=%s sslmode=disable", cl.Host, cl.Port, liveDB)
	if LiveCreate == nil {
		return nil, fmt.Errorf("pgtest: live mode but LiveCreate is not set")
	}
	if err := LiveCreate(dsn); err != nil {
		return nil, fmt.Errorf("pgtest: live create %s@%s:%d: %w", liveDB, cl.Host, cl.Port, err)
	}
	return &Cluster{Live: true, Port: port, DB: liveDB, LiveDSN: dsn}, nil
}

func startCluster(port uint32, db, lcCollate, lcCtype string) (*Cluster, error) {
	bin := BinDir()
	initdb := filepath.Join(bin, "initdb")
	pgctl := filepath.Join(bin, "pg_ctl")
	if _, err := os.Stat(initdb); err != nil {
		return nil, fmt.Errorf("pgtest: initdb not found at %s (set GPTEST_PGBIN): %w", initdb, err)
	}
	if os.Geteuid() == 0 {
		return nil, fmt.Errorf("pgtest: refusing to run as root (initdb would fail); run as a normal user")
	}

	dataDir, err := os.MkdirTemp(os.TempDir(), "gpstream-pg94-*")
	if err != nil {
		return nil, fmt.Errorf("pgtest: temp dir: %w", err)
	}
	c := &Cluster{DataDir: dataDir, Port: port, DB: db}

	out, err := exec.Command(initdb,
		"-D", dataDir,
		"--username", "gp",
		"--auth", "trust",
		"-E", "UTF8",
		"--no-locale",
	).CombinedOutput()
	if err != nil {
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("pgtest: initdb: %w\n%s", err, out)
	}

	logPath := filepath.Join(dataDir, "startup.log")
	out, err = exec.Command(pgctl, "-D", dataDir,
		"-l", logPath,
		"-o", fmt.Sprintf("-p %d -h 127.0.0.1 -k /tmp", port),
		"start", "-w", "-t", "60",
	).CombinedOutput()
	if err != nil {
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("pgtest: pg_ctl start: %w\n%s\nlog:\n%s", err, out, readFile(logPath))
	}

	// Create the database on the initial "postgres" db (user gp, trust
	// auth) using psql from the same toolchain. This package stays
	// driver-free: callers connect with their own driver. An explicit locale
	// uses TEMPLATE template0 (so it does not require the template to be
	// idle) and carries LC_COLLATE/LC_CTYPE (FR-10/FR-16).
	psql := filepath.Join(bin, "psql")
	create := fmt.Sprintf(`CREATE DATABASE %q`, db)
	if lcCollate != "" {
		create = fmt.Sprintf(`CREATE DATABASE %q TEMPLATE template0 LC_COLLATE='%s' LC_CTYPE='%s'`,
			db, lcCollate, lcCtype)
	}
	createOut, err := exec.Command(psql,
		"-h", "127.0.0.1", "-p", fmt.Sprint(port), "-U", "gp", "-d", "postgres",
		"-c", create,
	).CombinedOutput()
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("pgtest: create db %s: %w\n%s", db, err, createOut)
	}
	return c, nil
}

// URL returns a connection string for the cluster's database.
func (c *Cluster) URL() string {
	if c.Live {
		return c.LiveDSN
	}
	return fmt.Sprintf("host=127.0.0.1 port=%d user=gp dbname=%s sslmode=disable", c.Port, c.DB)
}

// Stop shuts the cluster down and removes its data dir. A live (GP6)
// cluster is not stopped: its database is dropped through the harness's
// LiveDrop callback (nil = leave the database, the harness cleans up).
func (c *Cluster) Stop() {
	if c.Live {
		if LiveDrop != nil {
			LiveDrop(c.LiveDSN)
		}
		return
	}
	pgctl := filepath.Join(BinDir(), "pg_ctl")
	if _, err := os.Stat(pgctl); err == nil {
		exec.Command(pgctl, "-D", c.DataDir, "-m", "fast", "stop").Run()
	}
	os.RemoveAll(c.DataDir)
}

func readFile(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return "<no log>"
	}
	return string(b)
}
