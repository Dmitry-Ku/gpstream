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
}

// Start initializes a fresh cluster in a temp dir, starts it, and creates
// the named database. It returns once the server accepts connections
// (bounded to ~60s by pg_ctl -w).
func Start(port uint32, db string) (*Cluster, error) {
	return startCluster(port, db, "", "")
}

// StartWithLocale is like Start but creates the database with an explicit
// LC_COLLATE / LC_CTYPE (both must be locales the cluster's initdb locale
// supports; on a --no-locale C cluster, "C.utf8" is available). An empty
// locale falls back to the cluster default. Used by the F-10 dry-run
// acceptance test to seed a genuine source/dest collation difference.
func StartWithLocale(port uint32, db, lcCollate, lcCtype string) (*Cluster, error) {
	return startCluster(port, db, lcCollate, lcCtype)
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
	return fmt.Sprintf("host=127.0.0.1 port=%d user=gp dbname=%s sslmode=disable", c.Port, c.DB)
}

// Stop shuts the cluster down and removes its data dir.
func (c *Cluster) Stop() {
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
