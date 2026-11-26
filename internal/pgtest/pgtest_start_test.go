package pgtest

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func havePGBinaries(t *testing.T) string {
	t.Helper()
	bin := BinDir()
	if _, err := os.Stat(filepath.Join(bin, "initdb")); err != nil {
		t.Skipf("PG toolchain not found at %s (set GPTEST_PGBIN)", bin)
	}
	return bin
}

// The 26.11 t7 incident: pg_ctl start reported failure while a postmaster
// (or its port) was left behind; every later start on the port failed
// EADDRINUSE, and the real bind error was hidden ("log: <no log>" - the
// log file was read AFTER the data dir was removed). Both fixed: the log is
// captured before cleanup, and a stray postmaster is killed.
func TestStartClusterOccupiedPortCapturesLog(t *testing.T) {
	bin := havePGBinaries(t)
	const port = 5971
	occ, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	_, err = startCluster(uint32(port), "srcdb", "", "")
	occ.Close()
	if err == nil {
		t.Fatal("expected a pg_ctl start failure (port occupied)")
	}
	if !strings.Contains(err.Error(), "Address already in use") {
		t.Fatalf("error must carry the captured startup log (bind FATAL), got:\n%s", err.Error())
	}
	// The port must be usable again immediately: no stray postmaster holds it.
	l, lerr := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if lerr != nil {
		t.Fatalf("port still held after the failed start (stray postmaster?): %v", lerr)
	}
	l.Close()
	_ = bin
}

// stopStrayPostmaster kills a live postmaster found in dataDir (TERM, then
// KILL after 10s), and is a silent no-op when nothing is running.
func TestStopStrayPostmaster(t *testing.T) {
	bin := havePGBinaries(t)
	dataDir, err := os.MkdirTemp("", "gpstream-pg94-kill-*")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dataDir)
	if out, err := exec.Command(filepath.Join(bin, "initdb"),
		"-D", dataDir, "--username", "gp", "--auth", "trust", "-E", "UTF8", "--no-locale").CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v\n%s", err, out)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	logPath := filepath.Join(dataDir, "startup.log")
	out, err := exec.Command(filepath.Join(bin, "pg_ctl"), "-D", dataDir, "-l", logPath,
		"-o", fmt.Sprintf("-p %d -h 127.0.0.1 -k %s", port, t.TempDir()),
		"start", "-w", "-t", "30").CombinedOutput()
	if err != nil {
		t.Fatalf("pg_ctl start: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(filepath.Join(dataDir, "postmaster.pid"))
	if err != nil {
		t.Fatalf("postmaster.pid: %v", err)
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(strings.Split(string(raw), "\n")[0]))
	if perr != nil || pid <= 1 {
		t.Fatalf("parse postmaster.pid: %v (%q)", perr, raw)
	}
	if syscall.Kill(pid, 0) != nil {
		t.Fatalf("postmaster %d not running after pg_ctl start", pid)
	}

	stopStrayPostmaster(dataDir)
	if syscall.Kill(pid, 0) == nil {
		t.Fatalf("postmaster %d still running after stopStrayPostmaster", pid)
	}
	// No-ops: missing dir, missing pidfile, already-dead pid.
	stopStrayPostmaster(filepath.Join(dataDir, "gone"))
	stopStrayPostmaster(dataDir)
	stopStrayPostmaster(dataDir)
}
