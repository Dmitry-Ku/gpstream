//go:build gp6lab

package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestMain routes the suite to the LIVE Greenplum clusters (A-D) when
// GP6_LAB_CLUSTERS is set ("host:port,host:port,..."): pgtest.Start/Stop then
// create/drop isolated databases on the live coordinators instead of booting
// local PG 9.4.26 clusters, and the two-agent runner becomes the PROCESS
// runner — the DEPLOYED gpstream package on the cluster hosts, launched over
// SSH — instead of the in-process runner. The controller (this test binary)
// runs on the lab controller host, which reaches the coordinators directly
// (control plane: plan, DDL, journal, verify, swap) and can SSH the agent
// hosts (data plane: the agents + kernel segreceivers). Without the env var
// the suite runs the standard local pgtest gp6lab.
func TestMain(m *testing.M) {
	if clusters := os.Getenv("GP6_LAB_CLUSTERS"); clusters != "" {
		pgtest.SetLivePool(clusters)
		pgtest.LiveCreate = liveDBCreate
		pgtest.LiveDrop = liveDBDrop
		// The agents are the DEPLOYED package on the cluster hosts.
		os.Setenv("GPSTREAM_AGENT_BIN_DIR", "/usr/bin")
	}
	os.Exit(m.Run())
}

// setTwoAgentRunner picks the two-agent data-path runner: the PROCESS runner
// (the deployed agents on the cluster hosts, launched over SSH — the real
// production / "deployed package" path) in live mode, and the in-process
// runner otherwise. In live mode it also points cfg at the source/destination
// cluster hosts (parsed from the DSNs) for the SSH launch + control addrs.
func setTwoAgentRunner(t *testing.T, m *manifest.Manager, cfg *config.Config, srcDSN, dstDSN string) {
	t.Helper()
	if pgtest.LiveMode() {
		applyLiveAgentConfig(cfg, srcDSN, dstDSN)
		m.SetTwoAgentRunner(manifest.NewProcessTwoAgentRunner(cfg))
		return
	}
	m.SetTwoAgentRunner(manifest.NewInProcTwoAgentRunner(cfg))
}

// liveControlPorts returns a unique (source, dest) control-port pair for one
// operation, drawn from a shared atomic counter so concurrent operations
// (TestF02TwoDBConcurrentSnapshot runs two managers at once) never bind the
// same control port on a shared cluster host. The 29000-29999 range is free
// on the lab; the dest DATA port is auto (":0", self-assigned + reported in
// the hello), so only the two control ports need to be reserved here.
var liveControlPortBase atomic.Int64

func liveControlPorts() (srcPort, dstPort int) {
	base := 29000 + int(liveControlPortBase.Add(2))
	return base + 1, base + 2
}

// applyLiveAgentConfig points the process runner at the DEPLOYED agents on the
// source/destination cluster hosts (the DSN host is the agent host): SSH
// launch as gpadmin, fixed routable control addrs (the controller dials them),
// /usr/bin install dir. The dest data listener is left auto (":0"); the dest
// agent's hello reports its routable data address, which the source agent and
// the kernel segreceivers dial.
func applyLiveAgentConfig(cfg *config.Config, srcDSN, dstDSN string) {
	srcHost := dsnHost(srcDSN)
	dstHost := dsnHost(dstDSN)
	user := os.Getenv("GP6_LAB_SSH_USER")
	if user == "" {
		user = "gp"
	}
	cfg.SourceAgentSSH = user + "@" + srcHost
	cfg.DestAgentSSH = user + "@" + dstHost
	// Unique per-operation control ports (concurrent ops on a shared host
	// must not collide); the src/dst agents are on different clusters.
	srcPort, dstPort := liveControlPorts()
	cfg.SourceControlAddr = net.JoinHostPort(srcHost, strconv.Itoa(srcPort))
	cfg.DestControlAddr = net.JoinHostPort(dstHost, strconv.Itoa(dstPort))
	if runas := os.Getenv("GP6_LAB_SSH_RUNAS"); runas != "" {
		cfg.AgentSSHRunAs = runas
	} else {
		cfg.AgentSSHRunAs = "gpadmin"
	}
	if key := os.Getenv("GP6_LAB_SSH_KEY"); key != "" {
		cfg.AgentSSHKey = key
	}
	cfg.AgentBinDir = "/usr/bin"
}

// dsnHost extracts "host=<h>" from a keyword connection string.
func dsnHost(dsn string) string {
	for _, part := range strings.Fields(dsn) {
		if h, ok := strings.CutPrefix(part, "host="); ok {
			return h
		}
	}
	return ""
}

// dsnReplaceUser returns the DSN with its "user=" value replaced (local pgtest
// DSNs use user=gp, live DSNs use user=gpadmin). Used by the low-privilege-role
// subtest (t7) to aim the dest agent at a non-superuser role.
func dsnReplaceUser(dsn, user string) string {
	parts := strings.Fields(dsn)
	for i, p := range parts {
		if strings.HasPrefix(p, "user=") {
			parts[i] = "user=" + user
		}
	}
	return strings.Join(parts, " ")
}

func dsnHostPort(dsn string) (string, int) {
	host, port := "", 0
	for _, part := range strings.Fields(dsn) {
		if h, ok := strings.CutPrefix(part, "host="); ok {
			host = h
		}
		if p, ok := strings.CutPrefix(part, "port="); ok {
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}
	return host, port
}

func dsnDBName(dsn string) string {
	for _, part := range strings.Fields(dsn) {
		if d, ok := strings.CutPrefix(part, "dbname="); ok {
			return d
		}
	}
	return ""
}

// liveCLIEnv returns the GPSTREAM_* env vars a `gpstream` CLI subprocess
// (f02, crash-matrix) needs to drive the DEPLOYED agents over SSH in live
// mode: the source/dest agent hosts (parsed from the DSNs), the runas user,
// the SSH key, the install dir, and the fixed control addrs. In local mode it
// returns nil (the convention-resolved local agents, no SSH).
func liveCLIEnv(t *testing.T, srcDSN, dstDSN string) []string {
	t.Helper()
	if !pgtest.LiveMode() {
		return nil
	}
	env := []string{
		"GPSTREAM_AGENT_BIN_DIR=/usr/bin",
	}
	applyLiveAgentConfigToEnv(&env, srcDSN, dstDSN)
	return env
}

// applyLiveAgentConfigToEnv appends the SSH-launch GPSTREAM_* vars to env
// (the same values applyLiveAgentConfig sets on a *config.Config), including
// a unique per-operation control-port pair from the shared counter.
func applyLiveAgentConfigToEnv(env *[]string, srcDSN, dstDSN string) {
	srcHost := dsnHost(srcDSN)
	dstHost := dsnHost(dstDSN)
	user := os.Getenv("GP6_LAB_SSH_USER")
	if user == "" {
		user = "gp"
	}
	srcPort, dstPort := liveControlPorts()
	*env = append(*env,
		"GPSTREAM_SOURCE_AGENT_SSH="+user+"@"+srcHost,
		"GPSTREAM_DEST_AGENT_SSH="+user+"@"+dstHost,
		"GPSTREAM_SOURCE_CONTROL_ADDR="+net.JoinHostPort(srcHost, strconv.Itoa(srcPort)),
		"GPSTREAM_DEST_CONTROL_ADDR="+net.JoinHostPort(dstHost, strconv.Itoa(dstPort)),
	)
	if runas := os.Getenv("GP6_LAB_SSH_RUNAS"); runas != "" {
		*env = append(*env, "GPSTREAM_AGENT_SSH_RUNAS="+runas)
	}
	if key := os.Getenv("GP6_LAB_SSH_KEY"); key != "" {
		*env = append(*env, "GPSTREAM_AGENT_SSH_KEY="+key)
	}
}

// liveDeadline scales a local-mode wait deadline for live mode, where SSH
// agent launch + a GP6 coordinator connect (10-20s each, two agents) dominate
// before the data path even starts. It extends startup-sensitive budgets
// (snapshot-live, run-must-return) without hiding a real deadlock: the run
// still has to return, just after the slower remote agent comes up.
func liveDeadline(d time.Duration) time.Duration {
	if pgtest.LiveMode() {
		return d + 150*time.Second
	}
	return d
}

// liveKillAgents best-effort kills any running gpstream agents — on the given
// cluster hosts over SSH in live mode (the crash matrix / f02 re-run after a
// killed controller must not fight an orphaned agent for the fixed control /
// data ports), or locally otherwise.
func liveKillAgents(t *testing.T, srcDSN, dstDSN string) {
	t.Helper()
	pat := "gpstream-(source|dest)-agent"
	if pgtest.LiveMode() {
		hosts := map[string]bool{dsnHost(srcDSN): true, dsnHost(dstDSN): true}
		user := os.Getenv("GP6_LAB_SSH_USER")
		if user == "" {
			user = "gp"
		}
		for h := range hosts {
			key := os.Getenv("GP6_LAB_SSH_KEY")
			args := []string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no"}
			if key != "" {
				args = append(args, "-i", key)
			}
			// Quote the ERE for the REMOTE shell: unquoted, bash chokes on
			// the parentheses (syntax error) and pkill never runs, so
			// "killed" agents kept running and held their PG sessions /
			// ports (55006 on the next test's DROP, refused control ports).
			args = append(args, user+"@"+h, "sudo -n pkill -f '"+pat+"'")
			_, _ = exec.Command("ssh", args...).CombinedOutput()
		}
		return
	}
	_, _ = exec.Command("pkill", "-f", pat).CombinedOutput()
}

// liveControllerBinary returns the gpstream controller binary a CLI-subprocess
// test should exec: the DEPLOYED /usr/bin/gpstream in live mode (the package
// under test), the convention-resolved local build otherwise.
func liveControllerBinary(t *testing.T, localBin string) string {
	t.Helper()
	if pgtest.LiveMode() {
		if _, err := os.Stat("/usr/bin/gpstream"); err == nil {
			return "/usr/bin/gpstream"
		}
		t.Fatalf("live mode: /usr/bin/gpstream (deployed controller) not found")
	}
	return localBin
}

// dialRetry dials addr until it succeeds or the deadline passes, retrying on
// connection-refused. The fake segreceivers in the two-agent segment tests use
// it instead of a single-shot dial: the in-process destination agent connects
// to the destination coordinator BEFORE binding its listener, and a live GP6
// coordinator connect takes 10-20s (a local pgtest PG is instant). Production
// segreceivers retry their dial while the destination binds; the fakes mirror
// that behavior so the tests pass on both local and live clusters.
func dialRetry(addr string, d time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(d)
	var lastErr error
	for {
		var conn net.Conn
		conn, lastErr = net.DialTimeout("tcp", addr, 3*time.Second)
		if lastErr == nil {
			return conn, nil
		}
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// liveDBCreate creates the isolated live database (idempotent) on the
// coordinator the DSN points at, by connecting to the "postgres" maintenance
// database. pgtest stays driver-free; this is the harness's callback.
func liveDBCreate(dsn string) error {
	host, port := dsnHostPort(dsn)
	pgDSN := fmt.Sprintf("host=%s port=%d user=gpadmin dbname=postgres sslmode=disable", host, port)
	c, err := pg.NewConn(context.Background(), pgDSN, "", "", "", "")
	if err != nil {
		return fmt.Errorf("live create: connect postgres@%s:%d: %w", host, port, err)
	}
	defer c.Close()
	db := dsnDBName(dsn)
	// DROP first: the same (port) maps to the same database name, and test
	// port pairs collide across the suite (e.g. 5681/5721/5731 are reused).
	// A hard-killed prior test (kill -9 skips t.Cleanup) leaves its tables
	// behind, so "CREATE ... already exists" must start from an EMPTY db,
	// not the previous test's leftovers. A leftover session (a killed
	// controller's pool, an orphaned agent's pg connection) holds the DB and
	// makes the DROP fail with 55006; terminate those backends first.
	if _, err := c.Exec(context.Background(),
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		 WHERE datname = '`+strings.ReplaceAll(db, "'", "''")+`' AND pid <> pg_backend_pid()`); err != nil {
		return fmt.Errorf("live terminate %s: %w", db, err)
	}
	time.Sleep(300 * time.Millisecond) // let the terminated sessions release the lock
	if _, err := c.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, db)); err != nil {
		return fmt.Errorf("live drop %s: %w", db, err)
	}
	if _, err := c.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %s`, db)); err != nil {
		return fmt.Errorf("live create %s: %w", db, err)
	}
	return nil
}

// liveDBDrop drops the isolated live database (best-effort; the next run
// re-creates it). Connects to the "postgres" maintenance db.
func liveDBDrop(dsn string) {
	host, port := dsnHostPort(dsn)
	pgDSN := fmt.Sprintf("host=%s port=%d user=gpadmin dbname=postgres sslmode=disable", host, port)
	c, err := pg.NewConn(context.Background(), pgDSN, "", "", "", "")
	if err != nil {
		return
	}
	defer c.Close()
	db := dsnDBName(dsn)
	_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, db))
}
