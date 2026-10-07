package agent

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// IsLocalHost reports whether a DSN/SSH host refers to THIS machine
// (127.0.0.1, ::1, localhost, this host's name, or one of its interface
// IPs). Used both by the agents (master-check: the DSN host must be local)
// and by the controller (transport selection: local host = subprocess,
// remote host = SSH).
func IsLocalHost(host string) bool {
	if host == "127.0.0.1" || host == "::1" || host == "localhost" {
		return true
	}
	hostname, _ := os.Hostname()
	if hostname != "" && host == hostname {
		return true
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && ipn.IP.String() == host {
			return true
		}
	}
	return false
}

// dsnHost extracts the connection host from a postgres conn string (keyword
// form "host=X ..." or URL form "postgres://user@host:port/db"). Empty host
// (unix socket / default) yields "".
func dsnHost(dsn string) string {
	if i := strings.Index(dsn, "://"); i >= 0 {
		if u, err := url.Parse(dsn); err == nil && u.Host != "" {
			if h, _, err := net.SplitHostPort(u.Host); err == nil {
				return h
			}
			return u.Host
		}
	}
	for _, part := range strings.Fields(dsn) {
		if h, ok := strings.CutPrefix(part, "host="); ok {
			return h
		}
	}
	return ""
}

// normHost lower-cases and strips a trailing dot from a hostname so that
// "gp-a" and "gp-a." compare equal.
func normHost(s string) string {
	return strings.ToLower(strings.TrimRight(strings.TrimSpace(s), "."))
}

// mustHostname returns this machine's hostname (best effort, "" on error).
func mustHostname() string {
	h, _ := os.Hostname()
	return h
}

// isMyHost reports whether the given name (a hostname or an IP) refers to
// THIS machine. It handles the common FQDN-vs-short-name mismatch (one is a
// dot-suffix of the other) and also accepts a name that is one of this host's
// interface IPs. Used to confirm the DSN's cluster coordinator is on this
// agent's host (P1e, requirement 2 — 30986/30989).
func isMyHost(name string) bool {
	name = normHost(name)
	if name == "" {
		return false
	}
	hostname := normHost(mustHostname())
	if hostname != "" {
		if name == hostname {
			return true
		}
		if strings.HasSuffix(hostname, "."+name) || strings.HasSuffix(name, "."+hostname) {
			return true
		}
	}
	// Coordinator recorded as an IP: accept it if it is one of our addresses.
	if net.ParseIP(name) != nil {
		return IsLocalHost(name)
	}
	return false
}

// CheckOnCoordinator verifies (P1e, requirement 2 — 30986/30989) that the
// agent is running on the coordinator (master) of EXACTLY the cluster the DSN
// points to. The agent must open its pg connection LOCALLY, on the coordinator
// host — never from another host — and the coordinator must be the DSN
// cluster's own coordinator, not some other cluster's. It runs before the
// agent binds its control listener, so a failing agent never accepts the
// controller's control dial; the controller's bounded dial then fails and the
// run/bench aborts with the verdict from the agent log.
//
// Steps:
//  1. DSN host must be local (127.0.0.1 / ::1 / localhost / this host's name
//     or IP). A remote host means the agent was started on the wrong host —
//     the exact 07.10 incident topology — hard error.
//  2. Connect to the local pg. No connection: "master/coordinator on my host
//     (<hostname>) is not responding".
//  3. Greenplum: the coordinator of the DSN's own cluster must be on this
//     host. We read the coordinator row (content = -1) from the DSN cluster's
//     catalog and require its hostname/address to be this agent's host. This
//     is the "exactly that cluster" guard: if the DSN points to cluster X
//     while the agent runs on cluster Y's coordinator, X's catalog names X's
//     coordinator (a different host) → hard error.
//  4. Greenplum: the node we actually connected to must be the coordinator
//     (the gp_segment_configuration row for THIS backend's port has
//     content = -1). A segment (content >= 0) is a hard error.
//  5. Not Greenplum (no gp_segment_configuration, e.g. a single-node test
//     postgres): the connected node is the coordinator by definition — pass.
func CheckOnCoordinator(ctx context.Context, dsn, sslCert, sslKey, sslCA, minTLS string) error {
	host := dsnHost(dsn)
	if host != "" && !IsLocalHost(host) {
		return fmt.Errorf("DSN host %q is remote — the agent must run on the cluster master (coordinator) and connect to pg locally (this host: %s)",
			host, mustHostname())
	}
	hostname := mustHostname()
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pg.NewConn(dctx, dsn, sslCert, sslKey, sslCA, minTLS)
	if err != nil {
		return fmt.Errorf("master/coordinator on my host (%s) is not responding: %v", hostname, err)
	}
	defer conn.Close()
	// Greenplum? (to_regclass: "" when the catalog table is absent.)
	if pg.RowStr(dctx, conn, "SELECT to_regclass('gp_segment_configuration')::text") == "" {
		return nil // not a Greenplum cluster: single-node mode, this node is the coordinator
	}
	// 3. The DSN's cluster's own coordinator must be on THIS host (30986/30989).
	// Read the coordinator row (content = -1) from the DSN cluster's catalog
	// and require its hostname/address to be this agent's host.
	coordHost := pg.RowStr(dctx, conn,
		"SELECT hostname FROM gp_segment_configuration WHERE content = -1 LIMIT 1")
	coordAddr := pg.RowStr(dctx, conn,
		"SELECT address FROM gp_segment_configuration WHERE content = -1 LIMIT 1")
	if coordHost == "" && coordAddr == "" {
		return fmt.Errorf("coordinator check: gp_segment_configuration has no coordinator (content=-1) row — cannot confirm the DSN's cluster")
	}
	if !isMyHost(coordHost) && !isMyHost(coordAddr) {
		return fmt.Errorf("DSN points to a cluster whose coordinator is at %s (%s), but this agent runs on %s — the agent must run on the coordinator of the DSN's cluster",
			orDash(coordHost), orDash(coordAddr), hostname)
	}
	// 4. The node we connected to must itself be the coordinator: the
	// gp_segment_configuration row for THIS backend's port has content = -1
	// on the master and content >= 0 on a segment. min(content) is -1 iff we
	// are the coordinator (the master port appears only in the content=-1 row).
	minContent := pg.RowStr(dctx, conn,
		"SELECT min(content) FROM gp_segment_configuration WHERE port = current_setting('port')::int")
	switch minContent {
	case "":
		return fmt.Errorf("master/coordinator check: gp_segment_configuration has no row for this backend's port — cannot confirm the agent is on the coordinator (host %s)", hostname)
	case "-1":
		return nil
	default:
		return fmt.Errorf("not the cluster master (connected as content=%s) — the agent must run on the coordinator host (this host: %s)", minContent, hostname)
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
