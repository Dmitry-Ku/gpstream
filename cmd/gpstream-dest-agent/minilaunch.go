package main

// minilaunch.go (architecture 08.10) — the dest-agent process (on the
// destination COORDINATOR) launches the D destination-segment mini-agents
// ITSELF: it reads its OWN cluster's segment catalog (gp_segment_configuration),
// derives D + each segment's host + utility-mode DSN, and starts one
// `gpstream-dest-agent --mini` per segment (local subprocess or SSH to the
// segment host, the operator's --mini-ssh-user). The CONTROLLER does not read
// the destination catalog and does not launch the minis (it does not know D);
// it only waits for the D-entry address book through the agent's control
// protocol (the mini-map op).
//
// This lives in the main package (not internal/agent) so internal/agent does
// not import internal/launch (an in-package launch test imports internal/agent
// — a main-package launcher avoids the cycle). The mini is a mode of this same
// binary (--mini), so the launch binary is os.Executable().

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/launch"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// miniSegTarget is one destination primary segment (the mini-launch target).
type miniSegTarget struct {
	Content  int32
	Hostname string
	Port     int
	DSN      string // utility-mode DSN to this segment (the mini's --dest)
}

// readDestCatalog reads the destination cluster's segment catalog
// (gp_segment_configuration, the up primaries, content order) and returns the
// mini-launch targets. A plain-PostgreSQL destination (no catalog — the
// self-test stand / single-node layout) has ONE local segment: the DSN's
// host:port (empty host = this host), DSN verbatim (plain PG rejects
// gp_session_role).
func readDestCatalog(ctx context.Context, dsn, sslCert, sslKey, sslCA, sslMinTLS string) ([]miniSegTarget, error) {
	conn, err := pg.NewConn(ctx, dsn, sslCert, sslKey, sslCA, sslMinTLS)
	if err != nil {
		return nil, fmt.Errorf("dest catalog: connect: %w", err)
	}
	defer conn.Close()
	if pg.RowStr(ctx, conn, `SELECT to_regclass('gp_segment_configuration')::text`) == "" {
		host, port := dsnHostPort(dsn)
		if agent.IsLocalHost(host) || host == "" {
			host = "127.0.0.1"
		}
		return []miniSegTarget{{Content: 0, Hostname: host, Port: port, DSN: dsn}}, nil
	}
	rows, err := conn.Query(ctx,
		`SELECT content, hostname, port FROM gp_segment_configuration
		  WHERE role = 'p' AND status = 'u' AND content >= 0
		  ORDER BY content`)
	if err != nil {
		return nil, fmt.Errorf("dest catalog: read gp_segment_configuration: %w", err)
	}
	segs := make([]miniSegTarget, 0, len(rows))
	for i, row := range rows {
		if len(row) < 3 {
			continue
		}
		// gp_segment_configuration: content is int2 (int16) and port is int4
		// (int32) — pgx v5 scans int2 into int16 and int4 into int32, NOT
		// int64, so a raw row[n].(int64) silently zeroes every value. catInt64
		// handles the actual scan types (the same convention as the other
		// segment-catalog readers' toInt64).
		content := catInt64(row[0])
		hostname, _ := row[1].(string)
		port := catInt64(row[2])
		if hostname == "" {
			return nil, fmt.Errorf("dest catalog: segment %d has no hostname", i)
		}
		uds, err := segmentUtilityDSN(dsn, hostname, int(port))
		if err != nil {
			return nil, fmt.Errorf("dest catalog: segment %d: %w", content, err)
		}
		segs = append(segs, miniSegTarget{
			Content:  int32(content),
			Hostname: hostname,
			Port:     int(port),
			DSN:      uds,
		})
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("dest catalog: no up primary segments on the destination")
	}
	// content must be dense 0..D-1 (a missing content breaks slice coverage).
	for i, seg := range segs {
		if seg.Content != int32(i) {
			return nil, fmt.Errorf("dest catalog: contents not dense (got %d at position %d) — the mesh needs all primaries up", seg.Content, i)
		}
	}
	return segs, nil
}

// segmentUtilityDSN rewrites the coordinator DSN (URL form) to a segment's
// host:port + gp_session_role=utility (the mini connects to its OWN segment
// locally). A non-URL DSN is passed through verbatim (single-host layouts).
// segmentUtilityDSN rewrites the destination DSN so the mini connects to ITS
// OWN segment (hostname:port, utility mode) instead of the coordinator. The
// mini's segment-direct COPY must be a LOCAL connection to that segment
// (architecture 08.10: agent and mini DB connections are local only).
//
// Both DSN forms are handled. The keyword/value form (host=... port=...) is
// the one operators actually pass on the command line: rewriting only the URL
// form silently left keyword DSNs on the coordinator's host:port, so all D
// minis utility-connected to the coordinator and the whole unit's rows landed
// in the master slice while the segments stayed empty (first live A->A run).
func segmentUtilityDSN(destDSN, hostname string, port int) (string, error) {
	if u, err := url.Parse(destDSN); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		u.Host = net.JoinHostPort(hostname, strconv.Itoa(port))
		q := u.Query()
		q.Set("gp_session_role", "utility")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	// Keyword/value form: rewrite host= and port= in place, keep every other
	// field (dbname/user/sslmode/...) untouched.
	out := make([]string, 0, 12)
	hostSeen, portSeen := false, false
	for _, f := range strings.Fields(destDSN) {
		if k, _, ok := strings.Cut(f, "="); ok {
			switch k {
			case "host":
				hostSeen = true
				out = append(out, "host="+hostname)
				continue
			case "port":
				portSeen = true
				out = append(out, "port="+strconv.Itoa(port))
				continue
			}
		}
		out = append(out, f)
	}
	if !hostSeen {
		return "", fmt.Errorf("segment DSN %q has no host= field: cannot rewrite it to segment %s:%d", destDSN, hostname, port)
	}
	if !portSeen {
		out = append(out, "port="+strconv.Itoa(port))
	}
	return strings.Join(out, " ") + " gp_session_role=utility", nil
}

// catInt64 converts a catalog column value to int64. gp_segment_configuration
// columns scan as int16 (content, int2) / int32 (port, int4) / int64 / string
// depending on the driver and the column type; the raw .(int64) assertion
// silently zeroes int16/int32 values (pgx v5 scans int2->int16, int4->int32),
// which is exactly what made readDestCatalog see content=0 for every segment.
func catInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int16:
		return int64(n)
	case int:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		var out int64
		_, _ = fmt.Sscanf(n, "%d", &out)
		return out
	default:
		return 0
	}
}

// dsnHostPort extracts (host, port) from a postgres conn string (URL form or
// keyword form). Empty host (unix socket / default) yields ("", 0).
func dsnHostPort(dsn string) (string, int) {
	if i := strings.Index(dsn, "://"); i >= 0 {
		if u, err := url.Parse(dsn); err == nil && u.Host != "" {
			if h, ps, err := net.SplitHostPort(u.Host); err == nil {
				p, _ := strconv.Atoi(ps)
				return h, p
			}
			return u.Host, 0
		}
	}
	host, port := "", 0
	for _, part := range strings.Fields(dsn) {
		if h, ok := strings.CutPrefix(part, "host="); ok {
			host = h
		}
		if p, ok := strings.CutPrefix(part, "port="); ok {
			port, _ = strconv.Atoi(p)
		}
	}
	return host, port
}

// launchDestMinis launches the D destination-segment mini-agents (local
// subprocess when the segment host is this host, SSH otherwise). Each mini
// connects to its OWN segment locally (utility DSN) and dials the report
// listener (reportBound, host-rewritten for the layout). It returns the
// handles (the caller stops them on teardown as the last resort; the
// supervisor's mini-done is the clean path).
func launchDestMinis(ctx context.Context, reportBound, sshUser, portRange, frameAck, ackTimeout string, maxReconnects int, retransmitMax int64, sslCert, sslKey, sslCA, dataCert, dataKey, dataCA string, dataIdleTimeout time.Duration, segs []miniSegTarget) ([]*launch.Handle, error) {
	// The report address the minis dial: the bound address, host-rewritten
	// when it bound 0.0.0.0 (a single-host layout dials 127.0.0.1; a remote
	// layout dials the coordinator's non-loopback IP).
	reportDial := reportBound
	if h, p, perr := net.SplitHostPort(reportDial); perr == nil && (h == "0.0.0.0" || h == "::" || h == "") {
		allLocal := true
		for _, seg := range segs {
			if !agent.IsLocalHost(seg.Hostname) {
				allLocal = false
				break
			}
		}
		if allLocal {
			reportDial = net.JoinHostPort("127.0.0.1", p)
		} else if coordHost := localHostBestEffort(); coordHost != "" {
			reportDial = net.JoinHostPort(coordHost, p)
		}
	}
	bin, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("launch minis: os.Executable: %w", err)
	}
	var handles []*launch.Handle
	for _, seg := range segs {
		sshHost := ""
		if sshUser != "" && !agent.IsLocalHost(seg.Hostname) {
			sshHost = sshUser + seg.Hostname
		}
		margs := []string{
			"--dest", seg.DSN,
			"--mini",
			"--seg", strconv.Itoa(int(seg.Content)),
			"--report-addr", reportDial,
			"--data-listen", "0.0.0.0:0",
			"--accept-timeout", "5m",
			"--stream-timeout", "6h",
			"--data-idle-timeout", dataIdleTimeout.String(),
			// DESIGN §4.6: the minis must receive the slice on the same
			// transport the segreceivers send it on (frame-ack by default).
			// Mismatch = the mini reads raw frames from a "GPSA" control
			// stream (first live A->A mesh run, bad magic 0x47505341).
			"--frame-ack", frameAck,
			"--ack-timeout", ackTimeout,
			"--max-reconnects", strconv.Itoa(maxReconnects),
			"--retransmit-buffer-max", strconv.FormatInt(retransmitMax, 10),
		}
		if portRange != "" {
			margs = append(margs, "--port-range", portRange)
		}
		if sslCert != "" {
			margs = append(margs, "--ssl-cert", sslCert, "--ssl-key", sslKey)
		}
		if sslCA != "" {
			margs = append(margs, "--ssl-ca", sslCA)
		}
		if dataCert != "" {
			margs = append(margs, "--data-tls-cert", dataCert, "--data-tls-key", dataKey)
		}
		if dataCA != "" {
			margs = append(margs, "--data-tls-ca", dataCA)
		}
		h, lerr := launch.Ensure(ctx, launch.Target{
			Binary:      bin,
			Args:        margs,
			ControlAddr: "", // the mini has no control socket: it dials the report listener
			SSHHost:     sshHost,
			LogPath:     miniLogPath(int(seg.Content)),
			WaitTimeout: 30 * time.Second,
		})
		if lerr != nil {
			for _, hh := range handles {
				hh.Stop()
			}
			return nil, fmt.Errorf("seg %d (%s): %w", seg.Content, seg.Hostname, lerr)
		}
		handles = append(handles, h)
	}
	return handles, nil
}

// miniLogPath is the per-seg mini-agent log path (the mini has no control
// socket, so the controller cannot tail it per-handle; the file is for the
// operator).
func miniLogPath(seg int) string {
	dir, err := os.MkdirTemp("", "gpstream-mini-logs-")
	if err != nil {
		return ""
	}
	return dir + "/gpstream-dest-agent-mini-seg" + strconv.Itoa(seg) + ".log"
}

// localHostBestEffort returns this host's non-loopback IPv4 (or "").
func localHostBestEffort() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && ipn.IP.To4() != nil {
			return ipn.IP.String()
		}
	}
	return ""
}

// keep the exec import referenced (the launch package owns the real spawn;
// this is a guard against an accidental future removal of the exec import if
// a local-spawn fallback is ever added in this file).
var _ = exec.Command
