// Package agent — 2C-3b (architecture 08.10): the AGENT-SIDE on_segment /
// mini-readiness dry-run checks (A+). The dry-run must NOT run a real ON
// SEGMENT kernel dispatch (no `COPY ... TO PROGRAM ... ON SEGMENT`, no
// launching the segriver / mini) — "ядро НЕ проверяем". Instead the side's
// AGENT (running as gpadmin on its cluster's coordinator) reads its own
// segment catalog and SSHes to each segment host to verify the binary
// (presence + executability + version) and the listen-port config. gpadmin
// has passwordless SSH to its own segments (the standard Greenplum layout).
//
// The kernel's ON SEGMENT support itself is NOT probed (the first real run
// verifies it) and is reported as a note, per the design decision.
package agent

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/version"
)

// quoteRemote single-quotes a value for the remote shell (the same
// convention as launch.shellQuoteAll; the agent package cannot import
// launch — an in-package launch test imports the agent, so the cycle is
// avoided by a local copy).
func quoteRemote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// probeScript is the per-host readiness probe: the binary's presence +
// executability, its reported version, and (optionally) whether the given
// local port is already taken (a bash /dev/tcp connect probe). The remote
// shell prints one line per probe:
//
//	BIN_OK | BIN_MISSING
//	<the binary's --version line, empty when the binary is absent>
//	PORT_FREE | PORT_BUSY | PORT_NA  (only when checkPort; PORT_NA = no bash)
//
// The script ALWAYS exits 0 (the trailing `true`): the status is carried in
// the printed lines, never in the exit code — RunRemoteOutput treats a
// non-zero exit as a transport failure, so a BUSY port (a probe finding,
// not a fault) must not look like one.
func probeScript(bin string, port int, checkPort bool) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "test -x %s && echo BIN_OK || echo BIN_MISSING\n", quoteRemote(bin))
	fmt.Fprintf(&sb, "%s --version 2>/dev/null | head -n1\n", quoteRemote(bin))
	if checkPort {
		// bash /dev/tcp connect probe; PORT_NA when the host has no bash
		// (the port is then simply not probed, not "free").
		fmt.Fprintf(&sb, "command -v bash >/dev/null 2>&1 && { (exec 3<>/dev/tcp/127.0.0.1/%d) 2>/dev/null && echo PORT_BUSY || echo PORT_FREE; } || echo PORT_NA\n", port)
	}
	sb.WriteString("true\n")
	return sb.String()
}

// parseProbe parses the probeScript output (see its doc) into the binary
// status, the remote version (the "<name> <ver (commit)>" line with the
// binary name token dropped — the same convention as gatherSourceVersion),
// and the port state ("", "FREE", or "BUSY"; "" = not probed, or the probe
// was unavailable on the host — no bash / PORT_NA).
func parseProbe(out string) (binOK bool, remoteVer, portState string) {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) > 0 {
		binOK = strings.TrimSpace(lines[0]) == "BIN_OK"
	}
	if len(lines) > 1 {
		v := strings.TrimSpace(lines[1])
		if fields := strings.Fields(v); len(fields) >= 2 {
			remoteVer = strings.Join(fields[1:], " ")
		} else {
			remoteVer = v
		}
	}
	if len(lines) > 2 {
		switch p := strings.TrimSpace(lines[2]); p {
		case "PORT_BUSY":
			portState = "BUSY"
		case "PORT_FREE":
			portState = "FREE"
		}
		// PORT_NA (no bash on the host) and anything else = not probed ("").
	}
	return
}

// hostProbe runs the readiness probe on ONE segment host (local when
// sshHost is empty — a single-host layout where the host IS this host) and
// returns the parsed result. The SSH error is non-nil when the host is
// unreachable (a broken key / network is itself the dry-run finding).
func hostProbe(ctx context.Context, sshHost, host, bin string, port int, checkPort bool) (binOK bool, remoteVer, portState string, err error) {
	out, perr := RunRemoteOutput(ctx, sshHost, probeScript(bin, port, checkPort))
	if perr != nil {
		return false, "", "", fmt.Errorf("probe %s: %w", host, perr)
	}
	b, v, p := parseProbe(out)
	return b, v, p, nil
}

// segListenPort extracts the base port from a --seg-listen "host:port"
// value (0, false when unset or malformed).
func segListenPort(segListen string) (int, bool) {
	if segListen == "" {
		return 0, false
	}
	_, p, err := net.SplitHostPort(segListen)
	if err != nil {
		return 0, false
	}
	n, err := strconv.Atoi(p)
	if err != nil || n < 1 || n > 65535 {
		return 0, false
	}
	return n, true
}

// checkOnSegmentAPlus (2C-3b) is the SOURCE side's on_segment capability
// check WITHOUT a kernel dispatch: the source agent reads its own segment
// catalog (the S primary hosts) and SSHes to each to verify the segreceiver
// binary (presence + executability + version against the agent's own build
// identity) and — when --seg-listen is set — that the listen-grid BASE port
// is free (the per-unit grid is base+unitID at run time; a busy base means
// the grid origin is taken). The kernel's ON SEGMENT support is NOT probed
// (no real dispatch) and is reported as a note. Returns (false, nil) when
// segBinary is empty (the check is not run).
func (s *SourceServer) checkOnSegmentAPlus(ctx context.Context, q pg.Querier, segBinary, segSSH, segListen string, greenplum bool) (bool, []string) {
	if segBinary == "" {
		return false, nil
	}
	if !greenplum {
		return false, []string{"segment-direct capability: not a Greenplum cluster (coordinator path only); kernel ON SEGMENT not checked (the first real run verifies it)"}
	}
	var hosts []string
	rows, err := q.Query(ctx, "SELECT hostname FROM gp_segment_configuration WHERE role = 'p' ORDER BY content")
	if err != nil {
		return false, []string{fmt.Sprintf("segment-direct capability: cannot read the source segment catalog: %v; kernel ON SEGMENT not checked", err)}
	}
	for _, row := range rows {
		if len(row) > 0 && row[0] != nil {
			if h := fmt.Sprint(row[0]); h != "" {
				hosts = append(hosts, h)
			}
		}
	}
	if len(hosts) == 0 {
		return false, []string{"segment-direct capability: no up source segments (gp_segment_configuration empty); kernel ON SEGMENT not checked"}
	}
	port, checkPort := segListenPort(segListen)
	summary := make([]string, 0, len(hosts)+1)
	ok := true
	for _, h := range hosts {
		// The 1F convention: when the SSH user is set, prefix it for EVERY
		// host (gpadmin@127.0.0.1 = the loopback layout, also verified).
		ssh := ""
		if segSSH != "" {
			ssh = segSSH + h
		}
		binOK, remoteVer, portState, perr := hostProbe(ctx, ssh, h, segBinary, port, checkPort)
		if perr != nil {
			ok = false
			summary = append(summary, fmt.Sprintf("%s: SSH FAIL — %v", h, perr))
			continue
		}
		line := h + ": "
		if !binOK {
			ok = false
			line += "segreceiver binary MISSING or not executable"
		} else {
			line += "segreceiver binary present+executable"
			if remoteVer != "" && remoteVer != version.String() {
				ok = false
				line += fmt.Sprintf(" (version mismatch: host=%s agent=%s)", remoteVer, version.String())
			}
		}
		if checkPort {
			switch portState {
			case "FREE":
				line += fmt.Sprintf(", listen base port %d free", port)
			case "BUSY":
				ok = false
				line += fmt.Sprintf(", listen base port %d BUSY", port)
			default:
				line += fmt.Sprintf(", listen base port %d: probe unavailable (no bash on host?)", port)
			}
		}
		summary = append(summary, line)
	}
	summary = append(summary, "kernel ON SEGMENT support: NOT checked in dry-run (no real dispatch; verified by the first real run)")
	return ok, summary
}

// checkMiniReady (2C-3b) is the DEST side's mini-agent readiness check
// (supervisor mode): the dest agent (on the destination coordinator) SSHes
// to each D segment host and verifies the mini binary — the SAME
// gpstream-dest-agent binary (the lockstep package install puts it at the
// same path on every host, the same assumption launchDestMinis uses) — is
// present + executable + the right version. It does NOT launch the minis
// (a dry-run must not start real processes). The mini data ports are
// auto-allocated at launch (0:0 or within --port-range), so the port
// config is reported, not probed per host.
func (s *DestServer) checkMiniReady(ctx context.Context) string {
	hosts := s.opts.MiniHosts
	if len(hosts) == 0 {
		return "FAIL: mini hosts unknown (the supervisor's MiniHosts are empty — pass --mini-hosts or let the agent read its catalog)"
	}
	bin, err := os.Executable()
	if err != nil || bin == "" {
		bin = "gpstream-dest-agent"
	}
	summary := make([]string, 0, len(hosts)+1)
	ok := true
	for _, h := range hosts {
		// The launch convention: SSH only when the host is NOT this host
		// (a single-host layout runs the mini locally).
		ssh := ""
		if s.opts.MiniSSHUser != "" && !IsLocalHost(h) {
			ssh = s.opts.MiniSSHUser + h
		}
		binOK, remoteVer, _, perr := hostProbe(ctx, ssh, h, bin, 0, false)
		if perr != nil {
			ok = false
			summary = append(summary, fmt.Sprintf("%s: SSH FAIL — %v", h, perr))
			continue
		}
		line := h + ": "
		if !binOK {
			ok = false
			line += "mini binary MISSING or not executable"
		} else {
			line += "mini binary present+executable"
			if remoteVer != "" && remoteVer != version.String() {
				ok = false
				line += fmt.Sprintf(" (version mismatch: host=%s agent=%s)", remoteVer, version.String())
			}
		}
		summary = append(summary, line)
	}
	if s.opts.PortAlloc != nil && s.opts.PortAlloc.Enabled() {
		lo, hi := s.opts.PortAlloc.Range()
		summary = append(summary, fmt.Sprintf("mini data ports: range %d-%d (auto-allocated per mini at launch)", lo, hi))
	} else {
		summary = append(summary, "mini data ports: auto-allocated at launch (no --port-range)")
	}
	if ok {
		return "OK: " + strings.Join(summary, "; ")
	}
	return "FAIL: " + strings.Join(summary, "; ")
}
