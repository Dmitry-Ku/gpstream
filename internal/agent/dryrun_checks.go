// Package agent — 2C-3b (architecture 08.10): the AGENT-SIDE on_segment /
// mini-readiness dry-run checks (A+). The dry-run must NOT run a real ON
// SEGMENT kernel dispatch (no `COPY ... TO PROGRAM ... ON SEGMENT`, no
// launching the segreceiver / mini) — "the kernel is NOT checked". Instead
// the side's AGENT (running as gpadmin on its cluster's coordinator) reads
// its own segment catalog and SSHes to each segment host to verify the
// binary (presence + executability + version) and the listen-port config.
// gpadmin has passwordless SSH to its own segments (the standard Greenplum
// layout).
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
// executability, its reported version, (optionally) whether the given local
// port is already taken (a bash /dev/tcp connect probe), and (optionally)
// whether each of the given hostnames resolves on the host (a getent probe —
// the mesh name preflight: the segreceivers dial the destination mini-agents
// by these names FROM THIS HOST). The remote shell prints one line per
// probe:
//
//	BIN_OK | BIN_MISSING
//	<the binary's --version line, empty when the binary is absent>
//	PORT_FREE | PORT_BUSY | PORT_NA  (only when checkPort; PORT_NA = no bash)
//	NAME_OK <name> | NAME_FAIL <name>  (one line per nameCheck)
//
// The script ALWAYS exits 0 (the trailing `true`): the status is carried in
// the printed lines, never in the exit code — RunRemoteOutput treats a
// non-zero exit as a transport failure, so a BUSY port or an UNRESOLVED
// name (probe findings, not faults) must not look like one.
func probeScript(bin string, port int, checkPort bool, nameChecks []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "test -x %s && echo BIN_OK || echo BIN_MISSING\n", quoteRemote(bin))
	fmt.Fprintf(&sb, "%s --version 2>/dev/null | head -n1\n", quoteRemote(bin))
	if checkPort {
		// bash /dev/tcp connect probe; PORT_NA when the host has no bash
		// (the port is then simply not probed, not "free").
		fmt.Fprintf(&sb, "command -v bash >/dev/null 2>&1 && { (exec 3<>/dev/tcp/127.0.0.1/%d) 2>/dev/null && echo PORT_BUSY || echo PORT_FREE; } || echo PORT_NA\n", port)
	}
	for _, n := range nameChecks {
		// getent hosts prints the resolution line and exits 0 iff the name
		// resolves (DNS or /etc/hosts — exactly what a dial will use). The
		// name is echoed unquoted in the result token: it is a catalog
		// identifier (no shell metacharacters), and the parser matches it
		// verbatim.
		fmt.Fprintf(&sb, "getent hosts %s >/dev/null 2>&1 && echo NAME_OK %s || echo NAME_FAIL %s\n", quoteRemote(n), n, n)
	}
	if len(nameChecks) > 0 {
		// The done marker lets the parser distinguish "the probe ran and
		// these names failed" from "the probe did not run at all" (an
		// OLDER agent whose probe script predates the name checks — a
		// mixed-version dry-run must WARN, not false-FAIL every name).
		sb.WriteString("echo NAMEPROBE_DONE\n")
	}
	sb.WriteString("true\n")
	return sb.String()
}

// parseProbe parses the probeScript output (see its doc) into the binary
// status, the remote version (the "<name> <ver (commit)>" line with the
// binary name token dropped — the same convention as gatherSourceVersion),
// the port state ("", "FREE", or "BUSY"; "" = not probed, or the probe was
// unavailable on the host — no bash / PORT_NA), and the name-check results:
// nameFailures = the requested names with a NAME_FAIL line or NO result
// line at all (missing output fails loud, the dial would just as well fail),
// nameProbeRan = the NAMEPROBE_DONE marker was seen (false = the probe did
// NOT run — an older agent's output has no name lines at all, and its
// results must not be read as failures). nameChecks empty = no name probes
// in the output, nothing to report (nameProbeRan is then meaningless, false).
func parseProbe(out string, nameChecks []string) (binOK bool, remoteVer, portState string, nameFailures []string, nameProbeRan bool) {
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
	resolved := make(map[string]bool)
	nameProbeRan = false
	for _, l := range lines {
		f := strings.Fields(l)
		if len(f) >= 2 && (f[0] == "NAME_OK" || f[0] == "NAME_FAIL") {
			if f[0] == "NAME_OK" {
				resolved[f[1]] = true
			}
		} else if len(f) >= 1 && f[0] == "NAMEPROBE_DONE" {
			nameProbeRan = true
		}
	}
	// Only when the probe actually ran (marker seen) do missing/failing
	// names count as failures; an older agent's output has no marker and no
	// name lines — reading it as "every name failed" would false-FAIL a
	// mixed-version dry-run.
	if nameProbeRan {
		for _, n := range nameChecks {
			if !resolved[n] {
				nameFailures = append(nameFailures, n)
			}
		}
	}
	return
}

// hostProbe runs the readiness probe on ONE segment host (local when
// sshHost is empty — a single-host layout where the host IS this host) and
// returns the parsed result. The SSH error is non-nil when the host is
// unreachable (a broken key / network is itself the dry-run finding).
func hostProbe(ctx context.Context, sshHost, host, bin string, port int, checkPort bool, nameChecks []string) (binOK bool, remoteVer, portState string, nameFailures []string, nameProbeRan bool, err error) {
	out, perr := RunRemoteOutput(ctx, sshHost, probeScript(bin, port, checkPort, nameChecks))
	if perr != nil {
		return false, "", "", nil, false, fmt.Errorf("probe %s: %w", host, perr)
	}
	b, v, p, nf, ran := parseProbe(out, nameChecks)
	return b, v, p, nf, ran, nil
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
// is free (a busy base means the grid origin is taken). When nameChecks is
// non-empty (mesh preflight, 14.10) the same per-host probe ALSO verifies
// each name resolves on the host — the mesh segreceivers dial the
// destination segment mini-agents by those names FROM these hosts, and an
// unresolvable name fails the mesh run mid-copy. The kernel's ON SEGMENT
// support is NOT probed (no real dispatch) and is reported as a note.
// Returns (ok, summary, nameFailures, probed, nameProbeMissing):
// nameFailures are the unresolvable names host-qualified ("name (from
// host)"), probed is the number of (host, name) probes executed (0 = the
// check did not run), nameProbeMissing is true when name checks were
// requested but NO host's probe output carried the done marker (an older
// agent version — the check did not run and must be reported as
// unverified, not as failures). (false, nil, nil, 0, false) when
// segBinary is empty (the check is not run).
func (s *SourceServer) checkOnSegmentAPlus(ctx context.Context, q pg.Querier, segBinary, segSSH, segListen string, nameChecks []string, greenplum bool) (bool, []string, []string, int, bool) {
	if segBinary == "" {
		return false, nil, nil, 0, false
	}
	if !greenplum {
		return false, []string{"segment-direct capability: not a Greenplum cluster (coordinator path only); kernel ON SEGMENT not checked (the first real run verifies it)"}, nil, 0, false
	}
	var hosts []string
	rows, err := q.Query(ctx, "SELECT hostname FROM gp_segment_configuration WHERE role = 'p' ORDER BY content")
	if err != nil {
		return false, []string{fmt.Sprintf("segment-direct capability: cannot read the source segment catalog: %v; kernel ON SEGMENT not checked", err)}, nil, 0, false
	}
	for _, row := range rows {
		if len(row) > 0 && row[0] != nil {
			if h := fmt.Sprint(row[0]); h != "" {
				hosts = append(hosts, h)
			}
		}
	}
	if len(hosts) == 0 {
		return false, []string{"segment-direct capability: no up source segments (gp_segment_configuration empty); kernel ON SEGMENT not checked"}, nil, 0, false
	}
	port, checkPort := segListenPort(segListen)
	summary := make([]string, 0, len(hosts)+1)
	var nameFailures []string
	seenNameFail := make(map[string]bool)
	probed := 0
	nameProbeSaw := 0
	ok := true
	for _, h := range hosts {
		// The 1F convention: when the SSH user is set, prefix it for EVERY
		// host (gpadmin@127.0.0.1 = the loopback layout, also verified).
		ssh := ""
		if segSSH != "" {
			ssh = segSSH + h
		}
		binOK, remoteVer, portState, nameFails, nameRan, perr := hostProbe(ctx, ssh, h, segBinary, port, checkPort, nameChecks)
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
		if len(nameChecks) > 0 {
			if nameRan {
				nameProbeSaw++
				probed += len(nameChecks)
				for _, n := range nameFails {
					ok = false
					qualified := fmt.Sprintf("%s (from %s)", n, h)
					if !seenNameFail[qualified] {
						seenNameFail[qualified] = true
						nameFailures = append(nameFailures, qualified)
					}
					line += fmt.Sprintf(", dest host %q does NOT resolve (add /etc/hosts or DNS entry on this host)", n)
				}
			} else {
				// No done marker: the agent on this host did not run the
				// name probe (an older agent version) — report it, do not
				// count the names as failed.
				line += ", name probe not run by the agent (older agent version? — update the agents)"
			}
		}
		summary = append(summary, line)
	}
	summary = append(summary, "kernel ON SEGMENT support: NOT checked in dry-run (no real dispatch; verified by the first real run)")
	nameProbeMissing := len(nameChecks) > 0 && nameProbeSaw == 0
	return ok, summary, nameFailures, probed, nameProbeMissing
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
		binOK, remoteVer, _, _, _, perr := hostProbe(ctx, ssh, h, bin, 0, false, nil)
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
