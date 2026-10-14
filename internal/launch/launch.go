// Package launch ensures a long-lived gpstream agent is running and its
// control socket is reachable (design 04.10, Inc2d).
//
// The controller must reach an agent's control socket to drive it. There are
// three ways that socket comes up:
//
//  1. DIAL-FIRST (adopted daemon): the operator (or a systemd unit) already
//     started the agent on the control address. The controller simply dials
//     it — no launch, no SSH.
//  2. LOCAL (subprocess): the agent runs on the controller's host. The
//     controller pre-binds a loopback control port, starts the agent as a
//     child process with that port, and dials it. (The gp6lab / single-host
//     layout.)
//  3. SSH (remote, the multi-host GP6 layout): the agent must run where it
//     opens its pg connection (the source master / a destination host). The
//     controller SSHes to that host (key-based, non-interactive) and starts
//     the agent there, detached (setsid+nohup) so it outlives the SSH
//     session, then dials the control address.
//
// The controller's key must be in the agent host's authorized_keys
// (the administrator places it; P1e: no --key / --run-as knobs). The agent
// runs as the SSH user.
package launch

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Target describes how to bring one agent's control socket up.
type Target struct {
	// Binary is the agent binary name (as seen on the launch host — the
	// packaged install, resolved by name on PATH).
	Binary string
	// Args is the agent's full argument list (includes --control-listen and,
	// for the dest agent, --data-listen). It is identical for local and SSH
	// launches.
	Args []string
	// ControlAddr is the control socket address (host:port) the controller
	// dials. For local launches it is a pre-bound loopback port; for remote
	// launches it is a routable address the controller allocates.
	ControlAddr string
	// SSHHost is "user@host" or "user@host:port" to launch the agent on.
	// Empty = local subprocess (single-host layout, the controller IS the
	// cluster master).
	SSHHost string
	// DialFirst adopts a daemon already listening on ControlAddr instead of
	// launching (no probe — the control listener is one-shot; the caller's
	// bounded retry dial is the readiness check).
	DialFirst bool
	// WaitTimeout bounds how long to wait for the control socket to become
	// reachable after a launch.
	WaitTimeout time.Duration
	// LogPath is where a remote agent's stderr is appended (diagnosis).
	LogPath string
}

// Handle is a running (or adopted) agent.
type Handle struct {
	target Target
	// cmd is the local subprocess (nil for remote / adopted).
	cmd *exec.Cmd
	// remote records that the agent was launched over SSH (best-effort kill
	// target: the binary name + control host).
	remote  bool
	adopted bool
}

// Remote reports whether the agent was launched over SSH (vs local / adopted).
func (h *Handle) Remote() bool { return h != nil && h.remote }

// Adopted reports whether the agent was an already-running daemon.
func (h *Handle) Adopted() bool { return h != nil && h.adopted }

// ControlAddr is the address the controller dials.
func (h *Handle) ControlAddr() string { return h.target.ControlAddr }

// Stop releases the agent: for a local subprocess it kills the process; for a
// remote one it best-effort signals the agent over SSH to exit (the agent also
// exits on its own control-connection timeout — orphan protection). Adopted
// daemons are left running (the operator owns them).
func (h *Handle) Stop() {
	if h == nil {
		return
	}
	if h.cmd != nil {
		if h.cmd.Process != nil {
			_ = h.cmd.Process.Kill()
			_ = h.cmd.Wait()
		}
		return
	}
	if h.remote {
		// Best-effort: ask the remote agent to exit. The agent's own
		// control-timeout orphan protection is the real guarantee.
		remoteStop(h.target)
	}
}

// Ensure brings the agent's control socket up (adopt / local / SSH) and blocks
// until it is reachable (bounded). It returns a Handle the caller uses to
// dial the control socket and later Stop.
func Ensure(ctx context.Context, t Target) (*Handle, error) {
	if t.WaitTimeout <= 0 {
		t.WaitTimeout = 30 * time.Second
	}
	h := &Handle{target: t}
	_ = ctx // readiness is the caller's bounded retry dial (one-shot control).

	// 1. Dial-first: adopt a daemon the operator already started. NO
	// probe: the agent control listener is ONE-SHOT (a single Accept; the
	// agent exits when that connection drops — the crash-recovery matrix
	// depends on it), so a dial+close probe would consume the one accept
	// and kill the agent's control before the controller's real dial. If
	// the daemon is not actually up, the caller's bounded retry dial
	// fails with a clean "connection refused" after the deadline.
	if t.DialFirst {
		h.adopted = true
		return h, nil
	}

	// 2. Launch (SSH remote, or local subprocess).
	if t.SSHHost != "" {
		if err := remoteStart(ctx, t); err != nil {
			return nil, fmt.Errorf("launch: ssh start %s: %w", t.SSHHost, err)
		}
		h.remote = true
	} else {
		cmd := exec.CommandContext(ctx, t.Binary, t.Args...)
		cmd.Stdout = nil
		if lp := t.LogPath; lp != "" {
			cmd.Stderr = openLog(lp)
		}
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("launch: start %s: %w", t.Binary, err)
		}
		h.cmd = cmd
	}

	// 3. Readiness. NO probe on any path: the agent control listener is
	// ONE-SHOT (a single Accept; the agent exits when that connection drops
	// — the crash-recovery matrix depends on it), so a dial+close probe would
	// consume the one accept and kill the agent's control before the
	// controller's real dial. The caller dials with a bounded retry
	// (dialControlRetry / dialDestControlRetry, 15s) which tolerates the
	// agent's startup for local, SSH, and adopted daemons alike.
	_ = ctx // WaitTimeout bounds the caller's retry dial, not this call.
	return h, nil
}

// remoteStart launches the agent over SSH, detached (setsid+nohup) so it
// outlives the SSH session, appending its output to LogPath.
func remoteStart(ctx context.Context, t Target) error {
	remoteCmd := t.Binary + " " + strings.Join(shellQuoteAll(t.Args), " ")
	// setsid: new session (survives the SSH session); nohup: ignore HUP;
	// redirect stdio (a daemon has no tty); & : return immediately.
	full := fmt.Sprintf("setsid nohup %s </dev/null >>%s 2>&1 &", remoteCmd, t.LogPath)
	if t.LogPath == "" {
		full = fmt.Sprintf("setsid nohup %s </dev/null >/dev/null 2>&1 &", remoteCmd)
	}
	return runSSH(ctx, t, full)
}

// remoteStop best-effort signals the remote agent to exit (pkill by an
// INSTANCE-SPECIFIC pattern on the control host). The agent's control-timeout
// orphan protection is the real guarantee; this just makes Stop timely.
//
// BUG (14.10, live e2e pair runs): the old pattern was the bare binary
// basename (pkill -f gpstream-source-agent), so when two concurrent runs
// shared an agent host, one run's Stop killed the OTHER run's agent
// mid-copy (its source agent logged "control connection lost (controller
// gone)" and exited; the surviving run's next control write got a broken
// pipe and its remaining units failed). The instance-specific pattern is
// the binary + that agent's unique control-listen address: every run
// allocates its own control port, so no sibling run's agent matches.
func remoteStop(t Target) {
	host := sshHostOnly(t.SSHHost)
	_ = runSSH(context.Background(), Target{SSHHost: host},
		fmt.Sprintf("pkill -f '%s'", remoteStopPattern(t)))
}

// remoteStopPattern builds the pkill -f pattern for one agent instance:
// the binary basename + its control-listen flag/value. The source agent
// carries the control listen in --serve; the destination agent in
// --control-listen (both are present in every mode). The listen address is
// unique per run (the controller allocates a fresh control port), which is
// what makes the pattern instance-specific. When no control-listen arg is
// found (unknown/legacy target shape) it falls back to the bare basename
// so orphan cleanup still happens.
func remoteStopPattern(t Target) string {
	base := t.Binary
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	for i, a := range t.Args {
		if (a == "--serve" || a == "--control-listen") && i+1 < len(t.Args) {
			return base + `.*` + a + " " + escapeRegExp(t.Args[i+1])
		}
	}
	return base
}

// escapeRegExp escapes the ERE metacharacters in s (pkill -f takes an
// extended regular expression) so the listen address matches literally.
func escapeRegExp(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch c {
		case '.', '+', '*', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// sshHostOnly strips the port from user@host:port (the pkill runs on the host;
// the port is only for the SSH connection).
func sshHostOnly(host string) string {
	if i := strings.LastIndex(host, "@"); i >= 0 {
		rest := host[i+1:]
		if strings.Count(rest, ":") == 1 {
			return host[:i+1] + strings.SplitN(rest, ":", 2)[0]
		}
		return host
	}
	return host
}

// runSSH runs a non-interactive, key-based SSH command on t.SSHHost.
func runSSH(ctx context.Context, t Target, remote string) error {
	// StrictHostKeyChecking: use "no" (not "accept-new") for compatibility
	// with the lab controller's OpenSSH 7.4 (CentOS 7), which predates
	// accept-new (7.6+). Key-based auth + BatchMode=yes still prevents
	// password/MITM-in-auth attacks. Production hardening: pre-populate
	// known_hosts and switch to "yes" via a future config knob.
	args := []string{
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=10",
		"-o", "NumberOfPasswordPrompts=0",
	}
	if i := strings.LastIndex(t.SSHHost, "@"); i >= 0 && strings.Contains(t.SSHHost[i+1:], ":") {
		// user@host:port -> -p port user@host
		at := t.SSHHost[i+1:]
		host, port, _ := strings.Cut(at, ":")
		args = append(args, "-p", port, t.SSHHost[:i+1]+host, remote)
	} else {
		args = append(args, t.SSHHost, remote)
	}
	cmd := exec.CommandContext(ctx, "ssh", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ssh %s: %v: %s", t.SSHHost, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// shellQuoteAll quotes each arg for the remote shell (single-quoted, with the
// standard ” escape) so the agent argv is preserved exactly.
func shellQuoteAll(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
	}
	return out
}

// openLog opens (create/append) a log file for agent stderr.
func openLog(path string) *os.File {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil
	}
	return f
}
