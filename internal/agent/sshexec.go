package agent

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// RunRemoteOutput (1F) runs a remote command over SSH and returns its stdout
// (for diagnostics the operator reads, e.g. "gpstream-segreceiver --version"
// on a source segment). It reuses the strict SSH options of internal/launch
// (BatchMode, no password prompts, key-based auth). An empty sshHost runs the
// command locally (a single-host layout where the source host IS the
// segment). Lives in the agent package (not launch): launch's test imports
// agent, so agent cannot import launch (import cycle).
func RunRemoteOutput(ctx context.Context, sshHost, remote string) (string, error) {
	args := []string{
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=10",
		"-o", "NumberOfPasswordPrompts=0",
	}
	if i := strings.LastIndex(sshHost, "@"); i >= 0 && strings.Contains(sshHost[i+1:], ":") {
		at := sshHost[i+1:]
		host, port, _ := strings.Cut(at, ":")
		args = append(args, "-p", port, sshHost[:i+1]+host, remote)
	} else if sshHost != "" {
		args = append(args, sshHost, remote)
	}
	var out []byte
	var err error
	if sshHost != "" {
		cmd := exec.CommandContext(ctx, "ssh", args...)
		out, err = cmd.Output()
		if err != nil {
			return "", fmt.Errorf("ssh %s: %v", sshHost, err)
		}
	} else {
		// Local: /bin/sh -c so quoting matches the remote path.
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", remote)
		out, err = cmd.Output()
		if err != nil {
			return "", fmt.Errorf("local %s: %v", remote, err)
		}
	}
	return strings.TrimSpace(string(out)), nil
}
