package manifest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/statusfile"
)

// The 26.11 remote-port bug: the controller probed its OWN loopback to pick
// the remote agent's control port; that port can be held on the AGENT host
// (e.g. a TIME_WAIT socket of a just-killed previous agent), so the agent's
// bind died with EADDRINUSE and the control dial timed out after the full
// deadline. Remote agents now self-pick ("0.0.0.0:0") and the controller
// discovers the real bound address from the agent's status file.
func TestControlListenRemoteSelfPick(t *testing.T) {
	r := &processTwoAgentRunner{cfg: &config.Config{}}
	listen, dial, err := r.controlListen("vm4")
	if err != nil {
		t.Fatalf("controlListen(remote): %v", err)
	}
	if listen != "0.0.0.0:0" {
		t.Fatalf("remote listen = %q, want 0.0.0.0:0 (kernel pick on the agent host)", listen)
	}
	if dial != "" {
		t.Fatalf("remote dial = %q, want empty (discovered via the status file)", dial)
	}
	// Local (same host) keeps the pre-bound layout: listen == dial, non-empty.
	lo, loDial, err := r.controlListen("localhost")
	if err != nil {
		t.Fatalf("controlListen(local): %v", err)
	}
	if lo == "" || lo != loDial {
		t.Fatalf("local: listen=%q dial=%q, want equal and non-empty", lo, loDial)
	}
}

// pollAgentControlAddr: the status file may report the address on a later
// heartbeat (the agent writes once at start, then every 2s). The bound
// address is a loopback/any alias (the remote agent binds 0.0.0.0) and must
// be rewritten to the agent host - the same rewrite the peer address gets.
func TestPollAgentControlAddr(t *testing.T) {
	calls := 0
	r := &processTwoAgentRunner{
		cfg: &config.Config{},
		statusRead: func(ctx context.Context, sshHost, file string) (*statusfile.Status, error) {
			calls++
			if calls < 3 {
				return &statusfile.Status{Side: "dest"}, nil // not bound yet
			}
			return &statusfile.Status{Side: "dest", Control: "[::]:43879"}, nil
		},
	}
	addr, err := r.pollAgentControlAddr(context.Background(), "gp@vm4", "/tmp/agent-dest.json", 5*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if addr != "vm4:43879" {
		t.Fatalf("addr = %q, want vm4:43879 (host rewrite of [::]:43879)", addr)
	}
	if calls < 3 {
		t.Fatalf("polled %d times, want >= 3", calls)
	}
}

func TestPollAgentControlAddrTimeout(t *testing.T) {
	r := &processTwoAgentRunner{
		cfg: &config.Config{},
		statusRead: func(ctx context.Context, sshHost, file string) (*statusfile.Status, error) {
			return nil, os.ErrNotExist // no status file yet
		},
	}
	if _, err := r.pollAgentControlAddr(context.Background(), "gp@vm4", "/tmp/agent-dest.json", 400*time.Millisecond); err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
}

// Real read seam (statusRead == nil): a local status file written by the
// statusfile writer is picked up; a loopback address is returned untouched.
func TestPollAgentControlAddrRealLocalFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "agent-dest.json")
	st := statusfile.Status{Side: "dest", State: "serving", Control: "127.0.0.1:5541"}
	if err := statusfile.Write(file, st); err != nil {
		t.Fatalf("write status: %v", err)
	}
	r := &processTwoAgentRunner{cfg: &config.Config{}}
	addr, err := r.pollAgentControlAddr(context.Background(), "", file, 5*time.Second)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if addr != "127.0.0.1:5541" {
		t.Fatalf("addr = %q, want 127.0.0.1:5541 (local untouched)", addr)
	}
}

// agentStatusFileFor: remote agents ALWAYS get a status file (control-port
// discovery needs it even when no operation-id / state-dir is set), and it
// must live in the OS temp dir — NOT under the state dir: the file is
// written on the AGENT host, where the state dir may not exist and its
// parent may not be writable by the SSH user (root-owned /var/lib — the
// 26.11 r5 incident: silent heartbeat failure, every remote pair timed out
// at the control-dial deadline). Local agents keep the state-dir path
// ("" when unresolvable) so `gpstream status` readback works.
func TestAgentStatusFileFor(t *testing.T) {
	withOp := NewProcessTwoAgentRunner(&config.Config{StateDir: "/var/lib/gpstream", OperationID: "op1"})
	remote := withOp.agentStatusFileFor("dest", "gp@vm4")
	if !strings.HasPrefix(remote, os.TempDir()+string(filepath.Separator)) {
		t.Fatalf("remote status file %q must live in the OS temp dir (writable by the SSH user), not the state dir", remote)
	}
	if !strings.Contains(remote, "op1") || !strings.Contains(remote, "dest") {
		t.Fatalf("remote status file must embed op-id and side for diagnostics: %q", remote)
	}
	// Stable within one launch (the flag value and the poll must agree).
	if again := withOp.agentStatusFileFor("dest", "gp@vm4"); again != remote {
		t.Fatalf("path must be stable within a launch: %q vs %q", remote, again)
	}
	// A NEW launch (fault-recovery re-run of the same op-id, possibly with
	// a reused controller pid) must get a DIFFERENT file: a stale file from
	// the previous launch would hand the controller a dead control address.
	other := NewProcessTwoAgentRunner(&config.Config{StateDir: "/var/lib/gpstream", OperationID: "op1"})
	if got := other.agentStatusFileFor("dest", "gp@vm4"); got == remote {
		t.Fatalf("two launches must not share a status file (stale read risk): %q", got)
	}
	// No op-id: still an absolute path (identical on both hosts).
	noOp := &processTwoAgentRunner{cfg: &config.Config{}}
	fb := noOp.agentStatusFileFor("dest", "gp@vm4")
	if !filepath.IsAbs(fb) {
		t.Fatalf("fallback status file must be absolute (identical on both hosts): %q", fb)
	}
	// Local agents: state-dir path, "" when unresolvable (no discovery).
	if local := withOp.agentStatusFileFor("source", "gp@localhost"); local != "/var/lib/gpstream/op1/agent-source.json" {
		t.Fatalf("local agent keeps the state-dir path, got %q", local)
	}
	if local := noOp.agentStatusFileFor("source", "gp@localhost"); local != "" {
		t.Fatalf("local agent without op-id needs no discovery file, got %q", local)
	}
}

// persistAgentRegistry records the paths the agents ACTUALLY write
// (agentStatusFileFor), so `gpstream status` readback hits the same files —
// for a remote agent that is the temp-dir path, not the state dir.
func TestAgentRegistryUsesRemoteTmpPaths(t *testing.T) {
	statedir := t.TempDir()
	r := NewProcessTwoAgentRunner(&config.Config{
		StateDir:       statedir,
		OperationID:    "opR",
		SourceAgentSSH: "gp@vm3",
		DestAgentSSH:   "gp@vm4",
		Dest:           "host=vm4 ...",
	})
	if err := os.MkdirAll(filepath.Join(statedir, "opR"), 0o750); err != nil {
		t.Fatalf("mkdir op dir: %v", err)
	}
	r.persistAgentRegistry()
	raw, err := os.ReadFile(filepath.Join(statedir, "opR", "agents.json"))
	if err != nil {
		t.Fatalf("agents.json: %v", err)
	}
	var reg agentRegistry
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("agents.json parse: %v", err)
	}
	for side, e := range map[string]agentStatusEntry{"source": reg.Source, "dest": reg.Dest} {
		if !strings.HasPrefix(e.StatusFile, os.TempDir()+string(filepath.Separator)) {
			t.Fatalf("%s registry path %q must be the temp-dir path the agent actually writes", side, e.StatusFile)
		}
		if strings.Contains(e.StatusFile, statedir) {
			t.Fatalf("%s registry path %q must not point into the state dir", side, e.StatusFile)
		}
	}
	if reg.Source.SSHHost != "gp@vm3" || reg.Dest.SSHHost != "gp@vm4" {
		t.Fatalf("registry must keep the full ssh targets: %+v", reg)
	}
}
