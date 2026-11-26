package manifest

import (
	"context"
	"os"
	"path/filepath"
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
// discovery needs it even when no operation-id / state-dir is set); local
// agents keep the old ("" when unresolvable) behavior.
func TestAgentStatusFileFor(t *testing.T) {
	withOp := &processTwoAgentRunner{cfg: &config.Config{StateDir: "/var/lib/gpstream", OperationID: "op1"}}
	if got := withOp.agentStatusFileFor("dest", "gp@vm4"); got != "/var/lib/gpstream/op1/agent-dest.json" {
		t.Fatalf("with op-id = %q, want /var/lib/gpstream/op1/agent-dest.json", got)
	}
	noOp := &processTwoAgentRunner{cfg: &config.Config{}}
	got := noOp.agentStatusFileFor("dest", "gp@vm4")
	if got == "" {
		t.Fatal("remote agent without op-id must still get a status file (control-port discovery)")
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("fallback status file must be absolute (identical on both hosts): %q", got)
	}
	if again := noOp.agentStatusFileFor("dest", "gp@vm4"); again != got {
		t.Fatalf("fallback must be stable within a process: %q vs %q", again, got)
	}
	if local := noOp.agentStatusFileFor("source", "gp@localhost"); local != "" {
		t.Fatalf("local agent needs no discovery file, got %q", local)
	}
}
