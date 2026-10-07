//go:build gp6lab

package launch

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
)

// TestEnsureLocalSubprocess launches the real dest agent as a local
// subprocess on a pre-bound control port and verifies Ensure returns once the
// control socket is reachable, then that the hello can be read.
//
// P1e regression note (07.10): the agent's coordinator check
// (CheckOnCoordinator, 30986/30989) runs BEFORE it binds the control
// listener and hard-fails when the DSN's local master is unreachable. The
// old test used a dummy DSN (127.0.0.1:1, nothing listening) → the agent
// exited before binding → the control dial was refused. The test now runs
// against a real local pgtest cluster (a non-Greenplum single-node postgres,
// which the coordinator check passes by design), matching every other
// live-pg test in the repo (gp6lab tag).
func TestEnsureLocalSubprocess(t *testing.T) {
	cl, err := pgtest.Start(5731, "gp_launch_local")
	if err != nil {
		t.Fatalf("pgtest.Start: %v", err)
	}
	t.Cleanup(cl.Stop)

	bin := findDestAgent(t)
	ctrlAddr := prebind(t)
	dataAddr := prebind(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h, err := Ensure(ctx, Target{
		Binary:      bin,
		Args:        []string{"--dest", cl.URL(), "--serve", "--data-listen", dataAddr, "--control-listen", ctrlAddr},
		ControlAddr: ctrlAddr,
		WaitTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("Ensure (local): %v", err)
	}
	defer h.Stop()
	if h.Remote() || h.Adopted() {
		t.Fatalf("handle should be a local subprocess (remote=%v adopted=%v)", h.Remote(), h.Adopted())
	}
	// The agent binds its control listener shortly after it starts; dial
	// with a short retry (the controller's real behavior — no probe, which
	// would consume the single accept).
	var client *agent.DestControlClient
	deadline := time.Now().Add(15 * time.Second)
	for {
		client, err = agent.DialDestControl(ctx, ctrlAddr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial control (retry exhausted): %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer client.Close()
	if client.Hello.Op != "hello" {
		t.Fatalf("hello = %+v", client.Hello)
	}
	// Stop kills the subprocess.
	h.Stop()
}
