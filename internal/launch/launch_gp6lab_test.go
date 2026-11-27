//go:build gp6lab

package launch

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
)

// findDestAgent locates a built gpstream-dest-agent binary (PATH or the repo
// build dir) for the local-launch test. Lives in the gp6lab-tagged file
// because only the tagged test uses it (an untagged copy is U1000 dead code
// in the default build — the 06517ec cleanup removed it and broke the
// gp6lab build; restored here under the tag that actually uses it).
func findDestAgent(t *testing.T) string {
	t.Helper()
	if b, err := exec.LookPath("gpstream-dest-agent"); err == nil {
		return b
	}
	// Fall back to a fresh build (the repo is the working tree; go is on
	// PATH in the gate). Build from the repo root.
	wd, _ := os.Getwd()
	root := wd
	for i := 0; i < 6 && root != "/"; i++ {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		root = filepath.Dir(root)
	}
	bin := filepath.Join(t.TempDir(), "gpstream-dest-agent")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/gpstream-dest-agent")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("no dest-agent binary and go build failed: %v\n%s", err, out)
	}
	return bin
}

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
