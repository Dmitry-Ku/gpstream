//go:build gp6lab

package integration

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestStatusLiveAgentsTwoAgent (2D-C, architecture 08.10) drives the REAL
// `gpstream run` + `gpstream status` CLIs end to end: while the run is in
// flight, each agent heartbeats its own live status JSON on its own host
// (<state-dir>/<op>/agent-<side>.json) and the controller persists the
// agents.json registry; `status` reads both files (locally here; over SSH in
// live mode) and renders the agents' live state — with NO database
// connection. The controller's copy pipeline holds zero direct DB
// connections (2D-A/2D-B), so the status file is the live-agent channel.
func TestStatusLiveAgentsTwoAgent(t *testing.T) {
	const (
		srcPort = 5793
		dstPort = 5794
	)
	bin := buildTwoAgentBinaries(t)

	ctx := context.Background()
	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	// A ~30 MB table: the run takes several seconds, leaving a window in
	// which `status` can observe both agents alive.
	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(src.Close)
	mustExec(t, src, `CREATE TABLE public.s2dc (id int, v text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.s2dc SELECT g, repeat('x', 100) || g FROM generate_series(1, 300000) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	controllerBin := liveControllerBinary(t, bin.gpstream)
	runArgs := liveCLIArgs(t, srcCl.URL(), dstCl.URL())
	stateDir := t.TempDir()
	opID := "s2dc-live"

	cmd := exec.Command(controllerBin, "run",
		"--source", srcCl.URL(), "--dest", dstCl.URL(),
		"--operation-id", opID, "--state-dir", stateDir,
		"--workers", "1", "--verify", "on")
	cmd.Args = append(cmd.Args, runArgs...)
	var runOut strings.Builder
	cmd.Stdout = &runOut
	cmd.Stderr = &runOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("start run: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}()

	// Wait for the run's registry + both agents' status files to appear.
	registry := filepath.Join(stateDir, opID, "agents.json")
	srcStatus := filepath.Join(stateDir, opID, "agent-source.json")
	dstStatus := filepath.Join(stateDir, opID, "agent-dest.json")
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(registry); err == nil {
			if _, err := os.Stat(srcStatus); err == nil {
				if _, err := os.Stat(dstStatus); err == nil {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := os.Stat(registry); err != nil {
		t.Fatalf("agents.json registry not written:\n%s", runOut.String())
	}
	if _, err := os.Stat(srcStatus); err != nil {
		t.Fatalf("agent-source.json not written:\n%s", runOut.String())
	}
	if _, err := os.Stat(dstStatus); err != nil {
		t.Fatalf("agent-dest.json not written:\n%s", runOut.String())
	}

	// The registry must record both agents (local mode: no ssh host).
	regRaw, err := os.ReadFile(registry)
	if err != nil {
		t.Fatalf("read agents.json: %v", err)
	}
	for _, want := range []string{`"source"`, `"dest"`, "agent-source.json", "agent-dest.json"} {
		if !strings.Contains(string(regRaw), want) {
			t.Errorf("agents.json missing %s:\n%s", want, regRaw)
		}
	}

	// While the run is in flight: `status` must render the live agents.
	st := exec.Command(controllerBin, "status", "--operation-id", opID, "--state-dir", stateDir)
	var stOut strings.Builder
	st.Stdout = &stOut
	st.Stderr = &stOut
	if err := st.Run(); err != nil {
		t.Fatalf("status CLI failed: %v\n--- output ---\n%s", err, stOut.String())
	}
	o := stOut.String()
	for _, want := range []string{"live agents:", "agent source:", "agent dest:"} {
		if !strings.Contains(o, want) {
			t.Errorf("status output missing %q:\n%s", want, o)
		}
	}

	// Wait for the run to finish (the deferred kill is a no-op then).
	runDone := make(chan error, 1)
	go func() { runDone <- cmd.Wait() }()
	select {
	case rerr := <-runDone:
		if rerr != nil {
			t.Fatalf("run CLI failed: %v\n%s", rerr, runOut.String())
		}
	case <-time.After(180 * time.Second):
		t.Fatalf("run did not finish:\n%s", runOut.String())
	}

	// After the run: `status` still renders the section (the last heartbeat
	// remains — the file survives the agents' exit, now shown as stale).
	st2 := exec.Command(controllerBin, "status", "--operation-id", opID, "--state-dir", stateDir)
	var st2Out strings.Builder
	st2.Stdout = &st2Out
	st2.Stderr = &st2Out
	if err := st2.Run(); err != nil {
		t.Fatalf("status CLI (post-run) failed: %v\n%s", err, st2Out.String())
	}
	if !strings.Contains(st2Out.String(), "live agents:") {
		t.Errorf("post-run status must still render the live-agents section:\n%s", st2Out.String())
	}
}
