package launch

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
)

// findDestAgent locates a built gpstream-dest-agent binary (repo build dir or
// PATH) for the local-launch test.
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

// prebind binds a loopback listener to learn a free port and releases it.
func prebind(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// TestEnsureLocalSubprocess launches the real dest agent as a local
// subprocess on a pre-bound control port and verifies Ensure returns once the
// control socket is reachable, then that the hello can be read.
func TestEnsureLocalSubprocess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix only")
	}
	bin := findDestAgent(t)
	ctrlAddr := prebind(t)
	dataAddr := prebind(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h, err := Ensure(ctx, Target{
		Binary:      bin,
		Args:        []string{"--dest", "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable", "--serve", "--data-listen", dataAddr, "--control-listen", ctrlAddr},
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

// TestEnsureAdopt verifies dial-first: an already-listening control socket is
// adopted (no launch).
func TestEnsureAdopt(t *testing.T) {
	ctrlAddr := prebind(t)
	l, err := net.Listen("tcp", ctrlAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	// Accept once so the dial-first probe connects (a real agent would be
	// sitting in Accept; a bare listener is enough for the reachability check).
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		conn, aerr := l.Accept()
		if aerr == nil {
			_ = conn.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h, err := Ensure(ctx, Target{
		ControlAddr: ctrlAddr,
		DialFirst:   true,
		Binary:      "/nonexistent", // must NOT be launched
		WaitTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Ensure (adopt): %v", err)
	}
	if !h.Adopted() {
		t.Fatalf("handle should be adopted (adopted=%v)", h.Adopted())
	}
	_ = acceptDone
}

// TestEnsureLocalNoProbe verifies the local path does NOT block on a control
// probe (it starts the child and returns; the controller dials with retry). A
// binary that never listens is still started cleanly; the dial is what fails
// (exactly the controller's behavior).
func TestEnsureLocalNoProbe(t *testing.T) {
	ctrlAddr := prebind(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	h, err := Ensure(ctx, Target{
		Binary:      "/bin/sleep", // never listens
		Args:        []string{"30"},
		ControlAddr: ctrlAddr,
		WaitTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Ensure (local) should not block on a probe: %v", err)
	}
	defer h.Stop()
	// The dial is what surfaces the missing socket (bounded).
	if _, err := agent.DialDestControl(ctx, ctrlAddr); err == nil {
		t.Fatal("dial to a non-listening address should fail")
	}
}

func TestShellQuoteAll(t *testing.T) {
	got := shellQuoteAll([]string{"--dest", "postgresql://u@h/db?sslmode=verify-full", "a'b"})
	want := []string{"'--dest'", "'postgresql://u@h/db?sslmode=verify-full'", "'a'\\''b'"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("shellQuoteAll[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSSHHostOnly(t *testing.T) {
	cases := map[string]string{
		"gp@192.0.2.85:2224": "gp@192.0.2.85",
		"gp@192.0.2.85":      "gp@192.0.2.85",
		"192.0.2.85":         "192.0.2.85",
	}
	for in, want := range cases {
		if got := sshHostOnly(in); got != want {
			t.Fatalf("sshHostOnly(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestRemoteStartBuildsCommand verifies the SSH command shape (via a fake
// ssh on PATH) without a real remote host.
func TestRemoteStartBuildsCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix only")
	}
	// A fake `ssh` that records its argv to a file and exits 0.
	dir := t.TempDir()
	logFile := filepath.Join(dir, "ssh-args.txt")
	fake := filepath.Join(dir, "ssh")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + logFile + "\nexit 0\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	oldPath := os.Getenv("PATH")
	defer os.Setenv("PATH", oldPath)
	os.Setenv("PATH", dir+":"+oldPath)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := remoteStart(ctx, Target{
		Binary:  "/usr/bin/gpstream-source-agent",
		Args:    []string{"--src", "postgresql://u@h/db", "--serve", "1.2.3.4:9"},
		SSHHost: "gp@192.0.2.85:2224",
		LogPath: "/var/log/agent.log",
	}); err != nil {
		t.Fatalf("remoteStart: %v", err)
	}
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	for _, want := range []string{
		"-p", "2224", "gp@192.0.2.85",
		// P1e: no -i (key) and no sudo -u (runas) — the admin places the key,
		// the agent runs as the SSH user.
		"setsid nohup /usr/bin/gpstream-source-agent '--src' 'postgresql://u@h/db' '--serve' '1.2.3.4:9'",
		"/var/log/agent.log",
	} {
		if !contains(s, want) {
			t.Fatalf("ssh argv missing %q\nfull:\n%s", want, s)
		}
	}
	for _, banned := range []string{"-i", "sudo -u"} {
		if contains(s, banned) {
			t.Fatalf("ssh argv must not contain %q (P1e removed key/runas)\nfull:\n%s", banned, s)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
