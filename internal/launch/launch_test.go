package launch

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

// TestEnsureLocalSubprocess moved to launch_gp6lab_test.go (gp6lab tag): the
// P1e coordinator check needs a reachable local pg, so it runs against a real
// pgtest cluster like every other live-pg test in the repo.

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

// TestRemoteStopPatternIsInstanceSpecific (BUG 14.10, live e2e pair runs):
// the pkill pattern must identify ONE agent instance (binary + its unique
// control-listen), not the bare binary name — otherwise one run's Stop kills
// a sibling run's agent on the shared host mid-copy.
func TestRemoteStopPatternIsInstanceSpecific(t *testing.T) {
	src := Target{
		Binary: "/usr/local/bin/gpstream-source-agent",
		Args:   []string{"--src", "postgresql://u@h/db", "--snapshot", "db", "--serve", "0.0.0.0:33923"},
	}
	got := remoteStopPattern(src)
	want := `gpstream-source-agent.*--serve 0\.0\.0\.0:33923`
	if got != want {
		t.Fatalf("source pattern = %q, want %q", got, want)
	}
	// A sibling run (same binary, different control port) must NOT match.
	re := regexp.MustCompile(got)
	sibling := "gpstream-source-agent --src postgresql://u@h/db --serve 0.0.0.0:46096"
	if re.MatchString(sibling) {
		t.Fatalf("pattern %q matches a sibling run's agent: %q", got, sibling)
	}
	own := "gpstream-source-agent --src postgresql://u@h/db --serve 0.0.0.0:33923 --connect-timeout 10s"
	if !re.MatchString(own) {
		t.Fatalf("pattern %q must match its own agent: %q", got, own)
	}

	dst := Target{
		Binary: "gpstream-dest-agent",
		Args:   []string{"--dest", "postgresql://u@h/db", "--control-listen", "0.0.0.0:41000", "--persist-listen", "0.0.0.0:41001"},
	}
	if got := remoteStopPattern(dst); got != `gpstream-dest-agent.*--control-listen 0\.0\.0\.0:41000` {
		t.Fatalf("dest pattern = %q", got)
	}

	// Legacy/unknown shape: no control-listen arg -> bare basename (the
	// orphan-cleanup guarantee is preserved, never weakened to nothing).
	legacy := Target{Binary: "gpstream-source-agent", Args: []string{"--weird"}}
	if got := remoteStopPattern(legacy); got != "gpstream-source-agent" {
		t.Fatalf("legacy pattern = %q, want the bare basename", got)
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
