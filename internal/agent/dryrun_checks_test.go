package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/portalloc"
)

// TestQuoteRemote (2C-3b) proves the remote-shell quoting is single-quoted with
// the embedded-quote escape (the same convention as launch.shellQuoteAll).
func TestQuoteRemote(t *testing.T) {
	for in, want := range map[string]string{
		"/usr/bin/x": `'/usr/bin/x'`,
		"a b":        `'a b'`,
		`it's`:       `'it'\''s'`,
		"":           `''`,
		`a'b'c`:      `'a'\''b'\''c'`,
	} {
		if got := quoteRemote(in); got != want {
			t.Errorf("quoteRemote(%q) = %s, want %s", in, got, want)
		}
	}
}

// TestProbeScriptAndParse (2C-3b) proves the per-host probe script + its
// parser round-trip across the status space: binary ok + version, binary
// missing, port free, port busy, and the port-probe-unavailable case (no
// bash -> no third line).
func TestProbeScriptAndParse(t *testing.T) {
	// The script must reference the binary (quoted) and, only when asked,
	// the /dev/tcp port probe.
	s := probeScript("/bin/x", 12345, false)
	if !strings.Contains(s, `test -x '/bin/x'`) {
		t.Errorf("script missing the -x test: %s", s)
	}
	if !strings.Contains(s, `'/bin/x' --version`) {
		t.Errorf("script missing the --version call: %s", s)
	}
	if strings.Contains(s, "/dev/tcp") {
		t.Errorf("script must NOT probe the port when checkPort=false: %s", s)
	}
	sp := probeScript("/bin/x", 12345, true)
	if !strings.Contains(sp, "/dev/tcp/127.0.0.1/12345") {
		t.Errorf("script missing the /dev/tcp port probe: %s", sp)
	}
	if !strings.Contains(sp, "command -v bash") {
		t.Errorf("script missing the bash guard (no-bash hosts must not false-report PORT_FREE): %s", sp)
	}
	if !strings.HasSuffix(sp, "true\n") {
		t.Errorf("script must end with `true` (always exit 0 — a BUSY port is a finding, not a transport fault): %s", sp)
	}

	// Parser: the three-line contract.
	cases := []struct {
		name     string
		out      string
		wantBin  bool
		wantVer  string
		wantPort string
	}{
		{"ok+ver+free", "BIN_OK\ngpstream-segreceiver 0.2.0 (abc)\nPORT_FREE", true, "0.2.0 (abc)", "FREE"},
		{"ok+ver+busy", "BIN_OK\ngpstream-segreceiver 0.2.0 (abc)\nPORT_BUSY", true, "0.2.0 (abc)", "BUSY"},
		{"ok+nover", "BIN_OK\n\n", true, "", ""},
		{"missing", "BIN_MISSING\n", false, "", ""},
		{"ok+ver+noprobe", "BIN_OK\ngpstream-segreceiver 0.2.0 (abc)", true, "0.2.0 (abc)", ""},
		{"ok+probe-unavail", "BIN_OK\ngpstream-segreceiver 0.2.0 (abc)\n", true, "0.2.0 (abc)", ""},
		{"ok+ver+portna", "BIN_OK\ngpstream-segreceiver 0.2.0 (abc)\nPORT_NA\n", true, "0.2.0 (abc)", ""},
		{"empty", "", false, "", ""},
	}
	for _, c := range cases {
		b, v, p := parseProbe(c.out)
		if b != c.wantBin || v != c.wantVer || p != c.wantPort {
			t.Errorf("%s: parseProbe = (%v,%q,%q), want (%v,%q,%q)", c.name, b, v, p, c.wantBin, c.wantVer, c.wantPort)
		}
	}
}

// TestSegListenPort (2C-3b) proves the --seg-listen base-port parse: a valid
// host:port yields the port; empty / malformed / out-of-range yield ok=false.
func TestSegListenPort(t *testing.T) {
	if p, ok := segListenPort("10.0.0.1:30000"); !ok || p != 30000 {
		t.Errorf("valid: got (%d,%v), want (30000,true)", p, ok)
	}
	if _, ok := segListenPort(""); ok {
		t.Errorf("empty must not parse")
	}
	if _, ok := segListenPort("hostonly"); ok {
		t.Errorf("no-port must not parse")
	}
	if p, ok := segListenPort("h:99999"); ok || p != 0 {
		t.Errorf("out-of-range port: got (%d,%v), want (0,false)", p, ok)
	}
	if p, ok := segListenPort("h:abc"); ok || p != 0 {
		t.Errorf("non-numeric port: got (%d,%v), want (0,false)", p, ok)
	}
}

// TestCheckMiniReadySingleHost (2C-3b) exercises the LOCAL (no-SSH) path of
// the mini-readiness check: a single-host layout (MiniHosts=["127.0.0.1"],
// no SSH user) runs the probe on this host via /bin/sh. The mini binary is
// os.Executable() (the test binary here), which is present + executable, so
// the check must report OK (the version line is empty for a go test binary,
// which is tolerated — no spurious mismatch). It must NOT launch anything.
func TestCheckMiniReadySingleHost(t *testing.T) {
	s := &DestServer{opts: DestServerOptions{
		MiniHosts:   []string{"127.0.0.1"},
		MiniSSHUser: "", // empty = local, no SSH
	}}
	got := s.checkMiniReady(context.Background())
	if !strings.HasPrefix(got, "OK:") {
		t.Fatalf("single-host mini-ready must be OK, got: %s", got)
	}
	if !strings.Contains(got, "127.0.0.1") || !strings.Contains(got, "present+executable") {
		t.Errorf("mini-ready should note the host is present+executable: %s", got)
	}
	if !strings.Contains(got, "mini data ports:") {
		t.Errorf("mini-ready should report the port config: %s", got)
	}
}

// TestCheckMiniReadyPortRange (2C-3b) proves the port-range note reflects the
// allocator's configured range.
func TestCheckMiniReadyPortRange(t *testing.T) {
	alloc, err := portalloc.ParseRange("40000-40100")
	if err != nil {
		t.Fatalf("parse range: %v", err)
	}
	s := &DestServer{opts: DestServerOptions{
		MiniHosts: []string{"127.0.0.1"},
		PortAlloc: alloc,
	}}
	got := s.checkMiniReady(context.Background())
	if !strings.Contains(got, "range 40000-40100") {
		t.Errorf("mini-ready should report the port range: %s", got)
	}
}

// TestCheckMiniReadyNoHosts (2C-3b) proves the degenerate case (no hosts) is a
// clear FAIL, not a panic.
func TestCheckMiniReadyNoHosts(t *testing.T) {
	s := &DestServer{opts: DestServerOptions{}}
	got := s.checkMiniReady(context.Background())
	if !strings.HasPrefix(got, "FAIL") {
		t.Fatalf("no-hosts mini-ready must FAIL, got: %s", got)
	}
}
