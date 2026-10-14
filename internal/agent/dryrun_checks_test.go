package agent

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/portalloc"
	"github.com/gpstream/gpstream/internal/version"
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
	s := probeScript("/bin/x", 12345, false, nil)
	if !strings.Contains(s, `test -x '/bin/x'`) {
		t.Errorf("script missing the -x test: %s", s)
	}
	if !strings.Contains(s, `'/bin/x' --version`) {
		t.Errorf("script missing the --version call: %s", s)
	}
	if strings.Contains(s, "/dev/tcp") {
		t.Errorf("script must NOT probe the port when checkPort=false: %s", s)
	}
	sp := probeScript("/bin/x", 12345, true, nil)
	if !strings.Contains(sp, "/dev/tcp/127.0.0.1/12345") {
		t.Errorf("script missing the /dev/tcp port probe: %s", sp)
	}
	if !strings.Contains(sp, "command -v bash") {
		t.Errorf("script missing the bash guard (no-bash hosts must not false-report PORT_FREE): %s", sp)
	}
	if !strings.HasSuffix(sp, "true\n") {
		t.Errorf("script must end with `true` (always exit 0 — a BUSY port is a finding, not a transport fault): %s", sp)
	}
	if strings.Contains(sp, "getent") {
		t.Errorf("script must NOT probe names when nameChecks is empty: %s", sp)
	}

	// Parser: the three-line contract (no name checks).
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
		b, v, p, nf, _ := parseProbe(c.out, nil)
		if b != c.wantBin || v != c.wantVer || p != c.wantPort {
			t.Errorf("%s: parseProbe = (%v,%q,%q), want (%v,%q,%q)", c.name, b, v, p, c.wantBin, c.wantVer, c.wantPort)
		}
		if nf != nil {
			t.Errorf("%s: parseProbe with no nameChecks must not report failures, got %v", c.name, nf)
		}
	}
}

// TestProbeScriptNameChecks (mesh preflight, 14.10) proves the name
// resolution probe: one quoted getent line per requested name, the result
// token carries the name verbatim, and the parser reports exactly the
// unresolvable names (NAME_FAIL or a MISSING result line — missing output
// fails loud, the dial would just as well fail).
func TestProbeScriptNameChecks(t *testing.T) {
	names := []string{"vm5", "vm6"}
	s := probeScript("/bin/x", 0, false, names)
	for _, n := range names {
		if !strings.Contains(s, "getent hosts '"+n+"'") {
			t.Errorf("script missing the quoted getent probe for %s: %s", n, s)
		}
		if !strings.Contains(s, "echo NAME_OK "+n) || !strings.Contains(s, "echo NAME_FAIL "+n) {
			t.Errorf("script missing the NAME_OK/NAME_FAIL result tokens for %s: %s", n, s)
		}
	}
	if !strings.HasSuffix(s, "true\n") {
		t.Errorf("script must still end with `true` with name checks: %s", s)
	}
	if !strings.Contains(s, "echo NAMEPROBE_DONE") {
		t.Errorf("script missing the NAMEPROBE_DONE marker (mixed-version guard): %s", s)
	}

	cases := []struct {
		name      string
		out       string
		wants     []string
		wantFails []string
		wantRan   bool
	}{
		{
			name:    "all resolve",
			out:     "BIN_OK\n\nNAME_OK vm5\nNAME_OK vm6\nNAMEPROBE_DONE\n",
			wants:   names,
			wantRan: true,
		},
		{
			name:  "one fails",
			out:   "BIN_OK\n\nNAME_OK vm5\nNAME_FAIL vm6\nNAMEPROBE_DONE\n",
			wants: names, wantFails: []string{"vm6"},
			wantRan: true,
		},
		{
			name:  "missing result line fails loud",
			out:   "BIN_OK\n\nNAME_OK vm5\nNAMEPROBE_DONE\n",
			wants: names, wantFails: []string{"vm6"},
			wantRan: true,
		},
		{
			name:    "empty output, names requested",
			out:     "",
			wants:   names,
			wantRan: false, // no marker: the probe did not run — not a failure
		},
		{
			name:    "old agent output (no marker): unverified, not failed",
			out:     "BIN_OK\n0.2.0 (abc)\nPORT_FREE\n",
			wants:   names,
			wantRan: false,
		},
	}
	for _, c := range cases {
		_, _, _, got, ran := parseProbe(c.out, c.wants)
		if ran != c.wantRan {
			t.Errorf("%s: nameProbeRan = %v, want %v", c.name, ran, c.wantRan)
		}
		if len(got) != len(c.wantFails) {
			t.Errorf("%s: parseProbe failures = %v, want %v", c.name, got, c.wantFails)
			continue
		}
		for i := range got {
			if got[i] != c.wantFails[i] {
				t.Errorf("%s: parseProbe failures = %v, want %v", c.name, got, c.wantFails)
			}
		}
	}
}

// TestHostProbeLocalNameChecks (mesh preflight, 14.10) runs the REAL probe
// script on the local host (sshHost empty -> /bin/sh) with one resolvable
// and one guaranteed-unresolvable name, proving the script + parser +
// RunRemoteOutput round-trip end to end (getent must exist — the same
// dependency the production probe has on every GP segment host).
func TestHostProbeLocalNameChecks(t *testing.T) {
	if _, terr := exec.LookPath("getent"); terr != nil {
		t.Skip("getent not available on this platform (the probe needs it)")
	}
	binOK, _, _, nameFails, nameRan, err := hostProbe(context.Background(), "", "local", "/bin/true", 0, false,
		[]string{"localhost", "gpstream-nosuchhost-xyz-1410"})
	if err != nil {
		t.Fatalf("local hostProbe: %v", err)
	}
	if !binOK {
		t.Errorf("local probe: /bin/true must report BIN_OK")
	}
	if !nameRan {
		t.Fatalf("local probe: the NAMEPROBE_DONE marker must be seen (the local agent is the current version)")
	}
	if len(nameFails) != 1 || nameFails[0] != "gpstream-nosuchhost-xyz-1410" {
		t.Errorf("name failures = %v, want [gpstream-nosuchhost-xyz-1410] (localhost resolves, the fake name must not)", nameFails)
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

// TestPeerVersionVerdictDryRunSkipsMiniMap (2C-1/2/4/5) proves the mesh
// dry-run does not deadlock on the mini-map: in dry-run mode the dest agent
// does NOT launch the D mini-agents (so the mini-map never completes), and
// its 1F version exchange must therefore skip the mini-map wait and check
// the source + segreceivers only. A real run keeps the wait.
func TestPeerVersionVerdictDryRunSkipsMiniMap(t *testing.T) {
	// A supervisor with a real (closed-on-cleanup) report listener but NO
	// minis: miniMapReady stays false, so a real run would block until the
	// AcceptTimeout deadline (the deadlock the dry-run must avoid).
	sup, err := NewSupervisor("127.0.0.1:0", nil, "", "", "", "")
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	t.Cleanup(sup.Close)

	sv := SourceVersion{Op: "source-version", Version: version.String()}

	// Dry-run: no mini-map wait -> OK immediately (the mini versions are
	// covered by the 2C-3b SSH mini-readiness check instead).
	sDry := &DestServer{opts: DestServerOptions{DryRun: true, AcceptTimeout: time.Second}}
	sDry.supervisor = sup
	if ack := sDry.peerVersionVerdict(sv); !ack.Ok {
		t.Fatalf("dry-run version exchange must skip the mini-map wait and be OK, got: %s", ack.Error)
	}

	// Real run (DryRun=false): the mini-map is not ready and the accept
	// timeout is short -> the wait times out (the pre-2C-1 behavior, kept for
	// real runs). This is the deadlock the dry-run flag removes.
	sReal := &DestServer{opts: DestServerOptions{AcceptTimeout: 200 * time.Millisecond}}
	sReal.supervisor = sup
	ack := sReal.peerVersionVerdict(sv)
	if ack.Ok {
		t.Fatal("real run with an unready mini-map must NOT be OK")
	}
	if !strings.Contains(ack.Error, "mini-map not ready") {
		t.Fatalf("real run should report the mini-map timeout, got: %s", ack.Error)
	}
}
