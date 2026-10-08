package segstream

import (
	"strings"
	"testing"
)

// TestProgramCommandMesh (P3 production-dispatch, mesh direct_1to1): the
// address-book ProgramCommand — --dest-addr is the D-entry comma-separated
// mini-agent data endpoints, --dest-count is S, and the segreceiver resolves
// its own destination (entry <SEGID>). The validation rejects a malformed
// address book (empty entry / bad host:port) and a too-short book.
func TestProgramCommandMesh(t *testing.T) {
	const bin = "/usr/local/bin/gpstream-segreceiver"
	book := "10.0.0.2:4100,10.0.0.3:4101,10.0.0.4:4102"

	// Plain: address book + dest-count embedded, <SEGID> left for the kernel.
	cmd, err := ProgramCommandMesh(bin, book, 42, 7, "", "", "", "", 3)
	if err != nil {
		t.Fatalf("mesh: %v", err)
	}
	if want := bin + " --dest-addr " + book + " --dest-count 3 --job 42 --unit 7 --content <SEGID>"; cmd != want {
		t.Fatalf("mesh cmd = %q, want %q", cmd, want)
	}

	// Compression + TLS are appended exactly as the other builders do.
	cmd2, err := ProgramCommandMesh(bin, book, 42, 7, "/c.crt", "/k.key", "/ca.pem", "zstd", 3)
	if err != nil {
		t.Fatalf("mesh tls+compress: %v", err)
	}
	if !strings.Contains(cmd2, "--ssl-cert /c.crt --ssl-key /k.key --ssl-ca /ca.pem") {
		t.Fatalf("mesh tls cmd missing flags: %q", cmd2)
	}
	if !strings.HasSuffix(cmd2, "--compress zstd") {
		t.Fatalf("mesh compress cmd must end with --compress zstd: %q", cmd2)
	}

	// The frame-ack variant appends the Inc3 transport flags (off = plain).
	cmdFA, err := ProgramCommandMeshFrameAck(bin, book, 42, 7, "", "", "", "", "on", "30s", 20, 1<<30, 3)
	if err != nil {
		t.Fatalf("mesh frame-ack: %v", err)
	}
	if !strings.Contains(cmdFA, "--frame-ack on --ack-timeout 30s --max-reconnects 20 --retransmit-buffer-max 1073741824") {
		t.Fatalf("mesh frame-ack cmd missing flags: %q", cmdFA)
	}
	cmdOff, err := ProgramCommandMeshFrameAck(bin, book, 42, 7, "", "", "", "", "off", "30s", 20, 1<<30, 3)
	if err != nil {
		t.Fatalf("mesh frame-ack off: %v", err)
	}
	if strings.Contains(cmdOff, "--frame-ack") {
		t.Fatalf("mesh frame-ack off must not carry the flag: %q", cmdOff)
	}

	// A malformed address book is a hard error (the segreceiver must not dial
	// a wrong endpoint).
	for _, bad := range []string{
		"10.0.0.2:4100,,10.0.0.4:4102", // empty entry
		"10.0.0.2:4100,notanaddr",      // bad host:port
		"",                             // empty
	} {
		if _, err := ProgramCommandMesh(bin, bad, 42, 7, "", "", "", "", 3); err == nil {
			t.Fatalf("mesh must reject bad address book %q", bad)
		}
	}

	// A book shorter than the source segment count is a hard error.
	if _, err := ProgramCommandMesh(bin, "10.0.0.2:4100", 42, 7, "", "", "", "", 3); err == nil {
		t.Fatalf("mesh must reject a book shorter than the source segment count")
	}
}

// TestProgramCommandRoutedMesh (P3 production-dispatch, mesh routed S!=D):
// the routed-mesh ProgramCommand — --dest-addr is the D-entry address book
// (comma-separated), plus the routing flags (--route-key/--route-d/--route-
// workers). This is the B->D dev case (S=4 != D=3).
func TestProgramCommandRoutedMesh(t *testing.T) {
	addrBook := "192.168.122.15:15001,192.168.122.15:15002,192.168.122.15:15003"
	cmd, err := ProgramCommandRoutedMesh("/usr/bin/gpstream-segreceiver", addrBook, 1, 2,
		"", "", "", "zstd", "23:0", 3, 2)
	if err != nil {
		t.Fatalf("ProgramCommandRoutedMesh: %v", err)
	}
	for _, want := range []string{
		"--dest-addr " + addrBook,
		"--route-key 23:0",
		"--route-d 3",
		"--route-workers 2",
		"--content <SEGID>",
		"--compress zstd",
	} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("program missing %q:\n%s", want, cmd)
		}
	}
	// The direct-mesh flags must NOT leak into the routed-mesh program.
	if strings.Contains(cmd, "--dest-count") {
		t.Fatalf("routed-mesh program must not carry --dest-count (the direct-mesh flag): %s", cmd)
	}
	// A bad route key is rejected.
	if _, err := ProgramCommandRoutedMesh("/usr/bin/gpstream-segreceiver", addrBook, 1, 2,
		"", "", "", "zstd", "bad key", 3, 2); err == nil {
		t.Fatalf("bad route key must fail")
	}
	// A single-host:port (broadcast) address is valid for the routed-mesh
	// program too (the grid repeats it D times) — but the D-entry form is
	// the mesh layout.
	if _, err := ProgramCommandRoutedMesh("/usr/bin/gpstream-segreceiver", "192.168.122.15:15001", 1, 2,
		"", "", "", "zstd", "23:0", 3, 2); err != nil {
		t.Fatalf("broadcast (1 addr) must be valid for routed-mesh: %v", err)
	}
	// The frame-ack variant appends the transport flags.
	cmdFA, err := ProgramCommandRoutedMeshFrameAck("/usr/bin/gpstream-segreceiver", addrBook, 1, 2,
		"", "", "", "zstd", "23:0", 3, 2, "on", "30s", 20, 1<<30)
	if err != nil {
		t.Fatalf("ProgramCommandRoutedMeshFrameAck: %v", err)
	}
	if !strings.Contains(cmdFA, "--frame-ack on") {
		t.Fatalf("frame-ack program missing --frame-ack on:\n%s", cmdFA)
	}
}
