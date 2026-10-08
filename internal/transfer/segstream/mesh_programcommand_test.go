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
