package segstream

import (
	"strings"
	"testing"
)

// TestProgramCommandTLS (F-13): the TLS flags are embedded in the shell
// command only for a complete, path-safe serving keypair; a lone cert/key is
// a loud error; shell metacharacters in any path are rejected (defense in
// depth — the paths land in the COPY TO PROGRAM one-liner).
func TestProgramCommandTLS(t *testing.T) {
	// Plain: no TLS -> identical to the legacy command.
	plain, err := ProgramCommandTLS("/usr/local/bin/gpstream-segreceiver", "10.0.0.5:9100", 42, 7, "", "", "")
	if err != nil {
		t.Fatalf("plain: %v", err)
	}
	if want := "/usr/local/bin/gpstream-segreceiver --listen 10.0.0.5:9100 --job 42 --unit 7 --content <SEGID>"; plain != want {
		t.Fatalf("plain cmd = %q, want %q", plain, want)
	}

	// TLS one-way (no CA): cert+key appended.
	c, err := ProgramCommandTLS("/bin/seg", "10.0.0.5:9100", 42, 7, "/etc/gpstream/srv.crt", "/etc/gpstream/srv.key", "")
	if err != nil {
		t.Fatalf("tls: %v", err)
	}
	if !strings.Contains(c, "--ssl-cert /etc/gpstream/srv.crt --ssl-key /etc/gpstream/srv.key") {
		t.Fatalf("tls cmd missing flags: %q", c)
	}
	if strings.Contains(c, "--ssl-ca") {
		t.Fatalf("tls cmd must not carry --ssl-ca when no CA: %q", c)
	}

	// TLS mTLS (CA): all three appended.
	m, err := ProgramCommandTLS("/bin/seg", "10.0.0.5:9100", 42, 7, "/etc/gpstream/srv.crt", "/etc/gpstream/srv.key", "/etc/gpstream/ca.pem")
	if err != nil {
		t.Fatalf("mtls: %v", err)
	}
	if !strings.Contains(m, "--ssl-cert /etc/gpstream/srv.crt --ssl-key /etc/gpstream/srv.key --ssl-ca /etc/gpstream/ca.pem") {
		t.Fatalf("mtls cmd missing flags: %q", m)
	}

	// A lone cert or key is a misconfiguration: loud error, no command.
	for _, bad := range [][3]string{
		{"/c.crt", "", ""},                 // cert only
		{"", "/k.key", ""},                 // key only
		{"/c.crt", "/k.key; rm -rf /", ""}, // shell metachar in the key
	} {
		if _, err := ProgramCommandTLS("/bin/seg", "10.0.0.5:9100", 1, 1, bad[0], bad[1], bad[2]); err == nil {
			t.Errorf("ProgramCommandTLS(%q,%q,%q) accepted, want error", bad[0], bad[1], bad[2])
		}
	}
	// Unsafe CA path rejected.
	if _, err := ProgramCommandTLS("/bin/seg", "10.0.0.5:9100", 1, 1, "/c.crt", "/k.key", "/ca; x"); err == nil {
		t.Error("ProgramCommandTLS accepted an unsafe CA path")
	}
}
