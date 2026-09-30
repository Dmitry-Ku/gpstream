package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDSNHasPassword: the F-13 credential heuristic recognizes the common
// password spellings and stays quiet on a clean DSN.
func TestDSNHasPassword(t *testing.T) {
	cases := map[string]bool{
		"":                                 false,
		"postgres://user@host/db":          false, // user, no password
		"postgres://user:secret@host/db":   true,  // URL userinfo
		"host=h user=u password=pw":        true,  // libpq keyword
		"postgres://user@h/db?password=pw": true,  // query param
		"postgres://gpadmin@10.0.0.11:5432/postgres?sslmode=disable": false,
	}
	for dsn, want := range cases {
		if got := dsnHasPassword(dsn); got != want {
			t.Errorf("dsnHasPassword(%q) = %v, want %v", dsn, got, want)
		}
	}
}

// TestSecurityWarningsPasswordFromFile (F-13): a DSN with an embedded
// password that came from the config FILE is flagged; a clean DSN, a DSN
// from the CLI (no file provenance), and a no-config run produce no note.
func TestSecurityWarningsPasswordFromFile(t *testing.T) {
	c := Config{
		ConfigFile:     "",
		Source:         "postgres://u:p@h/db",
		SourceFromFile: true,
	}
	if w := c.SecurityWarnings(); len(w) != 1 || !strings.Contains(w[0], "source DSN") {
		t.Fatalf("file DSN with password: warnings = %v, want one source-DSN note", w)
	}

	// Same DSN but from the CLI (no file provenance): no note.
	c.SourceFromFile = false
	if w := c.SecurityWarnings(); len(w) != 0 {
		t.Fatalf("CLI DSN: warnings = %v, want none", w)
	}

	// File DSN without a password: no note.
	c2 := Config{Source: "postgres://u@h/db", SourceFromFile: true}
	if w := c2.SecurityWarnings(); len(w) != 0 {
		t.Fatalf("passwordless file DSN: warnings = %v, want none", w)
	}

	// Both sides flagged independently.
	c3 := Config{
		Source: "postgres://u:p@h/s", SourceFromFile: true,
		Dest: "host=h password=pw", DestFromFile: true,
	}
	if w := c3.SecurityWarnings(); len(w) != 2 {
		t.Fatalf("both DSNs with passwords: warnings = %v, want two", w)
	}
}

// TestSecurityWarningsConfigFileMode (F-13 "config file ... mode 0600"): a
// config file wider than 0600 is flagged, a 0600 one is not.
func TestSecurityWarningsConfigFileMode(t *testing.T) {
	dir := t.TempDir()
	loose := filepath.Join(dir, "loose.toml")
	if err := os.WriteFile(loose, []byte("source = \"postgres://u@h/db\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := Config{ConfigFile: loose}
	w := c.SecurityWarnings()
	if len(w) != 1 || !strings.Contains(w[0], "0600") {
		t.Fatalf("0644 config: warnings = %v, want one 0600 note", w)
	}

	tight := filepath.Join(dir, "tight.toml")
	if err := os.WriteFile(tight, []byte("source = \"postgres://u@h/db\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c2 := Config{ConfigFile: tight}
	if w := c2.SecurityWarnings(); len(w) != 0 {
		t.Fatalf("0600 config: warnings = %v, want none", w)
	}
}
