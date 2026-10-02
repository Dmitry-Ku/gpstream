package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTmp writes content to a temp file and returns its path.
func writeTmp(t *testing.T, content string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	return f
}

// writeTmpAs writes content to a temp file with the given name (extension
// matters: LoadFile picks the format by it).
func writeTmpAs(t *testing.T, name, content string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	return f
}

// TestLoadFileUnknownMissingFile verifies a missing file surfaces the error.
func TestLoadFileMissing(t *testing.T) {
	c := Default()
	if err := c.LoadFile(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Error("expected an error for a missing config file")
	}
}

// TestLoadFileBadYAML verifies malformed YAML is rejected (not silently ignored).
func TestLoadFileBadYAML(t *testing.T) {
	c := Default()
	path := writeTmp(t, ": not : valid : yaml\n  - [")
	if err := c.LoadFile(path); err == nil {
		t.Error("expected a YAML parse error")
	}
}

// TestLoadFileTOML verifies the TOML layer (FR-05: /etc/gpstream/gpstream.toml)
// overlays known keys, including native numeric/bool values, onto defaults.

func TestLoadFileTOML(t *testing.T) {
	c := Default()
	path := writeTmpAs(t, "cfg.toml", `
source = "host=src.example.com"
dest = "host=dst.example.com"
rate_limit = 12.5
workers = 4
compress = "zstd"
zstd_level = 9
auto_resume = 2
verify = "on"
`)
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("load toml: %v", err)
	}
	if c.Source != "host=src.example.com" {
		t.Errorf("source = %q", c.Source)
	}
	if c.RateLimit != 12.5 {
		t.Errorf("rate limit = %v, want 12.5", c.RateLimit)
	}
	if c.WorkersPerPair != 4 {
		t.Errorf("workers = %d, want 4", c.WorkersPerPair)
	}
	if c.Compression != CompZstd {
		t.Errorf("compression = %v, want zstd", c.Compression)
	}
	if c.ZstdLevel != 9 {
		t.Errorf("zstd level = %d, want 9", c.ZstdLevel)
	}
	if c.AutoResume != 2 {
		t.Errorf("auto resume = %d, want 2", c.AutoResume)
	}
	if c.Verify != VerifyOn {
		t.Errorf("verify = %v, want on", c.Verify)
	}
}

// TestLoadFileBadTOML verifies malformed TOML is rejected.
func TestLoadFileBadTOML(t *testing.T) {
	c := Default()
	path := writeTmpAs(t, "bad.toml", "key = = \"broken")
	if err := c.LoadFile(path); err == nil {
		t.Error("expected a TOML parse error")
	}
}

// TestLoadFileOverlay verifies the file layer overlays known string keys onto
// defaults (FR-08: CLI > env > file > defaults).
func TestLoadFileOverlay(t *testing.T) {
	c := Default()
	path := writeTmp(t, `
source: host=src.example.com
dest: host=dst.example.com
rate_limit: "12.5"
workers: "4"
compress: zlib
zstd_level: "7"
auto_resume: "2"
verify: on
ssl_cert: /etc/cert.pem
ssl_key: /etc/key.pem
ssl_ca: /etc/ca.pem
ssl_min_tls: "1.3"
index_priority: fifo
timeout: "5m"
`)
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("load file: %v", err)
	}
	if c.Source != "host=src.example.com" {
		t.Errorf("source = %q", c.Source)
	}
	if c.Dest != "host=dst.example.com" {
		t.Errorf("dest = %q", c.Dest)
	}
	if c.RateLimit != 12.5 {
		t.Errorf("rate limit = %v, want 12.5", c.RateLimit)
	}
	if c.WorkersPerPair != 4 {
		t.Errorf("workers = %d, want 4", c.WorkersPerPair)
	}
	if c.Compression != CompZlib {
		t.Errorf("compression = %v, want zlib", c.Compression)
	}
	if c.ZstdLevel != 7 {
		t.Errorf("zstd level = %d, want 7", c.ZstdLevel)
	}
	if c.AutoResume != 2 {
		t.Errorf("auto resume = %d, want 2", c.AutoResume)
	}
	if c.Verify != VerifyOn {
		t.Errorf("verify = %v, want on", c.Verify)
	}
	if c.SSLMinTLS != "1.3" {
		t.Errorf("ssl min tls = %q, want 1.3", c.SSLMinTLS)
	}
	if c.IndexPriority != "fifo" {
		t.Errorf("index priority = %q, want fifo", c.IndexPriority)
	}
	if c.Timeout != 5*time.Minute {
		t.Errorf("timeout = %v, want 5m", c.Timeout)
	}
}

// TestLoadFileIgnoresUnknownKeysAndNonStrings verifies unknown keys and
// non-string leaves (ints/bools) are ignored without error (FR-08).
func TestLoadFileIgnoresUnknownAndNonStrings(t *testing.T) {
	c := Default()
	// "unknown_key" is ignored; "full" is a bool (not a string leaf) so it is
	// ignored here too; "source" (a string) is honoured.
	path := writeTmp(t, `
unknown_key: whatever
full: true
source: host=x
`)
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("load file: %v", err)
	}
	if c.Source != "host=x" {
		t.Errorf("source = %q, want host=x", c.Source)
	}
	// The bool "full" must not have clobbered the string fields.
}

// TestAssignConfigStringBadNumbers verifies bad numeric values are ignored
// (the field keeps its prior value) rather than panicking (FR-08).
func TestAssignConfigStringBadNumbers(t *testing.T) {
	c := Default()
	c.RateLimit = 9.0
	c.WorkersPerPair = 5
	assignConfigString(&c, "rate_limit", "not-a-float")
	assignConfigString(&c, "workers", "not-an-int")
	if c.RateLimit != 9.0 {
		t.Errorf("rate limit = %v, want 9.0 (bad float ignored)", c.RateLimit)
	}
	if c.WorkersPerPair != 5 {
		t.Errorf("workers = %d, want 5 (bad int ignored)", c.WorkersPerPair)
	}
}

// TestApplyEnvSecrets verifies SSL secrets are taken from env (FR-08: secrets
// never from argv).
func TestApplyEnvSecrets(t *testing.T) {
	t.Setenv("GPSTREAM_SSL_CERT", "/cert.pem")
	t.Setenv("GPSTREAM_SSL_KEY", "/key.pem")
	t.Setenv("GPSTREAM_SSL_CA", "/ca.pem")
	t.Setenv("GPSTREAM_AUTO_RESUME", "3")
	c := Default()
	c.ApplyEnv()
	if c.SSLCert != "/cert.pem" {
		t.Errorf("ssl cert = %q", c.SSLCert)
	}
	if c.SSLKey != "/key.pem" {
		t.Errorf("ssl key = %q", c.SSLKey)
	}
	if c.SSLCA != "/ca.pem" {
		t.Errorf("ssl ca = %q", c.SSLCA)
	}
	if c.AutoResume != 3 {
		t.Errorf("auto resume = %d, want 3", c.AutoResume)
	}
}

// TestApplyEnvBadNumbersIgnored verifies a bad env numeric is ignored (the
// field keeps its default), not set to zero/garbage.
func TestApplyEnvBadNumbersIgnored(t *testing.T) {
	t.Setenv("GPSTREAM_RATE_LIMIT", "not-a-float")
	t.Setenv("GPSTREAM_WORKERS", "not-an-int")
	c := Default()
	c.ApplyEnv()
	// Defaults: rate limit 0 (Default leaves it 0), workers 2.
	if c.WorkersPerPair != 2 {
		t.Errorf("workers = %d, want 2 (bad env int ignored)", c.WorkersPerPair)
	}
}
