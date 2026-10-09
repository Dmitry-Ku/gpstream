// Package unit holds the pure-logic (no data path) unit tests for gpstream.
// P7 removed the in-controller data path, so the fake-conn data-path unit
// tests (full Manager.Run over faked SQL) are gone — that coverage is carried
// by the gp6lab-tagged integration tests and the lab e2e. What remains
// here is the operator-facing config surface and the metrics collector:
// pure logic that needs no cluster, no agents, and no build tag.
package unit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/monitor"
	"github.com/gpstream/gpstream/internal/ratelimit"
)

// provider is a ratelimit.Provider with a controllable load.
type provider struct{ load float64 }

func (p *provider) Snapshot() ratelimit.Metrics {
	return ratelimit.Metrics{IOWaitPercent: p.load}
}

// TestConfigFromEnv verifies (FR-08/FR-12) that secrets and compression come
// from env (never argv), and that env vars map to the right config fields.
func TestConfigFromEnv(t *testing.T) {
	t.Setenv("GPSTREAM_COMPRESS", "zlib")
	t.Setenv("GPSTREAM_RATE_LIMIT", "50")
	t.Setenv("GPSTREAM_SSL_CERT", "/etc/ssl/cert.pem")
	t.Setenv("GPSTREAM_SSL_KEY", "/etc/ssl/key.pem")

	cfg := config.Default()
	cfg.ApplyEnv()
	if cfg.Compression != "zlib" {
		t.Errorf("compression = %q, want zlib (from env)", cfg.Compression)
	}
	if cfg.RateLimit != 50 {
		t.Errorf("rate-limit = %v, want 50 (from env)", cfg.RateLimit)
	}
	if cfg.SSLCert != "/etc/ssl/cert.pem" {
		t.Errorf("ssl cert = %q, want /etc/ssl/cert.pem (from env)", cfg.SSLCert)
	}
}

// TestConfigFromFile verifies that a YAML config file populates the config.
func TestConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := strings.Join([]string{
		"source: postgresql://u:***@10.0.0.1:6000/prod",
		"dest: postgresql://u:***@10.0.0.2:6000/preprod",
		// LoadFile only honours string leaves (FR-08): numbers/bools must be
		// quoted so YAML parses them as strings, not as int/bool.
		"rate_limit: \"50\"",
		"compress: zstd",
		"zstd_level: \"9\"",
	}, "\n")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	if err := cfg.LoadFile(path); err != nil {
		t.Fatalf("load config file: %v", err)
	}
	if cfg.RateLimit != 50 {
		t.Errorf("rate-limit = %v, want 50", cfg.RateLimit)
	}
	if cfg.Compression != "zstd" {
		t.Errorf("compression = %q, want zstd", cfg.Compression)
	}
	if cfg.ZstdLevel != 9 {
		t.Errorf("zstd level = %d, want 9", cfg.ZstdLevel)
	}
}

// TestConfigSafetyDefaults locks the operator-safe defaults: strict conflict
// (stop), verify ON (08.10: content verification is on by default — digests are
// always computed and gate the swap; --verify off disables it), zstd compression.
func TestConfigSafetyDefaults(t *testing.T) {
	cfg := config.Default()
	if cfg.ConflictAction != config.Stop {
		t.Errorf("default conflict = %q, want %q", cfg.ConflictAction, config.Stop)
	}
	if cfg.Verify != config.VerifyOn {
		t.Errorf("default verify = %q, want on", cfg.Verify)
	}
	if cfg.Compression != config.CompZstd {
		t.Errorf("default compression = %q, want zstd", cfg.Compression)
	}
}

// TestAdaptiveLimiterEndToEnd verifies (FR-14) that the limiter, driven by a
// realistic provider, reduces under overload and recovers under load — the
// full feedback loop an operator relies on.
func TestAdaptiveLimiterEndToEnd(t *testing.T) {
	prov := &provider{load: 0}
	l := ratelimit.New(ratelimit.Options{Baseline: 100, Floor: 10, Step: 0.25, Provider: prov})

	// Healthy start: at baseline.
	if l.Current() != 100 {
		t.Fatalf("initial = %.2f, want 100", l.Current())
	}
	// Overload (iowait > 30%): must reduce below baseline, never below floor.
	prov.load = 80
	for i := 0; i < 200; i++ {
		l.Update()
		if l.Current() < 10 {
			t.Fatalf("current = %.2f went below the 10 Gbps floor", l.Current())
		}
	}
	if l.Current() >= 100 {
		t.Errorf("expected reduction under overload, got %.2f", l.Current())
	}
	// Load clears: must recover back toward the baseline.
	prov.load = 0
	for i := 0; i < 200; i++ {
		l.Update()
	}
	if l.Current() < 99 {
		t.Errorf("expected recovery to ~baseline, got %.2f", l.Current())
	}
}

// TestMetricsCollectBytes verifies (FR-04) that the metrics collector records
// the bytes moved by the data path — the throughput signal an operator needs.
func TestMetricsCollectBytes(t *testing.T) {
	m := monitor.New()
	m.Record(1<<20, 2<<20) // 1 MiB wire, 2 MiB logical
	if m.Bytes != 1<<20 {
		t.Errorf("bytes = %d, want %d", m.Bytes, 1<<20)
	}
	if m.BytesDecoded != 2<<20 {
		t.Errorf("bytes decoded = %d, want %d", m.BytesDecoded, 2<<20)
	}
	m.RecordError()
	m.RecordFrameError()
	if m.Errors == 0 {
		t.Error("expected errors to be recorded")
	}
	if m.FrameErrors == 0 {
		t.Error("expected a frame error to be recorded")
	}
}
