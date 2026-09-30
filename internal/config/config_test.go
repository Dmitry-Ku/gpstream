package config

import (
	"testing"
	"time"
)

// TestExampleConfigLoads guards the shipped example (F-18: canonical TOML at
// /etc/gpstream/gpstream.toml) against the real loader — every key in
// packaging/examples/gpstream.toml must be a supported scalar key.
func TestExampleConfigLoads(t *testing.T) {
	path := "../../packaging/examples/gpstream.toml"
	c := Default()
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("LoadFile(%s) = %v", path, err)
	}
	if c.Compression != CompZstd {
		t.Errorf("compression = %v, want zstd from example", c.Compression)
	}
	if c.RateLimit != 80 {
		t.Errorf("rate limit = %v, want 80 from example", c.RateLimit)
	}
	if c.IndexPriority != "size" {
		t.Errorf("index priority = %v, want size from example", c.IndexPriority)
	}
	if c.WorkersPerPair != 8 {
		t.Errorf("workers per pair = %v, want 8 from example", c.WorkersPerPair)
	}
}

func TestDefault(t *testing.T) {
	c := Default()
	if c.ConflictAction != Stop {
		t.Errorf("default conflict action = %v, want stop", c.ConflictAction)
	}
	if c.Compression != CompZstd {
		t.Errorf("default compression = %v, want zstd", c.Compression)
	}
	if c.Timeout != 120*time.Second {
		t.Errorf("default timeout = %v, want 120s (F-10 §6)", c.Timeout)
	}
	if !c.ConsistentDB {
		t.Errorf("default consistency should be db")
	}
	if c.OnUnitFailure != "" {
		t.Errorf("default on-unit-failure = %q, want empty (continue)", c.OnUnitFailure)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("default config should validate, got %v", err)
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{"zstd too high", func(c *Config) { c.ZstdLevel = 20 }},
		{"zstd too low", func(c *Config) { c.ZstdLevel = 0 }},
		{"workers zero", func(c *Config) { c.WorkersPerPair = 0 }},
		{"negative auto-resume", func(c *Config) { c.AutoResume = -1 }},
		{"bad on-unit-failure", func(c *Config) { c.OnUnitFailure = "bogus" }},
		{"both metadata and data", func(c *Config) { c.MetadataOnly, c.DataOnly = true, true }},
		{"both consistent modes", func(c *Config) { c.ConsistentDB, c.ConsistentCluster = true, true }},
		{"consistent-tables and db", func(c *Config) { c.ConsistentDB, c.ConsistentTables = true, true }},
		{"consistent-tables and cluster", func(c *Config) { c.ConsistentCluster, c.ConsistentTables = true, true }},
		{"bad conflict action", func(c *Config) { c.ConflictAction = "bogus" }},
		{"bad verify", func(c *Config) { c.Verify = "bogus" }},
		{"negative small-table threshold", func(c *Config) { c.SmallTableThreshold = -1 }},
	}
	for _, tc := range cases {
		c := Default()
		tc.mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected validation error", tc.name)
		}
	}
}

func TestApplyEnv(t *testing.T) {
	t.Setenv("GPSTREAM_RATE_LIMIT", "50")
	t.Setenv("GPSTREAM_WORKERS", "8")
	t.Setenv("GPSTREAM_COMPRESS", "zlib")
	c := Default()
	c.ApplyEnv()
	if c.RateLimit != 50 {
		t.Errorf("rate limit = %v, want 50", c.RateLimit)
	}
	if c.WorkersPerPair != 8 {
		t.Errorf("workers = %d, want 8", c.WorkersPerPair)
	}
	if c.Compression != CompZlib {
		t.Errorf("compression = %v, want zlib", c.Compression)
	}
}

func TestApplyEnvConsistentTables(t *testing.T) {
	t.Setenv("GPSTREAM_CONSISTENT_TABLES", "true")
	c := Default()
	c.ApplyEnv()
	if !c.ConsistentTables {
		t.Errorf("consistent-tables = false, want true from env")
	}
}

// TestValidateNoRedistributeDoesNotSkipChecks: the old early `return nil`
// made --no-redistribute bypass every later validation (verify/copy-mode/
// frame-ack values went unvalidated). A bad value must still error.
func TestValidateNoRedistributeDoesNotSkipChecks(t *testing.T) {
	c := Default()
	c.NoRedistribute = true
	c.SkewThreshold = -1 // the sentinel --no-redistribute sets
	c.Verify = "bogus"
	if err := c.Validate(); err == nil {
		t.Fatal("Validate: want error for verify=bogus with --no-redistribute (early return regression)")
	}
}

// TestParseSizeValue: the file accepts the same size notation as the CLI.
func TestParseSizeValue(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1048576", 1048576},
		{"1M", 1 << 20},
		{"512K", 512 << 10},
		{"2G", 2 << 30},
		{"1T", 1 << 40},
		{"", 0},
	}
	for _, tc := range cases {
		if got, err := parseSizeValue(tc.in); err != nil || got != tc.want {
			t.Errorf("parseSizeValue(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	if _, err := parseSizeValue("abc"); err == nil {
		t.Error("parseSizeValue(abc): want error")
	}
}
