package config

import (
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	c := Default()
	if c.ConflictAction != SkipExisting {
		t.Errorf("default conflict action = %v, want skip-existing", c.ConflictAction)
	}
	if c.Compression != CompZstd {
		t.Errorf("default compression = %v, want zstd", c.Compression)
	}
	if c.Timeout != 60*time.Second {
		t.Errorf("default timeout = %v, want 60s", c.Timeout)
	}
	if !c.ConsistentDB {
		t.Errorf("default consistency should be db")
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
		{"both metadata and data", func(c *Config) { c.MetadataOnly, c.DataOnly = true, true }},
		{"both consistent modes", func(c *Config) { c.ConsistentDB, c.ConsistentCluster = true, true }},
		{"bad conflict action", func(c *Config) { c.ConflictAction = "bogus" }},
		{"bad verify", func(c *Config) { c.Verify = "bogus" }},
	}
	for _, tc := range cases {
		c := Default()
		tc.mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected validation error", tc.name)
		}
	}
}

func TestParsePorts(t *testing.T) {
	c := Default()
	c.DataPortRange = "2000-2010"
	lo, hi, err := c.ParsePorts()
	if err != nil {
		t.Fatalf("ParsePorts: %v", err)
	}
	if lo != 2000 || hi != 2010 {
		t.Errorf("got %d-%d, want 2000-2010", lo, hi)
	}
}

func TestParsePortsErrors(t *testing.T) {
	cases := []string{"", "abc", "5-1", "5", "5-"}
	for _, s := range cases {
		c := Default()
		c.DataPortRange = s
		if _, _, err := c.ParsePorts(); err == nil {
			t.Errorf("ParsePorts(%q): expected error", s)
		}
	}
}

func TestApplyEnv(t *testing.T) {
	t.Setenv("GPSTREAM_RATE_LIMIT", "50")
	t.Setenv("GPSTREAM_WORKERS_PER_PAIR", "8")
	t.Setenv("GPSTREAM_COMPRESS", "zlib")
	t.Setenv("GPSTREAM_LOG_LEVEL", "debug")
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
	if c.LogLevel != LogDebug {
		t.Errorf("log level = %v, want debug", c.LogLevel)
	}
}
