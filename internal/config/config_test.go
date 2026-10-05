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
		{"bad conflict action", func(c *Config) { c.ConflictAction = "bogus" }},
		{"bad verify", func(c *Config) { c.Verify = "bogus" }},
		{"removed copy-mode direct", func(c *Config) { c.CopyMode = "direct" }},
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

func TestDataPortRangeValidation(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantLo  int
		wantHi  int
		wantErr bool
	}{
		{"single base", "9000", 9000, 9000, false},
		{"range", "9000-9010", 9000, 9010, false},
		{"lo>hi", "9010-9000", 0, 0, true},
		{"bad lo", "0-100", 0, 0, true},
		{"bad hi", "1-70000", 0, 0, true},
		{"garbage", "abc", 0, 0, true},
		{"empty", "", 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lo, hi, err := parsePortRange(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got lo=%d hi=%d", lo, hi)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if lo != c.wantLo || hi != c.wantHi {
				t.Fatalf("got (%d,%d), want (%d,%d)", lo, hi, c.wantLo, c.wantHi)
			}
		})
	}
}

func TestDataPortRangeCheck(t *testing.T) {
	// coordinator: needs 3 ports
	cfg := Default()
	cfg.CopyMode = CopyCoordinator
	cfg.DataPortRange = "9000-9002"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := cfg.CheckDataPortRange(3); err != nil {
		t.Fatalf("coordinator 3-port range should pass: %v", err)
	}
	small := Default()
	small.CopyMode = CopyCoordinator
	small.DataPortRange = "9000-9001"
	if err := small.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := small.CheckDataPortRange(3); err == nil {
		t.Fatal("coordinator 2-port range should fail")
	}
	// on_segment: needs 2D+1
	cfg = Default()
	cfg.CopyMode = CopyOnSegment
	cfg.DataPortRange = "9000-9007"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := cfg.CheckDataPortRange(3); err != nil {
		t.Fatalf("on_segment D=3 needs 7, range 8 should pass: %v", err)
	}
	if err := cfg.CheckDataPortRange(4); err == nil {
		t.Fatal("on_segment D=4 needs 9, range 8 should fail")
	}
	// auto (no range): always passes
	auto := Default()
	auto.CopyMode = CopyOnSegment
	if err := auto.CheckDataPortRange(50); err != nil {
		t.Fatalf("auto range should pass: %v", err)
	}
}
