package config

// Coverage for the pure config helpers: parseSizeEnv, FrameAckOn,
// ReliablePipeConfig, ApplyEnv (full env sweep), Validate error matrix,
// leafString/assignConfigString (via LoadFile), ParsePorts.

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseSizeEnv(t *testing.T) {
	cases := map[string]int64{
		"1024":  1024,
		"512":   512,
		"512K":  512 << 10,
		"512k":  512 << 10,
		"2M":    2 << 20,
		"3m":    3 << 20,
		"1G":    1 << 30,
		"2g":    2 << 30,
		"1T":    1 << 40,
		"1t":    1 << 40,
		" 64M ": 64 << 20, // trimmed
		"":      0,
		"junk":  0, // ParseInt fails -> 0
		"12x":   0, // unknown suffix letter: kept, ParseInt("12x") fails -> 0
	}
	for in, want := range cases {
		if got := parseSizeEnv(in); got != want {
			t.Errorf("parseSizeEnv(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestFrameAckOn(t *testing.T) {
	c := Config{FrameAck: ""}
	if !c.FrameAckOn() {
		t.Error("empty frame-ack must be ON (default)")
	}
	c.FrameAck = "on"
	if !c.FrameAckOn() {
		t.Error("on must be ON")
	}
	c.FrameAck = "off"
	if c.FrameAckOn() {
		t.Error("off must be OFF")
	}
}

func TestReliablePipeConfig(t *testing.T) {
	// adaptive: 30s fallback + flag.
	c := Config{AckTimeout: "adaptive", MaxReconnects: 7, RetransmitBufferMax: 1 << 20}
	cfg := c.ReliablePipeConfig()
	if cfg.AckTimeout != 30*time.Second || !cfg.AdaptiveAckTimeout {
		t.Errorf("adaptive: AckTimeout=%v adaptive=%v, want 30s/true", cfg.AckTimeout, cfg.AdaptiveAckTimeout)
	}
	if cfg.MaxReconnects != 7 || cfg.RetransmitBufferMax != 1<<20 {
		t.Errorf("adaptive: reconnects=%d buffer=%d, want 7/1MiB", cfg.MaxReconnects, cfg.RetransmitBufferMax)
	}
	// duration.
	c = Config{AckTimeout: "15s"}
	cfg = c.ReliablePipeConfig()
	if cfg.AckTimeout != 15*time.Second || cfg.AdaptiveAckTimeout {
		t.Errorf("15s: got %v/%v, want 15s/false", cfg.AckTimeout, cfg.AdaptiveAckTimeout)
	}
	// invalid duration: falls back to defaults (no panic).
	c = Config{AckTimeout: "banana"}
	cfg = c.ReliablePipeConfig()
	if cfg.AckTimeout == 0 {
		t.Error("invalid ack-timeout must fall back to a non-zero default")
	}
	// Default: validated, carries the normalised transport defaults.
	c = Default()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg = c.ReliablePipeConfig()
	if cfg.MaxReconnects != 20 || cfg.RetransmitBufferMax != 64<<20 {
		t.Error("validated default config must carry normalised transport defaults")
	}
}

// TestApplyEnvSweep exercises every GPSTREAM_* overlay in ApplyEnv, including
// the bool parsing (true/1 vs anything else) and the invalid-number paths
// (value left untouched).
func TestApplyEnvSweep(t *testing.T) {
	t.Setenv("GPSTREAM_RATE_LIMIT", "42.5")
	t.Setenv("GPSTREAM_WORKERS_PER_PAIR", "3")
	t.Setenv("GPSTREAM_COMPRESS", "none")
	t.Setenv("GPSTREAM_SMALL_TABLE_THRESHOLD", "2097152")
	t.Setenv("GPSTREAM_AUTO_RESUME", "2")
	t.Setenv("GPSTREAM_ON_UNIT_FAILURE", "abort")
	t.Setenv("GPSTREAM_CONSISTENT_TABLES", "true")
	t.Setenv("GPSTREAM_SEGMENT_BALANCE_THRESHOLD", "1.25")
	t.Setenv("GPSTREAM_SSL_CERT", "/c.pem")
	t.Setenv("GPSTREAM_SSL_KEY", "/k.pem")
	t.Setenv("GPSTREAM_SSL_CA", "/ca.pem")
	t.Setenv("GPSTREAM_SEG_TLS_CERT", "/sc.pem")
	t.Setenv("GPSTREAM_SEG_TLS_KEY", "/sk.pem")
	t.Setenv("GPSTREAM_SEG_TLS_CA", "/sca.pem")
	t.Setenv("GPSTREAM_LOCALE_CHECK", "true")
	t.Setenv("GPSTREAM_FRAME_ACK", "off")
	t.Setenv("GPSTREAM_ACK_TIMEOUT", "adaptive")
	t.Setenv("GPSTREAM_MAX_RECONNECTS", "5")
	t.Setenv("GPSTREAM_RETRANSMIT_BUFFER_MAX", "64M")

	c := Default()
	c.ApplyEnv()

	if c.RateLimit != 42.5 || c.WorkersPerPair != 3 || c.Compression != "none" ||
		c.SmallTableThreshold != 2097152 || c.AutoResume != 2 ||
		c.OnUnitFailure != "abort" || !c.ConsistentTables || c.SegmentBalanceThreshold != 1.25 {
		t.Errorf("scalar env overlay wrong: %+v", c)
	}
	if c.SSLCert != "/c.pem" || c.SSLKey != "/k.pem" || c.SSLCA != "/ca.pem" ||
		c.SegTLSCert != "/sc.pem" || c.SegTLSKey != "/sk.pem" || c.SegTLSCA != "/sca.pem" {
		t.Errorf("tls env overlay wrong: %+v", c)
	}
	if !c.LocaleCheck {
		t.Errorf("locale-check env overlay wrong: %+v", c)
	}
	if c.FrameAck != "off" || c.AckTimeout != "adaptive" || c.MaxReconnects != 5 ||
		c.RetransmitBufferMax != 64<<20 {
		t.Errorf("frame-ack env overlay wrong: %+v", c)
	}
}

// TestApplyEnvInvalidValues: unparseable numbers must leave the field as-is.
func TestApplyEnvInvalidValues(t *testing.T) {
	t.Setenv("GPSTREAM_RATE_LIMIT", "not-a-float")
	t.Setenv("GPSTREAM_WORKERS_PER_PAIR", "not-an-int")
	t.Setenv("GPSTREAM_AUTO_RESUME", "junk")
	t.Setenv("GPSTREAM_MAX_RECONNECTS", "junk")
	t.Setenv("GPSTREAM_CONSISTENT_TABLES", "false")

	c := Default()
	c.RateLimit = 99
	c.WorkersPerPair = 7
	c.AutoResume = 3
	c.MaxReconnects = 11
	c.ApplyEnv()

	if c.RateLimit != 99 || c.WorkersPerPair != 7 || c.AutoResume != 3 || c.MaxReconnects != 11 {
		t.Errorf("invalid env values must not clobber: %+v", c)
	}
	if c.ConsistentTables {
		t.Error("false-ish bool env values must parse to false")
	}
}

func TestValidateErrorMatrix(t *testing.T) {
	base := Default()
	fail := func(mutate func(*Config), name string) {
		c := base
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
	fail(func(c *Config) { c.ZstdLevel = 0 }, "zstd=0")
	fail(func(c *Config) { c.ZstdLevel = 20 }, "zstd=20")
	fail(func(c *Config) { c.SmallTableThreshold = -1 }, "threshold<0")
	fail(func(c *Config) { c.WorkersPerPair = 0 }, "workers=0")
	fail(func(c *Config) { c.AutoResume = -1 }, "auto-resume<0")
	fail(func(c *Config) { c.OnUnitFailure = "stop" }, "on-unit-failure")
	fail(func(c *Config) { c.ResumeConsistency = "loose" }, "resume-consistency")
	fail(func(c *Config) { c.MetadataOnly, c.DataOnly = true, true }, "metadata+data")
	fail(func(c *Config) { c.ConsistentDB, c.ConsistentCluster = true, true }, "db+cluster")
	fail(func(c *Config) { c.ConsistentTables, c.ConsistentCluster = true, true }, "tables+cluster")
	fail(func(c *Config) { c.ConsistentTables, c.ConsistentDB = true, true }, "tables+db")
	fail(func(c *Config) { c.SegmentBalanceThreshold = NaN() }, "nan-threshold")
	fail(func(c *Config) { c.ConflictAction = ConflictAction("bogus") }, "resolve-conflicts")
	fail(func(c *Config) { c.Verify = VerifyMode("bogus") }, "verify")
	fail(func(c *Config) { c.CopyMode = "bogus" }, "copy-mode")
	fail(func(c *Config) { c.CopyMode = CopyOnSegment; c.SegBinary = "" }, "on_segment-no-binary")
	fail(func(c *Config) { c.CopyMode = CopyOnSegment; c.SegBinary = "/x"; c.SegListen = "" }, "on_segment-no-listen")
	fail(func(c *Config) { c.FrameAck = "maybe" }, "frame-ack")
	fail(func(c *Config) { c.AckTimeout = "banana" }, "ack-timeout")

	// Valid edge cases must NOT fail.
	ok := base
	ok.AckTimeout = "adaptive"
	if err := ok.Validate(); err != nil {
		t.Errorf("adaptive must validate: %v", err)
	}
	ok = base
	ok.AckTimeout = "" // normalised to 30s
	if err := ok.Validate(); err != nil || ok.AckTimeout != "30s" {
		t.Errorf("empty ack-timeout must normalise to 30s, got %q err %v", ok.AckTimeout, err)
	}
	ok = base
	ok.MaxReconnects = 0
	ok.RetransmitBufferMax = 0
	if err := ok.Validate(); err != nil || ok.MaxReconnects != 20 || ok.RetransmitBufferMax != 64<<20 {
		t.Errorf("zero transport knobs must normalise, got %d/%d err %v", ok.MaxReconnects, ok.RetransmitBufferMax, err)
	}
}

func NaN() float64 {
	var z float64
	return z / z
}

func TestLeafString(t *testing.T) {
	cases := []struct {
		in   any
		want string
		ok   bool
	}{
		{"str", "str", true},
		{42, "42", true},
		{int64(7), "7", true},
		{1.5, "1.5", true},
		{true, "true", true},
		{map[string]any{"a": 1}, "", false},
		{[]any{1}, "", false},
		{nil, "", false},
	}
	for _, c := range cases {
		got, ok := leafString(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("leafString(%v) = (%q,%v), want (%q,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

// TestLoadFileAssignsEveryStringKey writes a TOML and a YAML file carrying
// every assignConfigString key and checks the overlay (plus the numeric
// parse-failure tolerance: bad numbers are dropped, not fatal).
func TestLoadFileAssignsEveryStringKey(t *testing.T) {
	dir := t.TempDir()

	tomlSrc := `
source = "postgres://s"
dest = "postgres://d"
rate_limit = "7.5"
workers_per_pair = "4"
compress = "none"
zstd_level = "9"
small_table_threshold = "1024"
auto_resume = "1"
on_unit_failure = "abort"
consistent_tables = "true"
resume_consistency = "strict"
verify = "sample"
log_level = "warn"
ssl_cert = "/a"
ssl_key = "/b"
ssl_ca = "/c"
ssl_min_tls = "1.2"
seg_tls_cert = "/sa"
seg_tls_key = "/sb"
seg_tls_ca = "/sc"
data_port_range = "9000-9100"
skew_threshold = "0.8"
segment_balance_threshold = "1.7"
index_priority = "size"
timeout = "2m"
overwrite_only_conflicts = "1"
collation_check = "1"
locale_check = "true"
distribution_check = "1"
redistribution_plan = "1"
adaptive_rate_limit = "1"
checksum_algorithm = "sha256"
dedup = "1"
metrics = "1"
report_json = "/j"
report_csv = "/v"
report_text = "/t"
`
	// Same keys in YAML syntax (key: value).
	yamlSrc := `
source: "postgres://s"
dest: "postgres://d"
rate_limit: "7.5"
workers_per_pair: "4"
compress: "none"
zstd_level: "9"
small_table_threshold: "1024"
auto_resume: "1"
on_unit_failure: "abort"
consistent_tables: "true"
resume_consistency: "strict"
verify: "sample"
log_level: "warn"
ssl_cert: "/a"
ssl_key: "/b"
ssl_ca: "/c"
ssl_min_tls: "1.2"
seg_tls_cert: "/sa"
seg_tls_key: "/sb"
seg_tls_ca: "/sc"
data_port_range: "9000-9100"
skew_threshold: "0.8"
segment_balance_threshold: "1.7"
index_priority: "size"
timeout: "2m"
overwrite_only_conflicts: "1"
collation_check: "1"
locale_check: "true"
distribution_check: "1"
redistribution_plan: "1"
adaptive_rate_limit: "1"
checksum_algorithm: "sha256"
dedup: "1"
metrics: "1"
report_json: "/j"
report_csv: "/v"
report_text: "/t"
`
	ypath := filepath.Join(dir, "c.yaml")
	tpath := filepath.Join(dir, "c.toml")
	if err := os.WriteFile(ypath, []byte(yamlSrc), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tpath, []byte(tomlSrc), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{ypath, tpath} {
		c := Default()
		if err := c.LoadFile(path); err != nil {
			t.Fatalf("LoadFile(%s) = %v", path, err)
		}
		if c.Source != "postgres://s" || c.Dest != "postgres://d" || c.RateLimit != 7.5 ||
			c.WorkersPerPair != 4 || c.Compression != "none" || c.ZstdLevel != 9 ||
			c.SmallTableThreshold != 1024 || c.AutoResume != 1 || c.OnUnitFailure != "abort" ||
			!c.ConsistentTables || c.ResumeConsistency != "strict" || c.Verify != "sample" ||
			c.SSLCert != "/a" || c.SSLKey != "/b" || c.SSLCA != "/c" ||
			c.SSLMinTLS != "1.2" || c.SegTLSCert != "/sa" || c.SegTLSKey != "/sb" ||
			c.SegTLSCA != "/sc" || c.SkewThreshold != 0.8 ||
			c.SegmentBalanceThreshold != 1.7 || c.IndexPriority != "size" || c.Timeout != 2*time.Minute ||
			!c.LocaleCheck {
			t.Errorf("%s: overlay wrong: %+v", filepath.Base(path), c)
		}
	}
}

// TestLoadFileBadNumbersAreTolerant: a scalar with an unparseable number is
// dropped (field keeps its previous value), the load still succeeds.
func TestLoadFileBadNumbersAreTolerant(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bad.yaml")
	src := "rate_limit: \"not-a-float\"\nworkers_per_pair: \"nope\"\nzstd_level: \"x\"\nskew_threshold: \"zz\"\ntimeout: \"junk\"\n"
	if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c := Default()
	c.RateLimit = 11
	if err := c.LoadFile(p); err != nil {
		t.Fatalf("LoadFile = %v", err)
	}
	if c.RateLimit != 11 || c.ZstdLevel != Default().ZstdLevel || c.Timeout != Default().Timeout {
		t.Errorf("bad numbers must be dropped: %+v", c)
	}
}
