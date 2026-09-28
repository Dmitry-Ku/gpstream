// Package config holds the gpstream configuration model: defaults, CLI flag
// binding (contract from §5 of the specification), YAML file loading and
// environment overrides. Secrets are never accepted from argv (FR-08); they
// come only from 0600 files or environment variables.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	toml "github.com/BurntSushi/toml"
	"gopkg.in/yaml.v3"

	"github.com/gpstream/gpstream/internal/transfer"
)

// Compression selects the stream compression mode (FR-12).
type Compression string

const (
	CompNone Compression = "none"
	CompZlib Compression = "zlib"
	CompZstd Compression = "zstd"
)

// ConflictAction controls how the planner/manager treats tables that already
// exist in the destination (FR-10/FR-11). Combined with SkipConflicts it
// yields the runtime conflict policy (FR-10 §8):
//
//	SkipExisting + !SkipConflicts (the DEFAULT) -> STRICT: the run STOPS on a
//	  conflict so the operator decides (re-run with --skip-conflicts /
//	  --overwrite / a rename). Nothing is clobbered.
//	SkipExisting + SkipConflicts (--skip-conflicts / --skip-existing) -> the
//	  conflicting unit is skipped and the run continues.
//	Overwrite / Truncate / Append -> explicitly rewrite the destination
//	  (Overwrite additionally skips tables whose checksum already matches).
type ConflictAction string

const (
	// SkipExisting is the default conflict action. On its OWN (SkipConflicts
	// off) it is STRICT: the run stops on a conflict (FR-10 §8). Paired with
	// SkipConflicts it skips conflicting units.
	SkipExisting ConflictAction = "skip-existing"
	Overwrite    ConflictAction = "overwrite"
	Truncate     ConflictAction = "truncate"
	Append       ConflictAction = "append"
)

// VerifyMode controls the verification depth (FR-02).
type VerifyMode string

const (
	VerifyFull   VerifyMode = "full"
	VerifySample VerifyMode = "sample"
	VerifyOff    VerifyMode = "off"
)

// LogLevel selects verbosity.
type LogLevel string

const (
	LogDebug LogLevel = "debug"
	LogInfo  LogLevel = "info"
	LogWarn  LogLevel = "warn"
	LogError LogLevel = "error"
)

// CopyMode selects the source data-path transport (DESIGN §1.3 / T-B). The
// operator can override the doctor-detected capability with --copy-mode.
type CopyMode string

const (
	// CopyCoordinator is the default: COPY ... TO STDOUT through the source
	// coordinator (always works, single network path).
	CopyCoordinator CopyMode = "coordinator"
	// CopyOnSegment is the segment-direct fast path (Fallback B, T-B): the
	// kernel launches gpstream-segreceiver on every primary segment via
	// COPY ... TO PROGRAM ... ON SEGMENT; slices stream over direct TCP to
	// the controller (~2.0x throughput, verified on the lab).
	CopyOnSegment CopyMode = "on_segment"
	// CopyDirect is the fully segment-direct path (R5 endgame): the source
	// streams through the coordinator (already correct), gpstream computes
	// the destination segment per row (route.JumpConsistentHash over the
	// dist key, T4b) and writes each segment's rows directly into that
	// segment's local staging slice via utility-mode `COPY ... FROM STDIN`.
	// The destination coordinator is out of the write path. Requires a
	// Greenplum destination with a hash-distributed table and a
	// distribution-key type R5 can hash (int2/4/8, text, varchar, bytea);
	// any other table degrades to the coordinator path.
	CopyDirect CopyMode = "direct"
)

// Config is the fully resolved runtime configuration. It is the single source
// of truth consumed by every component; CLI flags, YAML and env all fold into
// it (CLI > env > file > defaults).
type Config struct {
	// Connection strings. Secrets may appear here but never in argv (FR-08).
	Source string
	Dest   string

	// Selection (FR-11).
	Full    bool
	DBs     []string
	Schemas []string // "db.schema"
	Tables  []string // "db.schema.table"
	Exclude []string // any exclude-* entries
	// Include (v4 §8 --include metadata,data): restrict which object classes
	// are copied — "metadata" (structure), "data" (rows), or both. Empty =
	// both (the default).
	Include []string

	// Metadata/data (FR-11).
	MetadataOnly bool
	DataOnly     bool

	// Conflict / safety (FR-10/FR-11).
	ConflictAction ConflictAction // default SkipExisting
	SkipConflicts  bool           // default false (strict)

	// Rename (FR-11).
	RenameDB     map[string]string // src -> dst
	RenameSchema map[string]string
	RenameTable  map[string]string // src -> dst (v2 §4.10)

	// Performance (FR-01/FR-03/FR-12/FR-14).
	Compression    Compression // default zstd
	ZstdLevel      int         // 1-19, default 3
	RateLimit      float64     // Gbps total ceiling
	WorkersPerPair int
	Streams        int    // v4 §8 --streams: parallel stream workers (alias of WorkersPerPair)
	DataPortRange  string // "a-b"

	// Resilience (FR-06/FR-07).
	AutoResume int        // N >= 0; 0 = manual only
	Verify     VerifyMode // default sample
	// ResumeConsistency selects the resume re-read semantics (T5, DESIGN
	// §2.2): "relaxed" (default) keeps a table verified in one snapshot
	// epoch; "strict" re-reads every table verified under an older snapshot.
	ResumeConsistency string // relaxed|strict, default relaxed

	// Indexes (FR-17).
	IndexWorkers  int
	NoIndexes     bool
	IndexPriority string // "size" | "manual"

	// Redistribution (FR-15).
	NoRedistribute bool
	SkewThreshold  float64

	// TLS (FR-13).
	SSL       bool // --tls: force an encrypted (TLS) connection (v2 §4.12)
	SSLCert   string
	SSLKey    string
	SSLCA     string
	SSLMinTLS string // e.g. "1.2"

	// Dry-run / planning (FR-10).
	DryRun   bool
	Timeout  time.Duration // plan formation, default 60s
	PlanFile string

	// Observability (FR-04). When set, copy serves Prometheus metrics here.
	MetricsAddr string

	// Operational.
	ConfigFile string
	LogLevel   LogLevel
	Quiet      bool
	Debug      bool

	// Consistency (FR-02).
	ConsistentDB      bool // default
	ConsistentCluster bool

	// Transport (T-B, DESIGN §1.3). CopyMode selects the source data path;
	// the on_segment fields configure the segment-direct fast path.
	CopyMode  CopyMode // default coordinator
	SegListen string   // controller listen address the segreceivers dial (host:port)
	SegBinary string   // gpstream-segreceiver path on the SOURCE segment hosts

	// Frame-level ACK and RST-resume (DESIGN §4.6, Tier-1, R2-approved).
	// FrameAck is "on" by default; "off" falls back to the plain pipe.
	// AckTimeout "adaptive" = 3x median RTT (min 1s), else a fixed duration.
	FrameAck              string // on (default) | off
	AckTimeout            string // 15s | 30s | adaptive (default 30s)
	MaxReconnects         int    // default 20
	RetransmitBufferMax   int64  // default 64 MiB (in-flight window; raise for long-haul WAN)

	// v2 conflict refinement (§6.3).
	// OverwriteOnlyConflicts refines Overwrite: only conflicting objects are
	// re-written, never the whole target (FR-11/§4.9).
	OverwriteOnlyConflicts bool

	// v2 diagnostics / checks (§6.10, §4.13, §4.14, §4.16).
	CollationCheck     bool // detect LC_COLLATE/collation differences
	LocaleCheck        bool // detect locale/encoding/timezone differences
	DistributionCheck  bool // detect distribution mismatch
	RedistributionPlan bool // surface redistribution in the plan
	AdaptiveRateLimit  bool // throttle per host-pair on a detected bottleneck

	// v2 checksum / dedup (§6.4).
	ChecksumAlgorithm  string // sha256|blake3 (default sha256)
	Dedup              bool   // skip units whose checksum matches the target
	SkipIfSameChecksum bool   // alias for Dedup

	// v2 observability / reports (§4.4, §6.9).
	MetricsOn  bool // enable the Prometheus endpoint
	ReportJSON string
	ReportCSV  string
	ReportText string

	// v4 §8: operation identity. The operation-id is the v4 handle for a job;
	// the state (journal mirror, staging, reports) lives under
	// <StateDir>/<operation-id>/ (default /var/lib/gpstream, v4 §8 "State").
	OperationID string
	StateDir    string
}

// Default returns a Config populated with the specification defaults.
func Default() Config {
	return Config{
		ConflictAction: SkipExisting,
		Compression:    CompZstd,
		ZstdLevel:      3,
		AutoResume:     0,
		Verify:         VerifySample,
		Timeout:        60 * time.Second,
		LogLevel:       LogInfo,
		SkewThreshold:  0.05,
		ConsistentDB:   true,
		IndexPriority:  "size",
		SSLMinTLS:      "1.2",
		WorkersPerPair: 2,
		StateDir:       "/var/lib/gpstream", // v4 §8 "State"
	}
}

// Validate reports configuration errors that are independent of the live
// clusters (e.g. mutually exclusive flags). It does not contact the DBs.
func (c Config) Validate() error {
	if c.ZstdLevel < 1 || c.ZstdLevel > 19 {
		return fmt.Errorf("zstd level must be 1-19, got %d", c.ZstdLevel)
	}
	if c.WorkersPerPair < 1 {
		return fmt.Errorf("workers-per-pair must be >= 1, got %d", c.WorkersPerPair)
	}
	if c.AutoResume < 0 {
		return fmt.Errorf("auto-resume must be >= 0")
	}
	switch c.ResumeConsistency {
	case "", "relaxed", "strict":
	default:
		return fmt.Errorf("unknown resume consistency %q (relaxed|strict)", c.ResumeConsistency)
	}
	if c.MetadataOnly && c.DataOnly {
		return fmt.Errorf("--metadata-only and --data-only are mutually exclusive")
	}
	if c.ConsistentDB && c.ConsistentCluster {
		return fmt.Errorf("--consistent-db and --consistent-cluster are mutually exclusive")
	}
	if c.NoRedistribute && c.SkewThreshold < 0 {
		return nil // fine
	}
	switch c.ConflictAction {
	case SkipExisting, Overwrite, Truncate, Append:
	default:
		return fmt.Errorf("unknown conflict action %q", c.ConflictAction)
	}
	switch c.Verify {
	case VerifyFull, VerifySample, VerifyOff:
	default:
		return fmt.Errorf("unknown verify mode %q", c.Verify)
	}
	switch c.CopyMode {
	case "", CopyCoordinator, CopyOnSegment, CopyDirect:
	default:
		return fmt.Errorf("unknown copy mode %q (want coordinator|on_segment|direct)", c.CopyMode)
	}
	if c.CopyMode == CopyOnSegment && c.SegBinary == "" {
		return fmt.Errorf("--copy-mode on_segment requires --seg-binary (gpstream-segreceiver path on the segment hosts)")
	}
	if c.CopyMode == CopyOnSegment && c.SegListen == "" {
		return fmt.Errorf("--copy-mode on_segment requires --seg-listen (controller listen address the segreceivers dial, e.g. 10.0.0.5:9432)")
	}
	// Frame-ack (DESIGN §4.6, R2): validate the knob and normalise the
	// defaults so the data path always sees a usable config.
	switch c.FrameAck {
	case "", "on", "off":
	default:
		return fmt.Errorf("unknown --frame-ack %q (want on|off)", c.FrameAck)
	}
	switch c.AckTimeout {
	case "":
		c.AckTimeout = "30s"
	case "adaptive":
	default:
		if _, err := time.ParseDuration(c.AckTimeout); err != nil {
			return fmt.Errorf("invalid --ack-timeout %q (want 15s|30s|adaptive)", c.AckTimeout)
		}
	}
	if c.MaxReconnects <= 0 {
		c.MaxReconnects = 20
	}
	if c.RetransmitBufferMax <= 0 {
		c.RetransmitBufferMax = 64 << 20
	}
	return nil
}

// FrameAckOn reports whether frame-level ACK/RST-resume is in effect
// (DESIGN §4.6: default on; --frame-ack off falls back to the plain pipe).
func (c *Config) FrameAckOn() bool { return c.FrameAck != "off" }

// ReliablePipeConfig builds the R2-approved frame-ack transport parameters
// from the CLI/env values (DESIGN §4.6).
func (c *Config) ReliablePipeConfig() transfer.ReliablePipeConfig {
	cfg := transfer.DefaultReliablePipeConfig()
	cfg.MaxReconnects = c.MaxReconnects
	cfg.RetransmitBufferMax = c.RetransmitBufferMax
	if c.AckTimeout == "adaptive" {
		cfg.AckTimeout = 30 * time.Second // fallback until RTT samples exist
		cfg.AdaptiveAckTimeout = true
	} else if d, err := time.ParseDuration(c.AckTimeout); err == nil && d > 0 {
		cfg.AckTimeout = d
	}
	return cfg
}

// ApplyEnv overlays environment variables (GPSTREAM_*) onto c. It is the
// env layer of the CLI > env > file > defaults precedence (FR-08): values
// already set by higher-precedence sources (CLI) are not clobbered.
func (c *Config) ApplyEnv() {
	get := func(k string) (string, bool) {
		v, ok := os.LookupEnv("GPSTREAM_" + k)
		return v, ok
	}
	if v, ok := get("RATE_LIMIT"); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.RateLimit = f
		}
	}
	if v, ok := get("WORKERS_PER_PAIR"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.WorkersPerPair = n
		}
	}
	if v, ok := get("COMPRESS"); ok {
		c.Compression = Compression(v)
	}
	if v, ok := get("LOG_LEVEL"); ok {
		c.LogLevel = LogLevel(v)
	}
	if v, ok := get("AUTO_RESUME"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.AutoResume = n
		}
	}
	// Secrets: only from env, never argv (FR-08).
	if v, ok := get("SSL_CERT"); ok {
		c.SSLCert = v
	}
	if v, ok := get("SSL_KEY"); ok {
		c.SSLKey = v
	}
	if v, ok := get("SSL_CA"); ok {
		c.SSLCA = v
	}
	// v2 flags (§6.3/§6.4/§6.6/§6.7/§6.9/§6.10/§6.11).
	if v, ok := get("OVERWRITE_ONLY_CONFLICTS"); ok {
		c.OverwriteOnlyConflicts = v == "true" || v == "1"
	}
	if v, ok := get("COLLATION_CHECK"); ok {
		c.CollationCheck = v == "true" || v == "1"
	}
	if v, ok := get("LOCALE_CHECK"); ok {
		c.LocaleCheck = v == "true" || v == "1"
	}
	if v, ok := get("DISTRIBUTION_CHECK"); ok {
		c.DistributionCheck = v == "true" || v == "1"
	}
	if v, ok := get("REDISTRIBUTION_PLAN"); ok {
		c.RedistributionPlan = v == "true" || v == "1"
	}
	if v, ok := get("ADAPTIVE_RATE_LIMIT"); ok {
		c.AdaptiveRateLimit = v == "true" || v == "1"
	}
	if v, ok := get("CHECKSUM_ALGORITHM"); ok {
		c.ChecksumAlgorithm = v
	}
	if v, ok := get("DEDUP"); ok {
		c.Dedup = v == "true" || v == "1"
	}
	if v, ok := get("METRICS"); ok {
		c.MetricsOn = v == "true" || v == "1"
	}
	if v, ok := get("REPORT_JSON"); ok {
		c.ReportJSON = v
	}
	if v, ok := get("REPORT_CSV"); ok {
		c.ReportCSV = v
	}
	if v, ok := get("REPORT_TEXT"); ok {
		c.ReportText = v
	}
	// Frame-ack (§4.6).
	if v, ok := get("FRAME_ACK"); ok {
		c.FrameAck = v
	}
	if v, ok := get("ACK_TIMEOUT"); ok {
		c.AckTimeout = v
	}
	if v, ok := get("MAX_RECONNECTS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxReconnects = n
		}
	}
	if v, ok := get("RETRANSMIT_BUFFER_MAX"); ok {
		c.RetransmitBufferMax = parseSizeEnv(v)
	}
}

// parseSizeEnv parses a GPSTREAM_* size value ("512M", "1G", "1024").
func parseSizeEnv(s string) int64 {
	s = strings.TrimSpace(s)
	mult := int64(1)
	if s != "" {
		switch strings.ToLower(string(s[len(s)-1])) {
		case "k":
			mult = 1 << 10
			s = s[:len(s)-1]
		case "m":
			mult = 1 << 20
			s = s[:len(s)-1]
		case "g":
			mult = 1 << 30
			s = s[:len(s)-1]
		case "t":
			mult = 1 << 40
			s = s[:len(s)-1]
		}
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n * mult
}

// LoadFile reads a config file — YAML or TOML, selected by extension
// (FR-05: /etc/gpstream/gpstream.toml) — and overlays known keys onto c.
// Unknown keys are ignored. It is the lowest-precedence non-default source:
// CLI > env > file > defaults.
func (c *Config) LoadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var m map[string]any
	switch strings.ToLower(filepath.Ext(path)) {
	case ".toml":
		if err := toml.Unmarshal(data, &m); err != nil {
			return err
		}
	default: // .yaml / .yml / no extension
		if err := yaml.Unmarshal(data, &m); err != nil {
			return err
		}
	}
	for k, v := range m {
		s, ok := leafString(v)
		if !ok {
			continue // only scalar leaves are honoured here
		}
		assignConfigString(c, strings.ToLower(strings.TrimSpace(k)), s)
	}
	return nil
}

// leafString renders a scalar config leaf (string/number/bool) into string
// form. Tables and arrays are not honoured (unknown-key tolerance).
func leafString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case int:
		return strconv.Itoa(t), true
	case int64:
		return strconv.FormatInt(t, 10), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(t), true
	}
	return "", false
}

// assignConfigString maps a lowercased YAML key to a string-typed Config field.
func assignConfigString(c *Config, key, v string) {
	switch key {
	case "source":
		c.Source = v
	case "dest":
		c.Dest = v
	case "rate_limit":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.RateLimit = f
		}
	case "workers_per_pair":
		if n, err := strconv.Atoi(v); err == nil {
			c.WorkersPerPair = n
		}
	case "compress":
		c.Compression = Compression(v)
	case "zstd_level":
		if n, err := strconv.Atoi(v); err == nil {
			c.ZstdLevel = n
		}
	case "auto_resume":
		if n, err := strconv.Atoi(v); err == nil {
			c.AutoResume = n
		}
	case "resume_consistency":
		c.ResumeConsistency = v
	case "verify":
		c.Verify = VerifyMode(v)
	case "log_level":
		c.LogLevel = LogLevel(v)
	case "ssl_cert":
		c.SSLCert = v
	case "ssl_key":
		c.SSLKey = v
	case "ssl_ca":
		c.SSLCA = v
	case "ssl_min_tls":
		c.SSLMinTLS = v
	case "data_port_range":
		c.DataPortRange = v
	case "skew_threshold":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.SkewThreshold = f
		}
	case "index_priority":
		c.IndexPriority = v
	case "timeout":
		if d, err := time.ParseDuration(v); err == nil {
			c.Timeout = d
		}
	case "overwrite_only_conflicts":
		c.OverwriteOnlyConflicts = v == "true" || v == "1"
	case "collation_check":
		c.CollationCheck = v == "true" || v == "1"
	case "locale_check":
		c.LocaleCheck = v == "true" || v == "1"
	case "distribution_check":
		c.DistributionCheck = v == "true" || v == "1"
	case "redistribution_plan":
		c.RedistributionPlan = v == "true" || v == "1"
	case "adaptive_rate_limit":
		c.AdaptiveRateLimit = v == "true" || v == "1"
	case "checksum_algorithm":
		c.ChecksumAlgorithm = v
	case "dedup":
		c.Dedup = v == "true" || v == "1"
	case "metrics":
		c.MetricsOn = v == "true" || v == "1"
	case "report_json":
		c.ReportJSON = v
	case "report_csv":
		c.ReportCSV = v
	case "report_text":
		c.ReportText = v
	}
}

// ParsePorts extracts the low/high endpoints of an "a-b" range string.
func (c Config) ParsePorts() (int, int, error) {
	if c.DataPortRange == "" {
		return 0, 0, fmt.Errorf("data-port-range is empty")
	}
	parts := strings.SplitN(c.DataPortRange, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("data-port-range must be a-b, got %q", c.DataPortRange)
	}
	lo, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("data-port-range low: %w", err)
	}
	hi, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("data-port-range high: %w", err)
	}
	if lo > hi {
		return 0, 0, fmt.Errorf("data-port-range low > high")
	}
	return lo, hi, nil
}
