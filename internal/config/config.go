// Package config holds the gpstream configuration model: defaults, CLI flag
// binding (contract from §5 of the specification), YAML file loading and
// environment overrides. Secrets are never accepted from argv (FR-08); they
// come only from 0600 files or environment variables.
package config

import (
	"fmt"
	"net/url"
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
// exist in the destination (FR-10/FR-11), selected by --resolve-conflicts:
//
//	Stop (the DEFAULT) -> STRICT: the run STOPS on a conflict so the operator
//	  decides (re-run with another policy or a rename). Nothing is clobbered.
//	Skip        -> the conflicting unit is skipped and the run continues.
//	Overwrite   -> copy to a staging table and swap over the final;
//	  additionally skips tables whose checksum already matches.
//	Truncate    -> copy in-place into the final table after TRUNCATE, verify.
type ConflictAction string

const (
	// Stop is the default conflict policy (FR-10 §8): the run stops on the
	// first collision and the operator decides.
	Stop      ConflictAction = "stop"
	Skip      ConflictAction = "skip"
	Overwrite ConflictAction = "overwrite"
	Truncate  ConflictAction = "truncate"
)

// IncludeClasses resolves the effective object classes for a run (v4 F-11):
// --metadata-only (structure without data) / --data-only (into the existing
// structure); with neither flag both classes are copied.
func (c *Config) IncludeClasses() (metadata, data bool) {
	if c.MetadataOnly {
		return true, false
	}
	if c.DataOnly {
		return false, true
	}
	return true, true
}

// VerifyMode controls the verification depth (FR-02).
type VerifyMode string

const (
	VerifyFull   VerifyMode = "full"
	VerifySample VerifyMode = "sample"
	VerifyOff    VerifyMode = "off"
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
	// Metadata/data (FR-11).
	MetadataOnly bool
	DataOnly     bool

	// Conflict / safety (FR-10/FR-11).
	ConflictAction ConflictAction // default Stop (strict)

	// Rename (FR-11).
	RenameDB     map[string]string // src -> dst
	RenameSchema map[string]string
	RenameTable  map[string]string // src -> dst (v2 §4.10)

	// Performance (FR-01/FR-03/FR-12/FR-14).
	Compression Compression // default zstd
	ZstdLevel   int         // 1-19, default 3
	// SmallTableThreshold is the F-12 auto-disable threshold (bytes): a table
	// whose estimated size (pg_total_relation_size) is below it is copied with
	// compression OFF, because the per-stream compressor overhead does not pay
	// back on tiny payloads. 0 disables the auto-disable. Default 1 MiB.
	SmallTableThreshold int64
	RateLimit           float64 // Gbps total ceiling
	WorkersPerPair      int

	// Resilience (FR-06/FR-07).
	AutoResume int // N >= 0; 0 = manual only
	// OnUnitFailure selects what the run does when a unit fails (spec F-06):
	// "continue" (default) records the failure, lets the rest of the units
	// finish, and hands the failed units to `gpstream resume`; "abort" stops
	// the run at the first failure (in-flight units finish, none new starts).
	OnUnitFailure string     // continue (default) | abort
	Verify        VerifyMode // default sample
	// ResumeConsistency selects the resume re-read semantics (T5, DESIGN
	// §2.2): "relaxed" (default) keeps a table verified in one snapshot
	// epoch; "strict" re-reads every table verified under an older snapshot.
	ResumeConsistency string // relaxed|strict, default relaxed

	// Indexes (FR-17).
	IndexWorkers  int
	NoIndexes     bool
	IndexPriority string // "size" | "fifo"

	// NoConstraints disables the F-11 constraint-restore pass (foreign keys
	// and user CHECK constraints) on the destination. Off by default: the
	// pass is non-fatal and idempotent, so keeping metadata complete is the
	// safer default.
	NoConstraints bool

	// Redistribution (FR-15).
	NoRedistribute bool
	SkewThreshold  float64

	// TLS (FR-13). PG connections are always sslmode=verify-full unless the
	// DSN sets another mode explicitly; the keypair/CA below provide the
	// client certificate and trust store for that handshake.
	SSLCert   string
	SSLKey    string
	SSLCA     string
	SSLMinTLS string // e.g. "1.2"

	// F-13 credential provenance (advisory): SourceFromFile/DestFromFile are
	// set when the DSN was loaded from the config FILE (LoadFile) and cleared
	// when the CLI overrides it. A DSN carrying a password that came from the
	// file triggers a SecurityWarnings() note (F-13: credentials belong in
	// env/.pgpass, not in a plaintext config).
	SourceFromFile bool `toml:"-"`
	DestFromFile   bool `toml:"-"`

	// Dry-run / planning (FR-10).
	DryRun   bool
	Timeout  time.Duration // plan formation, default 60s
	PlanFile string

	// Observability (FR-04). When set, copy serves Prometheus metrics here.
	MetricsAddr string

	// Operational.
	ConfigFile string

	// Consistency (FR-02). The three consistency scopes are mutually
	// exclusive: ConsistentDB (default) pins one long-running
	// REPEATABLE READ snapshot per database; ConsistentCluster adds a
	// cluster-wide freeze; ConsistentTables is the explicit per-table
	// fallback (each table under its own short snapshot — the documented
	// discrepancy between tables is the trade-off, spec F-02).
	ConsistentDB      bool // default
	ConsistentCluster bool
	ConsistentTables  bool // explicit fallback flag (spec F-02)

	// Transport (T-B, DESIGN §1.3). CopyMode selects the source data path;
	// the on_segment fields configure the segment-direct fast path.
	CopyMode  CopyMode // default coordinator
	SegListen string   // controller listen address the segreceivers dial (host:port)
	SegBinary string   // gpstream-segreceiver path on the SOURCE segment hosts

	// Data-plane TLS for the segment-direct (on_segment) hop (F-13 «TLS
	// between sender/receiver»). Deliberately SEPARATE from the SSL*
	// (PG-connection) fields: a cluster can have no TLS on its PG connections
	// (this testbed: sslmode=disable) while the inter-host segment stream is
	// still encrypted. Opt-in: empty = plaintext (the pre-F-13 behavior).
	// SegTLSCert+SegTLSKey are the controller's serving keypair; SegTLSCA
	// additionally makes the controller require a client certificate (mTLS)
	// and is the trusted CA bundle the segreceivers validate the controller
	// against.
	SegTLSCert string
	SegTLSKey  string
	SegTLSCA   string

	// Frame-level ACK and RST-resume (DESIGN §4.6, Tier-1, R2-approved).
	// FrameAck is "on" by default; "off" falls back to the plain pipe.
	// AckTimeout "adaptive" = 3x median RTT (min 1s), else a fixed duration.
	FrameAck            string // on (default) | off
	AckTimeout          string // 15s | 30s | adaptive (default 30s)
	MaxReconnects       int    // default 20
	RetransmitBufferMax int64  // default 64 MiB (in-flight window; raise for long-haul WAN)

	// LocaleCheck enables the dry-run locale/encoding summary (FR-16):
	// LC_COLLATE/LC_TYPE/encoding differences are already detected by the
	// planner; the flag controls the extra dry-run prediction block.
	LocaleCheck bool

	// v4 §8: operation identity. The operation-id is the v4 handle for a job;
	// the state (journal mirror, staging, reports) lives under
	// <StateDir>/<operation-id>/ (default /var/lib/gpstream, v4 §8 "State").
	OperationID string
	StateDir    string
}

// Default returns a Config populated with the specification defaults.
func Default() Config {
	return Config{
		ConflictAction:      Stop,
		Compression:         CompZstd,
		SmallTableThreshold: 1 << 20, // 1 MiB (F-12 auto-disable default)
		ZstdLevel:           3,
		AutoResume:          0,
		Verify:              VerifySample,
		Timeout:             120 * time.Second, // F-10 §6: default 120 s
		SkewThreshold:       0.05,
		ConsistentDB:        true,
		IndexPriority:       "size",
		SSLMinTLS:           "1.2",
		WorkersPerPair:      2,
		StateDir:            "/var/lib/gpstream", // v4 §8 "State"
	}
}

// Validate reports configuration errors that are independent of the live
// clusters (e.g. mutually exclusive flags) and NORMALISES the defaults
// (ack-timeout, reconnect budget) in place. It does not contact the DBs.
//
// Pointer receiver (was value): the normalisations below must reach the
// caller — parseConfig used to discard the normalised copy, silently
// running frame-ack with a zero ack-timeout/reconnect budget.
func (c *Config) Validate() error {
	if c.ZstdLevel < 1 || c.ZstdLevel > 19 {
		return fmt.Errorf("zstd level must be 1-19, got %d", c.ZstdLevel)
	}
	if c.SmallTableThreshold < 0 {
		return fmt.Errorf("small-table-threshold must be >= 0 (0 disables auto-disable), got %d", c.SmallTableThreshold)
	}
	if c.WorkersPerPair < 1 {
		return fmt.Errorf("workers must be >= 1, got %d", c.WorkersPerPair)
	}
	if c.AutoResume < 0 {
		return fmt.Errorf("auto-resume must be >= 0")
	}
	switch c.OnUnitFailure {
	case "", "continue", "abort":
	default:
		return fmt.Errorf("unknown on-unit-failure %q (want continue|abort)", c.OnUnitFailure)
	}
	switch c.IndexPriority {
	case "", "size", "fifo":
	default:
		return fmt.Errorf("unknown index-priority %q (want size|fifo)", c.IndexPriority)
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
	if c.ConsistentTables && c.ConsistentCluster {
		return fmt.Errorf("--consistent-tables and --consistent-cluster are mutually exclusive")
	}
	if c.ConsistentTables && c.ConsistentDB {
		return fmt.Errorf("--consistent-tables and --consistent-db are mutually exclusive")
	}
	// (NoRedistribute sets the skew sentinel -1 by itself; a negative
	// skew threshold is valid on its own, so there is nothing to special-
	// case here — do NOT return early, the checks below still apply.)
	switch c.ConflictAction {
	case Stop, Skip, Overwrite, Truncate:
	default:
		return fmt.Errorf("unknown --resolve-conflicts value %q (want stop|skip|overwrite|truncate)", c.ConflictAction)
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
	if v, ok := get("WORKERS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.WorkersPerPair = n
		}
	}
	if v, ok := get("COMPRESS"); ok {
		c.Compression = Compression(v)
	}
	if v, ok := get("SMALL_TABLE_THRESHOLD"); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			c.SmallTableThreshold = n
		}
	}
	if v, ok := get("AUTO_RESUME"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.AutoResume = n
		}
	}
	if v, ok := get("ON_UNIT_FAILURE"); ok {
		c.OnUnitFailure = v
	}
	if v, ok := get("CONSISTENT_TABLES"); ok {
		c.ConsistentTables = v == "true" || v == "1"
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
	// Data-plane TLS for the on_segment hop (F-13), separate from the PG
	// SSL_* vars: the segment stream can be encrypted even when the PG
	// connections are not.
	if v, ok := get("SEG_TLS_CERT"); ok {
		c.SegTLSCert = v
	}
	if v, ok := get("SEG_TLS_KEY"); ok {
		c.SegTLSKey = v
	}
	if v, ok := get("SEG_TLS_CA"); ok {
		c.SegTLSCA = v
	}
	if v, ok := get("LOCALE_CHECK"); ok {
		c.LocaleCheck = v == "true" || v == "1"
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
		key := strings.ToLower(strings.TrimSpace(k))
		assignConfigString(c, key, s)
		// F-13 provenance: a DSN that arrived from the file is tracked so a
		// password in it can be flagged (the CLI can clear it below).
		switch key {
		case "source":
			c.SourceFromFile = true
		case "dest":
			c.DestFromFile = true
		}
	}
	return nil
}

// dsnHasPassword reports whether a libpq/URL connection string carries an
// embedded password ("password=...", "user:pass@host", ?password=). It is a
// best-effort heuristic for the F-13 advisory (not a credential parser).
func dsnHasPassword(dsn string) bool {
	if dsn == "" {
		return false
	}
	if strings.Contains(dsn, "password=") || strings.Contains(dsn, "password%3D") {
		return true
	}
	// URL form: postgres://user:password@host — a colon inside the userinfo
	// segment (before the next '/') means a password is present.
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if _, has := u.User.Password(); has {
			return true
		}
	}
	return false
}

// SecurityWarnings returns F-13/F-08 advisory notes about the effective
// configuration (never errors). Called by the CLI once config is final, so
// the operator sees them on the console before any data moves:
//
//   - a DSN carrying a password that came from the config FILE (credentials
//     belong in env/.pgpass, not plaintext config) — F-13,
//   - the config file is world/group-readable (wider than 0600) — F-13
//     "config file ... mode 0600".
func (c Config) SecurityWarnings() []string {
	var out []string
	if c.ConfigFile != "" {
		if st, err := os.Stat(c.ConfigFile); err == nil {
			if mode := st.Mode().Perm() & 0o077; mode != 0 {
				out = append(out, fmt.Sprintf(
					"config file %s is mode %03o (group/other readable); F-13 wants 0600 (chmod 600)",
					c.ConfigFile, st.Mode().Perm()))
			}
		}
	}
	if c.SourceFromFile && dsnHasPassword(c.Source) {
		out = append(out, "source DSN carries a password from the config file; F-13 prefers env/.pgpass over plaintext config")
	}
	if c.DestFromFile && dsnHasPassword(c.Dest) {
		out = append(out, "dest DSN carries a password from the config file; F-13 prefers env/.pgpass over plaintext config")
	}
	return out
}

// AdvisoryWarnings reports non-security configuration notes (advisory only,
// never an error). Unlike SecurityWarnings it covers operational clarity:
//
//   - --no-indexes with explicit --index-workers / --index-priority: the
//     index build is OFF, so the tuning flags are ignored. They are NOT
//     mutually exclusive (IndexWorkers defaults to 2 and IndexPriority to
//     "size" on every run, so a hard error would break the default
//     --no-indexes runbook), but silently-ignored tuning is worth surfacing
//     so the operator sees it.
func (c Config) AdvisoryWarnings() []string {
	var out []string
	if c.NoIndexes {
		var tuned []string
		if c.IndexWorkers > 0 {
			tuned = append(tuned, "--index-workers")
		}
		if c.IndexPriority != "" && c.IndexPriority != "size" {
			tuned = append(tuned, "--index-priority")
		}
		if len(tuned) > 0 {
			part, pron := "is ignored", "remove it"
			if len(tuned) > 1 {
				part, pron = "are ignored", "remove them"
			}
			out = append(out, "--no-indexes is set, so index building is off; "+
				strings.Join(tuned, " and ")+" "+part+" ("+pron+" or drop --no-indexes)")
		}
	}
	return out
}

// parseSizeValue parses a plain byte count or a suffixed size ("1M", "512K",
// "2G", "1T", "1024") so the config FILE accepts the same values as the CLI's
// --small-table-threshold (parseBytes). It mirrors parseBytes: a trailing
// k/m/g/t unit letter is a multiplier, empty yields 0.
func parseSizeValue(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	if len(s) > 1 {
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
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
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
	case "workers":
		if n, err := strconv.Atoi(v); err == nil {
			c.WorkersPerPair = n
		}
	case "compress":
		c.Compression = Compression(v)
	case "zstd_level":
		if n, err := strconv.Atoi(v); err == nil {
			c.ZstdLevel = n
		}
	case "small_table_threshold":
		if n, err := parseSizeValue(v); err == nil {
			c.SmallTableThreshold = n
		}
	case "auto_resume":
		if n, err := strconv.Atoi(v); err == nil {
			c.AutoResume = n
		}
	case "on_unit_failure":
		c.OnUnitFailure = v
	case "consistent_tables":
		c.ConsistentTables = v == "true" || v == "1"
	case "resume_consistency":
		c.ResumeConsistency = v
	case "verify":
		c.Verify = VerifyMode(v)
	case "ssl_cert":
		c.SSLCert = v
	case "ssl_key":
		c.SSLKey = v
	case "ssl_ca":
		c.SSLCA = v
	case "ssl_min_tls":
		c.SSLMinTLS = v
	// Data-plane TLS for the on_segment hop (F-13), separate from the
	// PG-connection ssl_* keys.
	case "seg_tls_cert":
		c.SegTLSCert = v
	case "seg_tls_key":
		c.SegTLSKey = v
	case "seg_tls_ca":
		c.SegTLSCA = v
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
	case "locale_check":
		c.LocaleCheck = v == "true" || v == "1"
	}
}
