// Package config holds the gpstream configuration model: defaults, CLI flag
// binding (contract from §5 of the specification), YAML file loading and
// environment overrides. Secrets are never accepted from argv (FR-08); they
// come only from 0600 files or environment variables.
package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
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

// VerifyMode controls content verification after a unit lands (FR-02).
type VerifyMode string

const (
	VerifyOn  VerifyMode = "on"
	VerifyOff VerifyMode = "off"
)

// CopyMode selects the source data-path transport (DESIGN §1.3 / T-B). The
// operator can override the doctor-detected capability with --copy-mode.
type CopyMode string

const (
	// CopyCoordinator is the default: COPY ... TO STDOUT through the source
	// coordinator (always works, single network path).
	CopyCoordinator CopyMode = "coordinator"
	// CopyMesh is the segment-direct data path (design 04.10): the
	// kernel launches gpstream-segreceiver on every primary source segment
	// via COPY ... TO PROGRAM ... ON SEGMENT; the slices stream compressed
	// over the WAN to the destination agents, which load per-segment (1:1 /
	// routed) or merge through the destination coordinator (merge) — the
	// sub-path is chosen per table by the planner.
	CopyMesh CopyMode = "mesh"
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
	// MaxSnapshotDelta (P1d) bounds the snapshot-epoch pin spread for
	// --workers N > 1, in SECONDS: the N REPEATABLE READ pins are taken in
	// one ε-window and their spread (max(t_fix) − min(t_fix)) must be <=
	// this limit or the run refuses to start (a doctor error carrying the
	// actual delta). 0 disables the check. Default 1s (the live pin spread
	// is ~ms, a 3-orders-of-magnitude margin). strict mode (--workers 1)
	// never pins an epoch, so this has no effect there.
	MaxSnapshotDelta time.Duration

	// Resilience (FR-06/FR-07).
	AutoResume int // N >= 0; 0 = manual only
	// OnUnitFailure selects what the run does when a unit fails (spec F-06):
	// "continue" (default) records the failure, lets the rest of the units
	// finish, and hands the failed units to `gpstream resume`; "abort" stops
	// the run at the first failure (in-flight units finish, none new starts).
	OnUnitFailure string     // continue (default) | abort
	Verify        VerifyMode // default off

	// Indexes (FR-17).
	IndexWorkers  int
	NoIndexes     bool
	IndexPriority string // "size" | "fifo"

	// NoRoutedWrite (P9) disables the routed segment-to-segment data path for
	// S≠D topologies: on_segment then always merges through the destination
	// coordinator (SliceMux) instead of routing in the segreceivers.
	NoRoutedWrite bool
	// NoConstraints disables the F-11 constraint-restore pass (foreign keys
	// and user CHECK constraints) on the destination. Off by default: the
	// pass is non-fatal and idempotent, so keeping metadata complete is the
	// safer default.
	NoConstraints bool

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
	DryRun  bool
	Timeout time.Duration // plan formation, default 60s
	// benchTimeoutExplicit is true when --timeout was passed explicitly on the
	// command line: for the input-stream benchmark (bench) it then means the
	// benchmark wall-time limit (unlimited when not passed). run/dry-run keep
	// their plan-phase default. Not mapped from file/env (CLI-only).
	benchTimeoutExplicit bool

	// Observability (FR-04). When set, copy serves Prometheus metrics here.
	MetricsAddr string

	// Operational.
	ConfigFile string

	// Consistency (FR-02): the source agent always reads under one
	// run-wide REPEATABLE READ snapshot per database (the DB scope). P7
	// removed the --consistent-db/--consistent-cluster/--consistent-tables
	// scopes: the two-agent data path has a single snapshot mode.

	// Transport (T-B, DESIGN §1.3). CopyMode selects the source data path;
	// the on_segment fields configure the segment-direct fast path.
	CopyMode  CopyMode // default coordinator
	SegListen string   // base listen address the segreceivers dial (host:port); empty = auto (controller primary IP + free port, resolved at run start)
	SegBinary string   // gpstream-segreceiver path on the SOURCE segment hosts; empty = convention (sibling of the gpstream binary, then PATH)
	// PortRange restricts the auto data/control port allocation to an
	// allowed range "LO-HI" (or a single "LO" base) for environments with a
	// strict firewall (the operator opens exactly that range). Empty = fully
	// automatic: every long-lived agent binds :0 and reports its real address
	// in the control handshake. When set, the auto-picked (":0") listener ports
	// are restricted to the range (the controller pre-binds inside the range for
	// same-host agents and passes --port-range to remote agents) and the range
	// size is checked against the run's requirement (3 ports: source control +
	// dest data + dest control, both modes) at run/dry-run time.
	PortRange string // "LO-HI" or "LO" (base); empty = auto :0 + handshake
	PortLo    int    // parsed low end of PortRange (0 = automatic)
	PortHi    int    // parsed high end of PortRange (0 = automatic)

	// The data plane ALWAYS runs in the source/dest agents (ADR-002, P7): the
	// controller orchestrates a source agent and a dest agent (both launched
	// by the controller on ITS host) over a real, compressed, optionally-TLS
	// socket and holds no data connections. The agent binaries resolve by
	// convention (sibling of the gpstream binary, then PATH) so the packaged
	// install needs no flags; set via GPSTREAM_SOURCE_AGENT_BINARY /
	// GPSTREAM_DEST_AGENT_BINARY / GPSTREAM_SEG_BINARY (or the config file)
	// to override (dev builds, tarball installs).
	SourceAgentBinary string // gpstream-source-agent path; empty = convention
	DestAgentBinary   string // gpstream-dest-agent path; empty = convention

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

	// ControlTLS is the controller's control-channel TLS client settings
	// (design 04.10: the control channel crosses the WAN in the two-agent
	// layout). The controller verifies each agent's control-socket
	// certificate against ControlTLSCA (the agents serve with
	// --control-tls-cert/key; their --control-tls-ca enables mTLS, in which
	// case the controller presents ControlTLSCert/ControlTLSKey).
	ControlTLSCert string
	ControlTLSKey  string
	ControlTLSCA   string

	// Remote agent launch (design 04.10; P1e: CLI-only). An agent must run
	// where it opens its pg connection (source master / destination
	// coordinator). Each flag may be OMITTED (or name a local host): then the
	// agent runs as a local subprocess on the gpstream host — correct when
	// gpstream itself runs on that cluster's master. A remote host = SSH
	// launch there (key-based, default ~/.ssh keys, the SSH user). Either way
	// the agent's coordinator-check (CheckOnCoordinator) enforces the
	// topology: a non-master / non-local pg host is a hard error.
	SourceAgentSSH string // "user@host[:port]" (or local "host") of the source agent (source master); "" = local
	DestAgentSSH   string // "user@host[:port]" (or local "host") of the dest agent (dest coordinator); "" = local

	// Frame-level ACK and RST-resume (DESIGN §4.6, Tier-1, R2-approved).
	// FrameAck is "on" by default; "off" falls back to the plain pipe.
	// AckTimeout "adaptive" = 3x median RTT (min 1s), else a fixed duration.
	FrameAck            string // on (default) | off
	AckTimeout          string // 15s | 30s | adaptive (default 30s)
	MaxReconnects       int    // default 20
	RetransmitBufferMax int64  // default 64 MiB (in-flight window; raise for long-haul WAN)

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
		Verify:              VerifyOff,
		Timeout:             120 * time.Second, // F-10 §6: default 120 s
		IndexPriority:       "size",
		SSLMinTLS:           "1.2",
		WorkersPerPair:      1,                   // P1d: default 1 = strict (one exact snapshot point; N>1 = snapshot-epoch)
		MaxSnapshotDelta:    1 * time.Second,     // P1d: snapshot-epoch pin-spread limit (seconds)
		StateDir:            "/var/lib/gpstream", // v4 §8 "State"
	}
}

// MarkTimeoutExplicit records an explicit --timeout from the CLI. The CLI
// layer calls it when the flag is present; the file/env layers set Timeout
// directly and leave the marker false.
func (c *Config) MarkTimeoutExplicit(d time.Duration) {
	c.Timeout = d
	c.benchTimeoutExplicit = true
}

// BenchTimeout returns the wall-time limit for the input-stream benchmark:
// the explicit --timeout when the operator passed it, 0 (no limit) otherwise.
// A benchmark is a measurement, not a plan: capping it by default would
// truncate the very number it reports.
func (c *Config) BenchTimeout() time.Duration {
	if c.benchTimeoutExplicit {
		return c.Timeout
	}
	return 0
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
	if c.MaxSnapshotDelta < 0 {
		return fmt.Errorf("max-snapshot-delta must be >= 0 seconds (0 disables the check), got %s", c.MaxSnapshotDelta)
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
	if c.MetadataOnly && c.DataOnly {
		return fmt.Errorf("--metadata-only and --data-only are mutually exclusive")
	}
	switch c.ConflictAction {
	case Stop, Skip, Overwrite, Truncate:
	default:
		return fmt.Errorf("unknown --resolve-conflicts value %q (want stop|skip|overwrite|truncate)", c.ConflictAction)
	}
	switch c.Verify {
	case VerifyOn, VerifyOff:
	default:
		return fmt.Errorf("unknown verify mode %q", c.Verify)
	}
	switch c.CopyMode {
	case "", CopyCoordinator, CopyMesh:
	default:
		return fmt.Errorf("unknown copy mode %q (want coordinator|mesh)", c.CopyMode)
	}
	// on_segment: SegBinary/SegListen may stay empty — they resolve by
	// convention at run start (ResolveAgentBinaries / ResolveSegListen) and
	// fail loud there if the binary is not installed. An explicitly given
	// --seg-listen must already be a usable host:port (UnitListen adds
	// unitID to the port per unit).
	if c.SegListen != "" {
		h, p, err := net.SplitHostPort(c.SegListen)
		if err != nil || h == "" {
			return fmt.Errorf("--seg-listen %q must be host:port", c.SegListen)
		}
		pn, err := strconv.Atoi(p)
		if err != nil || pn < 1 || pn > 65535 {
			return fmt.Errorf("--seg-listen %q must be host:port (numeric port 1-65535)", c.SegListen)
		}
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
	if c.PortRange != "" {
		lo, hi, err := parsePortRange(c.PortRange)
		if err != nil {
			return fmt.Errorf("invalid --port-range %q: %w", c.PortRange, err)
		}
		c.PortLo, c.PortHi = lo, hi
	}
	return nil
}

// parsePortRange parses "LO-HI" (a range) or "LO" (a single base port) into
// (lo, hi). Both ends must be valid TCP ports.
func parsePortRange(s string) (int, int, error) {
	s = strings.TrimSpace(s)
	dash := strings.Index(s, "-")
	loStr, hiStr := s, s
	if dash >= 0 {
		loStr, hiStr = s[:dash], s[dash+1:]
	}
	lo, err := strconv.Atoi(strings.TrimSpace(loStr))
	if err != nil || lo < 1 || lo > 65535 {
		return 0, 0, fmt.Errorf("low port %q must be 1-65535", loStr)
	}
	hi, err := strconv.Atoi(strings.TrimSpace(hiStr))
	if err != nil || hi < 1 || hi > 65535 {
		return 0, 0, fmt.Errorf("high port %q must be 1-65535", hiStr)
	}
	if hi < lo {
		return 0, 0, fmt.Errorf("high port %d < low port %d", hi, lo)
	}
	return lo, hi, nil
}

// PortRangeSize returns the number of ports in the configured range (0
// when the range is empty = automatic).
func (c *Config) PortRangeSize() int {
	if c.PortLo == 0 {
		return 0
	}
	return c.PortHi - c.PortLo + 1
}

// RequiredDataPorts returns how many data/control ports the copy mode needs
// from the --port-range (the in-range allocation the agents bind on).
//
//   - coordinator (and the mesh coordinator-fallback for small/replicated
//     tables): exactly **3** — source control + dest data + dest control. The
//     per-segment segreceivers are dialers that all funnel into the dest
//     agent's single shared data listener, so the destination segment count
//     adds no ports (the pre-04.10 "one dest agent per segment" 2D+1 design
//     was removed with the port-economy redesign).
//
//   - mesh (P2, direct_1to1): **4 + D** — source control + dest control +
//     dest report + dest data + D mini-agent data ports (one in-range data
//     port per destination segment, each mini-agent binds its own). The
//     source report-listener was removed (its digest rides the slice trailer),
//     so it is not counted. PG ports on the segments are NOT in the range.
func (c *Config) RequiredDataPorts(dstSegments int) int {
	if c.CopyMode == CopyMesh {
		return 4 + dstSegments
	}
	return 3
}

// CheckPortRange verifies the configured range (if any) is large enough
// for the copy mode's requirement. Callers pass the destination segment
// count (from the catalog, so it works at dry-run time). Returns a clear
// error naming the required vs available count.
func (c *Config) CheckPortRange(dstSegments int) error {
	if c.PortLo == 0 {
		return nil // automatic: nothing to check
	}
	need := c.RequiredDataPorts(dstSegments)
	have := c.PortRangeSize()
	if have < need {
		if c.CopyMode == CopyMesh {
			return fmt.Errorf("--port-range %s has %d port(s) but this mesh run needs %d (4 + D: source control + dest control + dest report + dest data + %d mini-agent data ports, one per destination segment); widen the range",
				c.PortRange, have, need, dstSegments)
		}
		return fmt.Errorf("--port-range %s has %d port(s) but this run needs %d (source control + dest data + dest control — the on_segment segreceivers dial the shared data port, so the segment count adds no ports); widen the range",
			c.PortRange, have, need)
	}
	return nil
}

// FrameAckOn reports whether frame-level ACK/RST-resume is in effect
// (DESIGN §4.6: default on; --frame-ack off falls back to the plain pipe).
func (c *Config) FrameAckOn() bool { return c.FrameAck != "off" }

// frameAckArg is the agent launch-arg value for --frame-ack ("on" when in
// effect, "off" for the plain-pipe fallback).
func (c *Config) FrameAckArg() string {
	if c.FrameAckOn() {
		return "on"
	}
	return "off"
}

// ReliablePipeConfig builds the R2-approved frame-ack transport parameters
// from the CLI/env values (DESIGN §4.6).
func (c *Config) ReliablePipeConfig() transfer.ReliablePipeConfig {
	cfg := transfer.DefaultReliablePipeConfig()
	// Guard zeros: Validate() (which applies the MaxReconnects /
	// RetransmitBufferMax defaults) may not have run — e.g. an in-proc test
	// builds config.Default() directly. A zero RetransmitBufferMax would
	// cap the receiver's output window at 0 (deadlock: enqueue back-pressures
	// forever, Read sees no data).
	if c.MaxReconnects > 0 {
		cfg.MaxReconnects = c.MaxReconnects
	}
	if c.RetransmitBufferMax > 0 {
		cfg.RetransmitBufferMax = c.RetransmitBufferMax
	}
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
	if v, ok := get("MAX_SNAPSHOT_DELTA"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxSnapshotDelta = time.Duration(n) * time.Second
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
	// Agent/segment binaries: convention by default (sibling of the gpstream
	// binary, then PATH); these overrides are for dev builds and tarball
	// installs where the binaries are not beside each other.
	if v, ok := get("SOURCE_AGENT_BINARY"); ok {
		c.SourceAgentBinary = v
	}
	if v, ok := get("DEST_AGENT_BINARY"); ok {
		c.DestAgentBinary = v
	}
	if v, ok := get("SEG_BINARY"); ok {
		c.SegBinary = v
	}
	if v, ok := get("SEG_LISTEN"); ok {
		c.SegListen = v
	}
	if v, ok := get("PORT_RANGE"); ok {
		c.PortRange = v
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
	// Control-channel TLS (the controller's dials to the agents' control
	// sockets).
	if v, ok := get("CONTROL_TLS_CERT"); ok {
		c.ControlTLSCert = v
	}
	if v, ok := get("CONTROL_TLS_KEY"); ok {
		c.ControlTLSKey = v
	}
	if v, ok := get("CONTROL_TLS_CA"); ok {
		c.ControlTLSCA = v
	}
	// Remote agent launch is CLI-only (P1e): --source-agent-ssh /
	// --dest-agent-ssh (Inc2c env vars removed, not agreed).
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
	case "max_snapshot_delta":
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxSnapshotDelta = time.Duration(n) * time.Second
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
	case "control_tls_cert":
		c.ControlTLSCert = v
	case "control_tls_key":
		c.ControlTLSKey = v
	case "control_tls_ca":
		c.ControlTLSCA = v
	case "source_agent_ssh":
		c.SourceAgentSSH = v
	case "dest_agent_ssh":
		c.DestAgentSSH = v
	case "source_agent_binary":
		c.SourceAgentBinary = v
	case "dest_agent_binary":
		c.DestAgentBinary = v
	case "seg_binary":
		c.SegBinary = v
	case "seg_listen":
		c.SegListen = v
	case "port_range":
		c.PortRange = v
	case "index_priority":
		c.IndexPriority = v
	case "timeout":
		if d, err := time.ParseDuration(v); err == nil {
			c.Timeout = d
		}
	}
}

// ResolveAgentBinaries fills the empty agent/segment binary paths by
// convention so a packaged install (all four binaries in /usr/bin) needs no
// flags: (1) sibling of the running gpstream binary (os.Executable's
// directory), (2) PATH lookup. An explicitly configured value (CLI/config
// file/env) is never clobbered. It fails loud when a binary cannot be found
// — the on_segment path additionally needs the segreceiver to be installed
// on the SOURCE SEGMENT HOSTS at the same path (the GP kernel executes it
// there), which is verified by doctor and by the segreceivers' dial.
func (c *Config) ResolveAgentBinaries() error {
	var err error
	if c.SourceAgentBinary == "" {
		c.SourceAgentBinary, err = resolveSibling("gpstream-source-agent")
		if err != nil {
			return fmt.Errorf("resolve gpstream-source-agent: %w", err)
		}
	}
	if c.DestAgentBinary == "" {
		c.DestAgentBinary, err = resolveSibling("gpstream-dest-agent")
		if err != nil {
			return fmt.Errorf("resolve gpstream-dest-agent: %w", err)
		}
	}
	// The segreceiver is needed only for on_segment (the kernel runs it on
	// the source segments); other modes never reference it.
	if c.CopyMode == CopyMesh && c.SegBinary == "" {
		c.SegBinary, err = resolveSibling("gpstream-segreceiver")
		if err != nil {
			return fmt.Errorf("resolve gpstream-segreceiver: %w", err)
		}
	}
	return nil
}

// resolveSibling finds name next to the running gpstream binary, then on the
// PATH. It returns an absolute path (the GP kernel runs the segreceiver path
// verbatim on the segment hosts, so a bare PATH name would not survive).
func resolveSibling(name string) (string, error) {
	if exe, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(exe), name)
		if st, err := os.Stat(cand); err == nil && st.Mode().Perm()&0o111 != 0 {
			return cand, nil
		}
	}
	if p, err := exec.LookPath(name); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("%s not found (looked next to %s and on PATH)", name, "the gpstream binary")
}

// ResolveSegBinaryOrEmpty is the soft variant for doctor: it resolves the
// segreceiver by convention but returns "" (no error) when it is not found,
// so the on_segment probe is skipped instead of failing the diagnostic.
func ResolveSegBinaryOrEmpty() (string, error) {
	p, err := resolveSibling("gpstream-segreceiver")
	if err != nil {
		return "", nil
	}
	return p, nil
}

// ResolveSegListen fills an empty --seg-listen (on_segment only) so the
// operator does not have to name a controller address: the controller's
// primary non-loopback IPv4 + a free port (prebound, so the base is almost
// certainly free; each unit then binds base+unitID at its own start). The
// host must be routable from the SOURCE segment hosts — a multi-homed
// controller should set --seg-listen explicitly. nUnits is only used for a
// clearer error if the free-port probe fails.
func (c *Config) ResolveSegListen(nUnits int) error {
	if c.SegListen != "" {
		return nil
	}
	host, err := primaryIPv4()
	if err != nil {
		return fmt.Errorf("auto --seg-listen: %w (set --seg-listen HOST:PORT explicitly on a multi-homed controller)", err)
	}
	port, err := freePort(host)
	if err != nil {
		return fmt.Errorf("auto --seg-listen: no free port on %s for %d unit(s): %w (set --seg-listen HOST:PORT)", host, nUnits, err)
	}
	c.SegListen = net.JoinHostPort(host, strconv.Itoa(port))
	return nil
}

// primaryIPv4 returns the first non-loopback, global IPv4 address of the
// host (the one the source segments are most likely to reach).
func primaryIPv4() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var candidate string
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLoopback() || !ipn.IP.To4().IsGlobalUnicast() {
			continue
		}
		// Prefer the most common LAN ranges, then anything else: on a
		// multi-NIC box the first global unicast is usually the default one.
		if candidate == "" {
			candidate = ipn.IP.String()
		}
	}
	if candidate == "" {
		return "", fmt.Errorf("no non-loopback IPv4 address found")
	}
	return candidate, nil
}

// freePort prebinds a throwaway listener on host:0 and returns the port, the
// same "bind :0, read, release" trick prebindLoopback uses for loopback. The
// tiny release/rebind race is acceptable: a collision surfaces as a clear
// per-unit bind error, not silent data corruption.
func freePort(host string) (int, error) {
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// getBool reads an env var as a boolean (true/1/on = true).
func getBool(key string) (bool, bool) {
	v, ok := os.LookupEnv("GPSTREAM_" + key)
	if !ok {
		return false, false
	}
	return v == "true" || v == "1" || v == "on", true
}
