// Package metrics implements observability (FR-04): throughput, bytes,
// error rate, latency and the adaptive state (rate-limit and compression
// level). It exposes a Prometheus textformat endpoint and is the consumer of
// the limiter's diagnostic messages, so a compression-level / rate-limit
// degradation (FR-12/FR-14) is observable as a metric.
//
// The package is self-contained (no third-party metrics dependency) to
// respect FR-08; the exposition format is the Prometheus textformat v0.0.4.
package monitor

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Metrics is the in-memory state of a running manifest.
type Metrics struct {
	mu           sync.Mutex
	start        time.Time
	Bytes        uint64  // bytes transferred (compressed on the wire)
	BytesDecoded uint64  // bytes after decompression (logical)
	Rows         uint64  // rows copied (FR-04 "rows")
	Errors       uint64  // frame / transfer errors
	FrameErrors  uint64  // CRC-mismatch / framing errors
	ChecksumErrs uint64  // content-checksum mismatches in the verify phase (FR-04 "checksum errors")
	AutoRecovery uint64  // auto-recovery actions: resume re-reads + failed-unit retries (FR-04 "auto-recovery events")
	RateLimit    float64 // current Gbps (from the limiter)
	// ReceiveWriteMs is the average data-path frame-write latency (ms) over
	// the last second — the destination's receive-queue depth as the
	// controller observes it (FR-14 "receive queue", FR-04 gauge).
	ReceiveWriteMs float64
	CompressLevel  int    // current zstd level (degrades on CPU saturation)
	CompressMode   string // none / zlib / zstd
	IndexQueue     int    // pending background index builds (FR-17/FR-04)
	IndexDone      int    // finished background index builds (FR-17/FR-04)
	// ActiveFlows is the number of units currently in the copying state
	// (FR-04 "number of active flows").
	ActiveFlows int
	// TotalBytes is the planned logical volume from the plan (for the ETA:
	// remaining = TotalBytes - BytesDecoded). Zero when unknown.
	TotalBytes uint64
	// Pair is the host pair label (FR-04 "per host pair"): source ->
	// destination host:port. Empty when not set.
	Pair string
	// tablesByStatus aggregates the unit states (FR-04 "table counters by
	// status").
	tablesByStatus map[string]int
	// perTable holds the per-table counters (FR-04 "per table"): rows, wire
	// bytes and logical bytes, plus the current state for the live table.
	perTable map[string]*tableMetric
	// Frame-ack observability (DESIGN §4.6 / FR-04): retransmitted frames,
	// transport reconnects, and the worst observed ACK lag.
	Retransmitted uint64
	Reconnects    uint64
	AckLagMax     time.Duration
	compressHits  map[string]uint64
	rateHistory   []sample // for throughput window
}

// tableMetric is one row of the per-table breakdown (FR-04 "per table").
type tableMetric struct {
	Rows    uint64
	Wire    uint64
	Logical uint64
	State   string
}

type sample struct {
	t   time.Time
	bin uint64 // cumulative bytes at this sample
}

// New creates a Metrics collector.
func New() *Metrics {
	return &Metrics{
		start:          time.Now(),
		compressHits:   map[string]uint64{},
		tablesByStatus: map[string]int{},
		perTable:       map[string]*tableMetric{},
	}
}

// Record records a transfer of n (wire) and nLogical (logical) bytes.
func (m *Metrics) Record(n, nLogical uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Bytes += n
	m.BytesDecoded += nLogical
	now := time.Now()
	m.rateHistory = append(m.rateHistory, sample{t: now, bin: m.Bytes})
	// Keep ~60s of samples.
	for len(m.rateHistory) > 0 && now.Sub(m.rateHistory[0].t) > 60*time.Second {
		m.rateHistory = m.rateHistory[1:]
	}
}

// RecordError records a transfer error (FR-04 error rate).
func (m *Metrics) RecordError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Errors++
}

// RecordRows records the rows copied by a finished unit (FR-04 "rows").
// table (a "db.schema.name" or "schema.name" key) may be empty for
// non-table work; rows are always added to the aggregate.
func (m *Metrics) RecordRows(table string, rows int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := uint64(rows)
	if rows < 0 {
		r = 0
	}
	m.Rows += r
	if table != "" {
		if tm := m.table(table); tm != nil {
			tm.Rows += r
		}
	}
}

// RecordBytes records a unit's wire (compressed) and logical (decoded) byte
// counts (FR-04 "volume", "input/output compression"), both aggregate and per-table.
func (m *Metrics) RecordBytes(table string, wire, logical uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Bytes += wire
	m.BytesDecoded += logical
	now := time.Now()
	m.rateHistory = append(m.rateHistory, sample{t: now, bin: m.Bytes})
	// Keep ~60s of samples.
	for len(m.rateHistory) > 0 && now.Sub(m.rateHistory[0].t) > 60*time.Second {
		m.rateHistory = m.rateHistory[1:]
	}
	if table != "" {
		if tm := m.table(table); tm != nil {
			tm.Wire += wire
			tm.Logical += logical
		}
	}
}

// SetTotalBytes records the planned logical volume from the plan, which the
// ETA is computed from (remaining = total - decoded).
func (m *Metrics) SetTotalBytes(total int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if total > 0 {
		m.TotalBytes = uint64(total)
	}
}

// SetPair labels the collector with the source->destination host pair
// (FR-04 "per host pair").
func (m *Metrics) SetPair(pair string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Pair = pair
}

// SetUnitState records a unit's state transition (FR-04 "table counters by
// status" + "number of active flows"). It is a state-move gauge: the unit
// leaves its previous state and enters the new one, so the per-status counts
// always sum to the number of known units. table is the per-table key
// ("schema.name" or "db.schema.name"); it may be empty for non-table work.
// The manager calls it at run start (for every unit, with the state
// inherited from the journal) and on every subsequent journal state change,
// so the gauge is complete and monotone-correct.
func (m *Metrics) SetUnitState(table, state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var prev string
	if table != "" {
		if tm := m.perTable[table]; tm != nil {
			prev = tm.State
		}
	}
	// Active flows = units in the copying state.
	if state == "copying" && prev != "copying" {
		m.ActiveFlows++
	} else if state != "copying" && prev == "copying" {
		m.ActiveFlows--
	}
	// Per-status gauge: move the unit out of its previous state.
	if prev != "" {
		if m.tablesByStatus[prev]--; m.tablesByStatus[prev] <= 0 {
			delete(m.tablesByStatus, prev)
		}
	}
	if state != "" {
		m.tablesByStatus[state]++
	}
	if table != "" {
		if tm := m.table(table); tm != nil {
			tm.State = state
		}
	}
}

// RecordChecksumError records a content-checksum mismatch in the verify phase
// (FR-04 "checksum errors").
func (m *Metrics) RecordChecksumError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ChecksumErrs++
}

// RecordAutoRecovery records an auto-recovery action (a resume re-read or a
// retried failed unit) (FR-04 "auto-recovery events").
func (m *Metrics) RecordAutoRecovery() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AutoRecovery++
}

// table returns (creating if needed) the per-table metric under the lock.
func (m *Metrics) table(key string) *tableMetric {
	tm, ok := m.perTable[key]
	if !ok {
		tm = &tableMetric{}
		m.perTable[key] = tm
	}
	return tm
}

// etaLocked computes the remaining-seconds estimate; the caller MUST hold m.mu.
// Both the planned volume (TotalBytes) and the transferred volume (BytesDecoded)
// are logical bytes, so remaining = TotalBytes - BytesDecoded and the
// throughput is logical too — subtracting wire/compressed bytes from a logical
// total understates progress (the run would look far from done).
func (m *Metrics) etaLocked() int {
	if m.TotalBytes == 0 {
		return 0
	}
	remaining := int64(m.TotalBytes) - int64(m.BytesDecoded)
	if remaining <= 0 {
		return 0
	}
	elapsed := time.Since(m.start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	tp := float64(m.BytesDecoded) / elapsed
	if tp <= 0 {
		return 0
	}
	return int(float64(remaining) / tp)
}

// ETASeconds returns the estimated remaining seconds at the recent throughput
// (FR-04 «ETA»). Zero when the throughput is unknown or nothing is planned.
func (m *Metrics) ETASeconds() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.etaLocked()
}

// RecordFrameAck accumulates the per-unit frame-ack counters (DESIGN §4.6):
// retransmitted frames, transport reconnects, and the observed ACK lag.
func (m *Metrics) RecordFrameAck(retransmitted uint64, reconnects uint64, ackLag time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Retransmitted += retransmitted
	m.Reconnects += reconnects
	if ackLag > m.AckLagMax {
		m.AckLagMax = ackLag
	}
}

// RecordFrameError records a CRC/framing error (FR-01/§4.2).
func (m *Metrics) RecordFrameError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Errors++
	m.FrameErrors++
}

// SetRateLimit records the limiter's current rate (Gbps) (FR-14/FR-04).
func (m *Metrics) SetRateLimit(gbps float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RateLimit = gbps
}

// SetReceiveWriteMs records the average data-path frame-write latency (ms)
// (FR-14 "receive queue" / FR-04 gauge). Called by the limiter sampler each
// tick so the /metrics endpoint exposes the receive-queue depth the
// controller observes.
func (m *Metrics) SetReceiveWriteMs(ms float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReceiveWriteMs = ms
}

// SetCompression records a compression-mode / level change (FR-12/FR-14
// auto-degradation). It is observable (FR-04) and counts the change.
func (m *Metrics) SetCompression(mode string, level int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%d", mode, level)
	m.CompressMode = mode
	m.CompressLevel = level
	m.compressHits[key]++
}

// SetIndexProgress records the background index pool's queue depth and
// finished-job count (FR-17/FR-04). It is invoked by the pool after every
// job completes.
func (m *Metrics) SetIndexProgress(queueDepth, done int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.IndexQueue = queueDepth
	m.IndexDone = done
}

// Throughput returns the recent throughput in bytes/sec (FR-04).
func (m *Metrics) Throughput() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rateHistory) < 2 {
		return 0
	}
	first := m.rateHistory[0]
	last := m.rateHistory[len(m.rateHistory)-1]
	dur := last.t.Sub(first.t).Seconds()
	if dur <= 0 {
		return 0
	}
	return float64(last.bin-first.bin) / dur
}

// Latency returns the job duration (FR-04; a job-level latency).
func (m *Metrics) Latency() time.Duration {
	return time.Since(m.start)
}

// ErrorRate returns the error rate as errors/byte over the manifest.
func (m *Metrics) ErrorRate() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Bytes == 0 {
		return 0
	}
	return float64(m.Errors) / float64(m.Bytes)
}

// Prometheus renders the metrics in the Prometheus textformat (FR-04). It
// snapshots the state under the lock, then renders without holding it, so a
// call that internally reads other (locking) accessors cannot deadlock.
func (m *Metrics) Prometheus() string {
	m.mu.Lock()
	snap := snapshot{
		bytes:        m.Bytes,
		decoded:      m.BytesDecoded,
		rows:         m.Rows,
		errors:       m.Errors,
		frameErrors:  m.FrameErrors,
		checksumErrs: m.ChecksumErrs,
		autoRecovery: m.AutoRecovery,
		rate:         m.RateLimit,
		receiveMs:    m.ReceiveWriteMs,
		compressMode: m.CompressMode,
		compressLv:   m.CompressLevel,
		indexQueue:   m.IndexQueue,
		indexDone:    m.IndexDone,
		activeFlows:  m.ActiveFlows,
		etaSec:       m.etaLocked(), // m.mu already held above
		retrans:      m.Retransmitted,
		reconnects:   m.Reconnects,
		ackLag:       m.AckLagMax,
		pair:         m.Pair,
		byStatus:     make(map[string]int, len(m.tablesByStatus)),
	}
	for s, n := range m.tablesByStatus {
		snap.byStatus[s] = n
	}
	snap.perTable = make(map[string]tableMetric, len(m.perTable))
	for k, tm := range m.perTable {
		snap.perTable[k] = *tm
	}
	m.mu.Unlock()
	return snap.render(m.Throughput())
}

// snapshot is an immutable copy of the metrics state for lock-free rendering.
type snapshot struct {
	bytes        uint64
	decoded      uint64
	rows         uint64
	errors       uint64
	frameErrors  uint64
	checksumErrs uint64
	autoRecovery uint64
	rate         float64
	receiveMs    float64
	compressMode string
	compressLv   int
	indexQueue   int
	indexDone    int
	activeFlows  int
	etaSec       int
	retrans      uint64
	reconnects   uint64
	ackLag       time.Duration
	pair         string
	byStatus     map[string]int
	perTable     map[string]tableMetric
}

func (s snapshot) render(throughput float64) string {
	out := make([]string, 0, 40)
	out = append(out, "# HELP gpstream_bytes_total Bytes transferred (compressed on the wire).")
	out = append(out, "# TYPE gpstream_bytes_total counter")
	out = append(out, fmt.Sprintf("gpstream_bytes_total %d", s.bytes))

	out = append(out, "# HELP gpstream_bytes_decoded_total Logical bytes after decompression.")
	out = append(out, "# TYPE gpstream_bytes_decoded_total counter")
	out = append(out, fmt.Sprintf("gpstream_bytes_decoded_total %d", s.decoded))

	out = append(out, "# HELP gpstream_rows_total Rows copied (FR-04).")
	out = append(out, "# TYPE gpstream_rows_total counter")
	out = append(out, fmt.Sprintf("gpstream_rows_total %d", s.rows))

	out = append(out, "# HELP gpstream_errors_total Transfer errors.")
	out = append(out, "# TYPE gpstream_errors_total counter")
	out = append(out, fmt.Sprintf("gpstream_errors_total %d", s.errors))

	out = append(out, "# HELP gpstream_frame_errors_total CRC/framing errors.")
	out = append(out, "# TYPE gpstream_frame_errors_total counter")
	out = append(out, fmt.Sprintf("gpstream_frame_errors_total %d", s.frameErrors))

	out = append(out, "# HELP gpstream_checksum_errors_total Content-checksum mismatches in the verify phase (FR-04).")
	out = append(out, "# TYPE gpstream_checksum_errors_total counter")
	out = append(out, fmt.Sprintf("gpstream_checksum_errors_total %d", s.checksumErrs))

	out = append(out, "# HELP gpstream_auto_recovery_total Auto-recovery actions (resume re-reads, retries) (FR-04).")
	out = append(out, "# TYPE gpstream_auto_recovery_total counter")
	out = append(out, fmt.Sprintf("gpstream_auto_recovery_total %d", s.autoRecovery))

	out = append(out, "# HELP gpstream_rate_limit_gbits Current rate limit in Gbps (FR-14).")
	out = append(out, "# TYPE gpstream_rate_limit_gbits gauge")
	out = append(out, fmt.Sprintf("gpstream_rate_limit_gbits %g", s.rate))

	out = append(out, "# HELP gpstream_receive_write_latency_ms Average data-path write latency (ms) — destination receive-queue depth as the controller observes it (FR-14).")
	out = append(out, "# TYPE gpstream_receive_write_latency_ms gauge")
	out = append(out, fmt.Sprintf("gpstream_receive_write_latency_ms %g", s.receiveMs))

	out = append(out, "# HELP gpstream_throughput_bytes Current throughput in bytes/sec.")
	out = append(out, "# TYPE gpstream_throughput_bytes gauge")
	out = append(out, fmt.Sprintf("gpstream_throughput_bytes %g", throughput))

	out = append(out, "# HELP gpstream_eta_seconds Estimated remaining seconds at the recent throughput (FR-04).")
	out = append(out, "# TYPE gpstream_eta_seconds gauge")
	out = append(out, fmt.Sprintf("gpstream_eta_seconds %d", s.etaSec))

	out = append(out, "# HELP gpstream_compression_level Current compression level (FR-12).")
	out = append(out, "# TYPE gpstream_compression_level gauge")
	out = append(out, fmt.Sprintf("gpstream_compression_level %d", s.compressLv))

	out = append(out, "# HELP gpstream_compression_mode Current compression mode (FR-12).")
	out = append(out, "# TYPE gpstream_compression_mode gauge")
	out = append(out, fmt.Sprintf("gpstream_compression_mode{mode=%q} 1", s.compressMode))

	out = append(out, "# HELP gpstream_index_queue Background index builds pending (FR-17).")
	out = append(out, "# TYPE gpstream_index_queue gauge")
	out = append(out, fmt.Sprintf("gpstream_index_queue %d", s.indexQueue))

	out = append(out, "# HELP gpstream_index_done_total Background index builds finished (FR-17).")
	out = append(out, "# TYPE gpstream_index_done_total counter")
	out = append(out, fmt.Sprintf("gpstream_index_done_total %d", s.indexDone))

	out = append(out, "# HELP gpstream_active_flows Units currently copying (FR-04 “number of active flows”).")
	out = append(out, "# TYPE gpstream_active_flows gauge")
	out = append(out, fmt.Sprintf("gpstream_active_flows %d", s.activeFlows))

	out = append(out, "# HELP gpstream_frames_retransmitted_total Frames retransmitted after loss/corruption (§4.6).")
	out = append(out, "# TYPE gpstream_frames_retransmitted_total counter")
	out = append(out, fmt.Sprintf("gpstream_frames_retransmitted_total %d", s.retrans))

	out = append(out, "# HELP gpstream_reconnects_total Data-path transport reconnects (§4.6).")
	out = append(out, "# TYPE gpstream_reconnects_total counter")
	out = append(out, fmt.Sprintf("gpstream_reconnects_total %d", s.reconnects))

	out = append(out, "# HELP gpstream_ack_lag_max_seconds Worst observed ACK lag (§4.6).")
	out = append(out, "# TYPE gpstream_ack_lag_max_seconds gauge")
	out = append(out, fmt.Sprintf("gpstream_ack_lag_max_seconds %f", s.ackLag.Seconds()))

	// Tables by status (FR-04 "table counters by status"). HELP/TYPE come
	// before the samples, in deterministic order.
	if len(s.byStatus) > 0 {
		out = append(out, "# HELP gpstream_tables_by_status Units by state (FR-04).")
		out = append(out, "# TYPE gpstream_tables_by_status gauge")
		for _, st := range statusOrder {
			if n, ok := s.byStatus[st]; ok {
				out = append(out, fmt.Sprintf("gpstream_tables_by_status{status=%q} %d", st, n))
			}
		}
		// Any state outside the known order (defensive).
		for st, n := range s.byStatus {
			if knownStatus[st] {
				continue
			}
			out = append(out, fmt.Sprintf("gpstream_tables_by_status{status=%q} %d", st, n))
		}
	}

	// Per-table breakdown (FR-04 "per table"). Deterministic order.
	if len(s.perTable) > 0 {
		keys := make([]string, 0, len(s.perTable))
		for k := range s.perTable {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out = append(out, "# HELP gpstream_table_rows Rows copied per table (FR-04).")
		out = append(out, "# TYPE gpstream_table_rows counter")
		for _, k := range keys {
			out = append(out, fmt.Sprintf("gpstream_table_rows{table=%q} %d", k, s.perTable[k].Rows))
		}
		out = append(out, "# HELP gpstream_table_bytes_wire Wire bytes per table (FR-04).")
		out = append(out, "# TYPE gpstream_table_bytes_wire counter")
		for _, k := range keys {
			out = append(out, fmt.Sprintf("gpstream_table_bytes_wire{table=%q} %d", k, s.perTable[k].Wire))
		}
		out = append(out, "# HELP gpstream_table_bytes_logical Logical bytes per table (FR-04).")
		out = append(out, "# TYPE gpstream_table_bytes_logical counter")
		for _, k := range keys {
			out = append(out, fmt.Sprintf("gpstream_table_bytes_logical{table=%q} %d", k, s.perTable[k].Logical))
		}
		out = append(out, "# HELP gpstream_table_state Current unit state per table (1 = in this state) (FR-04).")
		out = append(out, "# TYPE gpstream_table_state gauge")
		for _, k := range keys {
			if st := s.perTable[k].State; st != "" {
				out = append(out, fmt.Sprintf("gpstream_table_state{table=%q,status=%q} 1", k, st))
			}
		}
	}

	// Host pair (FR-04 "per host pair").
	if s.pair != "" {
		out = append(out, "# HELP gpstream_pair_up Host pair of the running copy (1 = up) (FR-04).")
		out = append(out, "# TYPE gpstream_pair_up gauge")
		out = append(out, fmt.Sprintf("gpstream_pair_up{pair=%q} 1", s.pair))
	}

	return joinLines(out)
}

// statusOrder renders the per-status gauges in a stable, operator-readable
// order; knownStatus guards the defensive fallback loop.
var statusOrder = []string{
	"pending", "copying", "verifying", "completed", "swapped", "failed", "skipped",
}

var knownStatus = map[string]bool{
	"pending": true, "copying": true, "verifying": true,
	"completed": true, "swapped": true, "failed": true, "skipped": true,
}

// Handler returns an http.Handler serving the metrics (FR-04).
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, m.Prometheus())
	})
}

// TableRow is one row of the live per-table table (FR-04 "live table by
// tables" for `gpstream status`).
type TableRow struct {
	Table   string
	Rows    uint64
	Wire    uint64
	Logical uint64
	State   string
}

// TablesByStatus returns a copy of the per-status unit counts (FR-04).
func (m *Metrics) TablesByStatus() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.tablesByStatus))
	for s, n := range m.tablesByStatus {
		out[s] = n
	}
	return out
}

// PerTable returns a copy of the per-table counters, sorted by table name
// (FR-04), for the live table in `gpstream status`.
func (m *Metrics) PerTable() []TableRow {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TableRow, 0, len(m.perTable))
	for k, tm := range m.perTable {
		out = append(out, TableRow{
			Table:   k,
			Rows:    tm.Rows,
			Wire:    tm.Wire,
			Logical: tm.Logical,
			State:   tm.State,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out
}

// RowsTotal returns the aggregate copied-row count (FR-04).
func (m *Metrics) RowsTotal() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Rows
}

// ActiveFlowsCount returns the number of units currently copying (FR-04).
func (m *Metrics) ActiveFlowsCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ActiveFlows
}

// PairLabel returns the source->destination host-pair label (FR-04).
func (m *Metrics) PairLabel() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Pair
}

// StartedAt returns the collector's start time (FR-04, for runstats.json).
func (m *Metrics) StartedAt() time.Time {
	return m.start
}

// RunStats is the serialised live-run snapshot written to runstats.json
// (FR-04): the data `gpstream status` renders for a running operation.
type RunStats struct {
	UpdatedAt   string         `json:"updated_at"`
	StartedAt   string         `json:"started_at"`
	ElapsedSec  int            `json:"elapsed_sec"`
	Pair        string         `json:"pair,omitempty"`
	Bytes       uint64         `json:"bytes"`
	Decoded     uint64         `json:"decoded"`
	Rows        uint64         `json:"rows"`
	Throughput  float64        `json:"throughput_bytes"`
	ETASeconds  int            `json:"eta_seconds"`
	ActiveFlows int            `json:"active_flows"`
	ByStatus    map[string]int `json:"by_status"`
	PerTable    []TableRow     `json:"per_table"`
}

func joinLines(s []string) string {
	out := ""
	for i, l := range s {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out + "\n"
}
