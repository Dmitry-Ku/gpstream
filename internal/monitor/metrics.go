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
	"sync"
	"time"
)

// Metrics is the in-memory state of a running manifest.
type Metrics struct {
	mu            sync.Mutex
	start         time.Time
	Bytes         uint64  // bytes transferred (compressed on the wire)
	BytesDecoded  uint64  // bytes after decompression (logical)
	Errors        uint64  // frame / transfer errors
	FrameErrors   uint64  // CRC-mismatch / framing errors
	RateLimit     float64 // current Gbps (from the limiter)
	CompressLevel int     // current zstd level (degrades on CPU saturation)
	CompressMode  string  // none / zlib / zstd
	IndexQueue    int     // pending background index builds (FR-17/FR-04)
	IndexDone     int     // finished background index builds (FR-17/FR-04)
	compressHits  map[string]uint64
	rateHistory   []sample // for throughput window
}

type sample struct {
	t   time.Time
	bin uint64 // cumulative bytes at this sample
}

// New creates a Metrics collector.
func New() *Metrics {
	return &Metrics{
		start:        time.Now(),
		compressHits: map[string]uint64{},
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
		errors:       m.Errors,
		frameErrors:  m.FrameErrors,
		rate:         m.RateLimit,
		compressMode: m.CompressMode,
		compressLv:   m.CompressLevel,
		indexQueue:   m.IndexQueue,
		indexDone:    m.IndexDone,
	}
	m.mu.Unlock()
	return snap.render(m.Throughput())
}

// snapshot is an immutable copy of the metrics state for lock-free rendering.
type snapshot struct {
	bytes        uint64
	decoded      uint64
	errors       uint64
	frameErrors  uint64
	rate         float64
	compressMode string
	compressLv   int
	indexQueue   int
	indexDone    int
}

func (s snapshot) render(throughput float64) string {
	out := make([]string, 0, 16)
	out = append(out, "# HELP gpstream_bytes_total Bytes transferred (compressed on the wire).")
	out = append(out, "# TYPE gpstream_bytes_total counter")
	out = append(out, fmt.Sprintf("gpstream_bytes_total %d", s.bytes))

	out = append(out, "# HELP gpstream_bytes_decoded_total Logical bytes after decompression.")
	out = append(out, "# TYPE gpstream_bytes_decoded_total counter")
	out = append(out, fmt.Sprintf("gpstream_bytes_decoded_total %d", s.decoded))

	out = append(out, "# HELP gpstream_errors_total Transfer errors.")
	out = append(out, "# TYPE gpstream_errors_total counter")
	out = append(out, fmt.Sprintf("gpstream_errors_total %d", s.errors))

	out = append(out, "# HELP gpstream_frame_errors_total CRC/framing errors.")
	out = append(out, "# TYPE gpstream_frame_errors_total counter")
	out = append(out, fmt.Sprintf("gpstream_frame_errors_total %d", s.frameErrors))

	out = append(out, "# HELP gpstream_rate_limit_gbits Current rate limit in Gbps (FR-14).")
	out = append(out, "# TYPE gpstream_rate_limit_gbits gauge")
	out = append(out, fmt.Sprintf("gpstream_rate_limit_gbits %g", s.rate))

	out = append(out, "# HELP gpstream_throughput_bytes Current throughput in bytes/sec.")
	out = append(out, "# TYPE gpstream_throughput_bytes gauge")
	out = append(out, fmt.Sprintf("gpstream_throughput_bytes %g", throughput))

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

	return joinLines(out)
}

// Handler returns an http.Handler serving the metrics (FR-04).
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, m.Prometheus())
	})
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
