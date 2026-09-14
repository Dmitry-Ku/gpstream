package monitor

import (
	"strings"
	"testing"
	"time"
)

func TestRecordAndThroughput(t *testing.T) {
	m := New()
	m.Record(1000, 800)
	m.Record(1000, 800)
	if m.Bytes != 2000 {
		t.Errorf("bytes = %d, want 2000", m.Bytes)
	}
	if m.BytesDecoded != 1600 {
		t.Errorf("decoded = %d, want 1600", m.BytesDecoded)
	}
	if m.Throughput() <= 0 {
		t.Errorf("throughput should be > 0 after records")
	}
}

func TestErrors(t *testing.T) {
	m := New()
	m.Record(1000, 1000)
	m.RecordError()
	m.RecordFrameError()
	if m.Errors != 2 {
		t.Errorf("errors = %d, want 2", m.Errors)
	}
	if m.FrameErrors != 1 {
		t.Errorf("frame errors = 1, want 1")
	}
	if m.ErrorRate() == 0 {
		t.Error("error rate should be > 0")
	}
}

func TestCompressionAndRate(t *testing.T) {
	m := New()
	m.SetCompression("zstd", 7)
	if m.CompressMode != "zstd" || m.CompressLevel != 7 {
		t.Errorf("compression = %s/%d", m.CompressMode, m.CompressLevel)
	}
	m.SetRateLimit(80)
	if m.RateLimit != 80 {
		t.Errorf("rate limit = %g, want 80", m.RateLimit)
	}
}

func TestPrometheus(t *testing.T) {
	m := New()
	m.Record(1000, 800)
	m.SetCompression("zstd", 5)
	m.SetRateLimit(75)
	s := m.Prometheus()
	for _, want := range []string{
		"gpstream_bytes_total 1000",
		"gpstream_bytes_decoded_total 800",
		"gpstream_rate_limit_gbits 75",
		"gpstream_compression_level 5",
		`gpstream_compression_mode{mode="zstd"} 1`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("prometheus output missing %q", want)
		}
	}
}

func TestLatency(t *testing.T) {
	m := New()
	time.Sleep(5 * time.Millisecond)
	if m.Latency() < 5*time.Millisecond {
		t.Errorf("latency = %v, want >= 5ms", m.Latency())
	}
}

func TestIndexProgressMetrics(t *testing.T) {
	m := New()
	m.SetIndexProgress(3, 5)
	out := m.Prometheus()
	if !strings.Contains(out, "gpstream_index_queue 3") {
		t.Fatalf("index queue gauge missing or wrong:\n%s", out)
	}
	if !strings.Contains(out, "gpstream_index_done_total 5") {
		t.Fatalf("index done counter missing or wrong:\n%s", out)
	}
	m.SetIndexProgress(0, 7)
	out = m.Prometheus()
	if !strings.Contains(out, "gpstream_index_queue 0") || !strings.Contains(out, "gpstream_index_done_total 7") {
		t.Fatalf("index metrics not updated:\n%s", out)
	}
}
