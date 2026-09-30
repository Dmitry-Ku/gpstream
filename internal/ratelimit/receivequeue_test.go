package ratelimit

import (
	"testing"
	"time"
)

// TestReduceOnReceiveQueue verifies F-14 «рост очереди приёма»: when the
// destination's receive queue grows (data-path write latency rises, the
// in-process «метрики COPY»), the limiter reduces the pair rate with the
// dest-receive-queue cause.
func TestReduceOnReceiveQueue(t *testing.T) {
	p := &fakeProvider{m: Metrics{}} // healthy host metrics
	l := New(Options{Baseline: 100, Floor: 1, Provider: p})
	l.SetReceiveWriteLatency(func() float64 { return 80 }) // > 50ms threshold
	changes := make(chan [2]Gbps, 16)
	causes := make(chan string, 16)
	l.SetOnChange(func(from, to Gbps, cause string) {
		changes <- [2]Gbps{from, to}
		causes <- cause
	})
	l.Update()
	select {
	case c := <-changes:
		if c[0] != 100 || c[1] < 70 || c[1] > 80 {
			t.Errorf("rate = %g -> %g, want 100 -> ~75", c[0], c[1])
		}
	case <-time.After(time.Second):
		t.Fatal("no rate change emitted on receive-queue growth")
	}
	cause := <-causes
	if cause != "dest-receive-queue 80ms write-latency (>50ms)" {
		t.Errorf("cause = %q, want dest-receive-queue with the metric value", cause)
	}
}

// TestNoReduceOnHealthyReceiveQueue: a draining destination (low write
// latency) must NOT reduce the rate.
func TestNoReduceOnHealthyReceiveQueue(t *testing.T) {
	p := &fakeProvider{m: Metrics{}}
	l := New(Options{Baseline: 100, Floor: 1, Provider: p})
	l.SetReceiveWriteLatency(func() float64 { return 5 }) // well under threshold
	l.Update()                                            // at baseline, healthy → nothing to do
	if got := l.Current(); got != 100 {
		t.Errorf("rate = %g, want unchanged 100 (healthy receive queue must not reduce)", got)
	}
}

// TestReceiveQueueRecoveryHysteresis: after a reduction, recovery is allowed
// only once the receive-queue write latency drops below the recover
// threshold (20ms) — a borderline receiver (between 20 and 50ms) neither
// reduces nor recovers (no flapping).
func TestReceiveQueueRecoveryHysteresis(t *testing.T) {
	p := &fakeProvider{m: Metrics{}}
	l := New(Options{Baseline: 100, Floor: 1, Provider: p})
	lat := 0.0
	l.SetReceiveWriteLatency(func() float64 { return lat })
	var changed bool
	l.SetOnChange(func(Gbps, Gbps, string) { changed = true })

	// Reduce first (latency 80ms).
	lat = 80
	l.Update()
	if changed {
		changed = false
	} else {
		t.Fatal("first Update did not reduce")
	}
	below := l.Current()

	// Borderline (35ms): between recover (20) and reduce (50) thresholds —
	// the rate must stay put.
	lat = 35
	l.Update()
	if changed {
		t.Errorf("borderline receive-queue latency caused a rate change")
	}
	if got := l.Current(); got != below {
		t.Errorf("rate = %g, want unchanged %g in the hysteresis band", got, below)
	}

	// Healthy (10ms): recover towards baseline.
	lat = 10
	l.Update()
	if !changed {
		t.Fatal("no recovery emitted when the receive queue drained")
	}
	if got := l.Current(); got <= below || got > 100 {
		t.Errorf("recovered rate = %g, want in (%g, 100]", got, below)
	}
}

// TestWriteRecorderAvg checks the sliding-window average: recent slow writes
// dominate, and samples older than the window drop out.
func TestWriteRecorderAvg(t *testing.T) {
	r := &WriteRecorder{}
	// Nothing recorded yet: 0 (limiter treats it as "no data / drained").
	if got := r.AvgMs(time.Second); got != 0 {
		t.Errorf("empty recorder avg = %g, want 0", got)
	}
	// 100ms writes in the current window.
	for i := 0; i < 10; i++ {
		r.Record(100 * time.Millisecond)
	}
	got := r.AvgMs(time.Second)
	if got < 90 || got > 110 {
		t.Errorf("avg = %gms, want ~100ms", got)
	}
	// A fast write in the same window pulls the average down (mix of the
	// slow and fast writes ≈ 50ms).
	for i := 0; i < 10; i++ {
		r.Record(0 * time.Millisecond)
	}
	got = r.AvgMs(time.Second)
	if got < 40 || got > 60 {
		t.Errorf("avg after fast writes = %gms, want ~50ms", got)
	}
}

// TestWriteRecorderStalePrune verifies the average decays as samples age out
// of the window (a receiver that started slow then drained stops reducing
// the rate without any explicit "clear").
func TestWriteRecorderStalePrune(t *testing.T) {
	r := &WriteRecorder{}
	for i := 0; i < 5; i++ {
		r.Record(80 * time.Millisecond)
	}
	if got := r.AvgMs(time.Second); got < 70 {
		t.Fatalf("avg = %gms, want ~80ms before aging", got)
	}
	// Age the samples past the window.
	time.Sleep(1100 * time.Millisecond)
	if got := r.AvgMs(time.Second); got != 0 {
		t.Errorf("avg after window expiry = %gms, want 0 (stale samples pruned)", got)
	}
}

// TestProcProviderDiskDelta verifies the disk util/await fix: counters in
// /proc/diskstats are cumulative since boot, so a host with heavy past IO
// must NOT report a 90%+ utilisation on an idle delta (the old proxy read
// the raw cumulative io_ticks and would have pinned the limiter at the
// floor forever).
func TestProcProviderDiskDelta(t *testing.T) {
	dir := t.TempDir()
	disk := dir + "/diskstats"
	// One whole disk (sda). Layout: major minor name rd_ios rd_merges
	// rd_sectors rd_ticks wr_ios wr_merges wr_sectors wr_ticks io_ticks ...
	writeFile(t, disk, "8 0 sda 100 0 2000 50 200 0 4000 60 110 0 0 0 0 0 0 0\n")
	p := NewProcProvider()
	p.DiskPath = disk
	_ = p.Snapshot() // prime the baseline
	// A small delta over a large elapsed time: idle disk.
	writeFile(t, disk, "8 0 sda 105 0 2100 55 205 0 4100 65 115 0 0 0 0 0 0 0\n")
	time.Sleep(1100 * time.Millisecond) // ~1.1s between samples
	m := p.Snapshot()
	// await = (5+5)ms / (5+5) ios = 1ms; util = 5ms / ~1100ms * 100 < 1%.
	if m.DiskUtil > 10 {
		t.Errorf("util = %g%%, want < 10%% for a near-idle delta (cumulative-since-boot counters)", m.DiskUtil)
	}
	if m.DiskAwait < 0 || m.DiskAwait > 5 {
		t.Errorf("await = %gms, want ~1ms", m.DiskAwait)
	}
	// And the first snapshot of a fresh provider is 0 (no previous sample).
	p2 := NewProcProvider()
	p2.DiskPath = disk
	if m2 := p2.Snapshot(); m2.DiskUtil != 0 || m2.DiskAwait != 0 {
		t.Errorf("first snapshot = await %g util %g, want 0/0 (no previous sample)", m2.DiskAwait, m2.DiskUtil)
	}
}

// TestProcProviderDiskBusyDelta: a disk fully busy over the interval reports
// ~100% utilisation (the limiter's disk-util > 90 trigger still fires on a
// genuinely saturated disk).
func TestProcProviderDiskBusyDelta(t *testing.T) {
	dir := t.TempDir()
	disk := dir + "/diskstats"
	writeFile(t, disk, "8 0 sda 100 0 2000 50 200 0 4000 60 0 0 0 0 0 0 0 0\n")
	p := NewProcProvider()
	p.DiskPath = disk
	_ = p.Snapshot()
	// io_ticks grows by ~95% of the interval: a saturated disk.
	writeFile(t, disk, "8 0 sda 200 0 4000 150 300 0 6000 160 1045 0 0 0 0 0 0 0\n")
	time.Sleep(1100 * time.Millisecond)
	m := p.Snapshot()
	if m.DiskUtil < 80 {
		t.Errorf("util = %g%%, want >= 80%% for a saturated disk", m.DiskUtil)
	}
}
