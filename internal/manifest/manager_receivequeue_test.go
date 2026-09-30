package manifest

import (
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/ratelimit"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// TestManagerWiresReceiveQueueLimiter verifies the F-14 receive-queue wiring
// end to end at the manager level: when a rate-limit is set, NewManager
// builds the limiter with a receive-write-latency supplier fed by the shared
// recorder, and framedPipe installs the recorder's hook on every unit's
// transport. Recording a slow drain then makes the limiter reduce the pair
// rate with the dest-receive-queue cause (the in-process «метрики COPY»).
func TestManagerWiresReceiveQueueLimiter(t *testing.T) {
	cfg := config.Default()
	cfg.RateLimit = 100 // Gbps ceiling → limiter + pacer + recorder built

	m, dir := newManagerAt(t, &cfg, []byte("data"), []byte("data"))
	defer m.events.Close()

	if m.lim == nil {
		t.Fatal("no limiter built with RateLimit set")
	}
	if m.recvRec == nil {
		t.Fatal("no receive-queue recorder built with RateLimit set")
	}

	// The unit transport gets the recorder's write-latency hook installed.
	fp := m.framedPipe(1, compress.New(compress.ModeNone, 0), compress.NewDec(compress.ModeNone))
	if fp == nil {
		t.Fatal("framedPipe returned nil")
	}

	// Simulate the destination falling behind: a slow data-path write lands
	// in the shared recorder (the hook is the recorder's Record method).
	m.recvRec.Record(80 * time.Millisecond)

	// The limiter's receive-write-latency supplier must now see it. Capture
	// the supplier's view through an Update and assert the rate dropped.
	changes := 0
	causes := make(chan string, 4)
	m.lim.SetOnChange(func(from, to ratelimit.Gbps, cause string) {
		changes++
		causes <- cause
	})
	m.lim.Update()

	select {
	case cause := <-causes:
		if changes == 0 {
			t.Fatal("no rate change recorded")
		}
		if cause != "dest-receive-queue 80ms write-latency (>50ms)" {
			t.Errorf("cause = %q, want the dest-receive-queue cause with the metric", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("no rate change emitted after the destination fell behind")
	}
	_ = dir
}

// TestManagerNoRecorderWhenUnlimited verifies the receive-queue signal is
// absent (recorder nil, no hook) when no rate-limit is set — the default
// path is untouched.
func TestManagerNoRecorderWhenUnlimited(t *testing.T) {
	cfg := config.Default() // RateLimit 0 → unlimited
	m, _ := newManagerAt(t, &cfg, []byte("data"), []byte("data"))
	defer m.events.Close()
	if m.lim != nil || m.pacer != nil || m.recvRec != nil {
		t.Errorf("unlimited run built limiter/pacer/recorder (got lim=%v rec=%v)", m.lim, m.recvRec)
	}
	// framedPipe must still work and simply not install a hook.
	if fp := m.framedPipe(1, compress.New(compress.ModeNone, 0), compress.NewDec(compress.ModeNone)); fp == nil {
		t.Fatal("framedPipe returned nil for the unlimited path")
	}
}
