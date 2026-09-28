package manifest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// eventsOf reads and decodes every line of the run's events.jsonl into maps,
// so a test can assert on the event kinds and fields the Manager emitted
// (spec F-04/F-14).
func eventsOf(t *testing.T, dir string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("event line not JSON: %v: %q", err, line)
		}
		out = append(out, rec)
	}
	return out
}

func findEvent(t *testing.T, evs []map[string]any, kind string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, e := range evs {
		if e["kind"] == kind {
			out = append(out, e)
		}
	}
	return out
}

// TestManagerStrictConflictEmitsEvent verifies the strict-default conflict
// (FR-10 §8) is recorded in events.jsonl as a conflict event with
// action=stop (spec F-04: conflicts are a diagnostic event).
func TestManagerStrictConflictEmitsEvent(t *testing.T) {
	cfg := config.Default()
	m, dir := newManagerAt(t, &cfg, []byte("data"), []byte("data"))

	p := oneUnitPlan()
	p.Units[0].HasConflict = true

	if err := m.Run(context.Background(), p); err == nil {
		t.Fatal("run should stop on a strict conflict")
	}
	evs := eventsOf(t, dir)
	stops := findEvent(t, evs, "conflict")
	if len(stops) != 1 {
		t.Fatalf("got %d conflict events, want 1: %v", len(stops), evs)
	}
	if stops[0]["action"] != "stop" {
		t.Errorf("action = %v, want stop", stops[0]["action"])
	}
	if stops[0]["table"] != "orders" {
		t.Errorf("table = %v, want orders", stops[0]["table"])
	}
}

// TestManagerSkipConflictEmitsEvent verifies the --skip-conflicts path
// records a conflict event with action=skip (spec F-04).
func TestManagerSkipConflictEmitsEvent(t *testing.T) {
	cfg := config.Default()
	cfg.SkipConflicts = true
	m, dir := newManagerAt(t, &cfg, []byte("data"), []byte("data"))

	p := oneUnitPlan()
	p.Units[0].HasConflict = true

	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	evs := eventsOf(t, dir)
	skips := findEvent(t, evs, "conflict")
	if len(skips) != 1 {
		t.Fatalf("got %d conflict events, want 1: %v", len(skips), evs)
	}
	if skips[0]["action"] != "skip" {
		t.Errorf("action = %v, want skip", skips[0]["action"])
	}
}

// TestManagerUnitFailureEmitsEvent verifies a failed unit (checksum mismatch)
// is recorded in events.jsonl as a unit_failed event with the error (spec
// F-04: retries/failures are a diagnostic event).
func TestManagerUnitFailureEmitsEvent(t *testing.T) {
	cfg := config.Default()
	m, dir := newManagerAt(t, &cfg, []byte("source-data"), []byte("different-dest-data"))

	if err := m.Run(context.Background(), oneUnitPlan()); err == nil {
		t.Fatal("run should fail on checksum mismatch")
	}
	evs := eventsOf(t, dir)
	fails := findEvent(t, evs, "unit_failed")
	if len(fails) != 1 {
		t.Fatalf("got %d unit_failed events, want 1: %v", len(fails), evs)
	}
	if fails[0]["table"] != "orders" {
		t.Errorf("table = %v, want orders", fails[0]["table"])
	}
	if fails[0]["error"] == nil || fails[0]["error"] == "" {
		t.Error("unit_failed event must carry the error")
	}
}

// TestManagerOnRateChangeEmitsEvent verifies F-14 directly: a limiter rate
// shift logs a rate_limit_shift event with the old/new pair and the reason,
// and still updates the metric.
func TestManagerOnRateChangeEmitsEvent(t *testing.T) {
	cfg := config.Default()
	m, dir := newManagerAt(t, &cfg, []byte("data"), []byte("data"))
	m.onRateChange(5.0, 2.5, "cpu 90%")
	m.events.Close()

	evs := eventsOf(t, dir)
	shifts := findEvent(t, evs, "rate_limit_shift")
	if len(shifts) != 1 {
		t.Fatalf("got %d rate_limit_shift events, want 1: %v", len(shifts), evs)
	}
	if shifts[0]["from_gbps"] != 5.0 || shifts[0]["to_gbps"] != 2.5 || shifts[0]["reason"] != "cpu 90%" {
		t.Errorf("shift = %v, want from=5 to=2.5 reason=cpu 90%%", shifts[0])
	}
	if m.Metrics().RateLimit != 2.5 {
		t.Errorf("metric rate = %v, want 2.5", m.Metrics().RateLimit)
	}
}

// newManagerAt builds a Manager whose journal mirror (and thus its
// events.jsonl) lives under a fresh temp dir, and returns that dir so a
// test can read the events the run emitted.
func newManagerAt(t *testing.T, cfg *config.Config, srcPayload, dstPayload []byte) (*Manager, string) {
	t.Helper()
	src, dst := newPair(t, srcPayload, dstPayload)
	dir := t.TempDir()
	j := NewJournal(pg.NewFakeConn(), filepath.Join(dir, "journal.jsonl"))
	return NewManager(cfg, src, dst, j), dir
}
