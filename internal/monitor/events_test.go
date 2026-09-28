package monitor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// readLines returns the non-empty lines of a file.
func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

// TestEventLogWritesJSONLines verifies the events.jsonl format: one JSON
// object per line with ts, kind and the event's fields (spec F-04/F-14).
func TestEventLogWritesJSONLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	l := NewEventLog(path)
	l.Log("rate_limit_shift", "from_gbps", 5.0, "to_gbps", 2.5, "reason", "cpu 90%")
	l.Log("conflict", "schema", "public", "table", "orders", "action", "stop")
	l.Close()

	lines := readLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %v", len(lines), lines)
	}
	var r0, r1 map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &r0); err != nil {
		t.Fatalf("line 0 not JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &r1); err != nil {
		t.Fatalf("line 1 not JSON: %v", err)
	}
	if r0["kind"] != "rate_limit_shift" {
		t.Errorf("kind = %v, want rate_limit_shift", r0["kind"])
	}
	if r0["from_gbps"] != 5.0 || r0["to_gbps"] != 2.5 || r0["reason"] != "cpu 90%" {
		t.Errorf("fields = %v, want from_gbps=5 to_gbps=2.5 reason=cpu 90%%", r0)
	}
	if r0["ts"] == nil {
		t.Error("missing ts")
	}
	if r1["kind"] != "conflict" || r1["table"] != "orders" || r1["action"] != "stop" {
		t.Errorf("line 1 = %v, want conflict/orders/stop", r1)
	}
}

// TestEventLogNoOp verifies an empty path is a no-op: Log never fails and no
// file is created (tests and in-memory runs stay side-effect free).
func TestEventLogNoOp(t *testing.T) {
	dir := t.TempDir()
	l := NewEventLog("")
	l.Log("conflict", "table", "x") // must not panic
	l.Close()                       // must not panic
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("no-op log created files: %v", entries)
	}
}

// TestEventLogLazyOpen verifies the file appears only on the first write, so
// a run that emits no events leaves no empty file behind.
func TestEventLogLazyOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	l := NewEventLog(path)
	l.Close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("file created before first write (err=%v)", err)
	}
}

// TestEventLogOddFieldDropped verifies an odd fields tail is dropped
// gracefully (a caller bug must not corrupt the stream or panic).
func TestEventLogOddFieldDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	l := NewEventLog(path)
	l.Log("unit_failed", "schema", "public", "table", "orders", "error", "boom", "orphan")
	l.Close()
	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if _, ok := rec["orphan"]; ok {
		t.Error("odd tail field must be dropped")
	}
	if rec["error"] != "boom" {
		t.Errorf("error = %v, want boom", rec["error"])
	}
}

// TestEventLogConcurrent verifies concurrent Log from many workers (FR-03
// pool) produces exactly one valid JSON line per event (run with -race).
func TestEventLogConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	l := NewEventLog(path)
	const n = 8
	const per = 50
	var wg sync.WaitGroup
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				l.Log("unit_failed", "worker", g, "i", i)
			}
		}(g)
	}
	wg.Wait()
	l.Close()
	lines := readLines(t, path)
	if len(lines) != n*per {
		t.Fatalf("got %d lines, want %d", len(lines), n*per)
	}
	for i, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d not JSON: %v", i, err)
		}
	}
}

// TestEventLogCloseIdempotent verifies Close can be called repeatedly and
// after writes, without error.
func TestEventLogCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	l := NewEventLog(path)
	l.Log("a")
	l.Close()
	l.Close()
	l.Log("after-close") // still safe (re-opens); not asserted
}
