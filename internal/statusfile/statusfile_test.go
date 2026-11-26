package statusfile

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriteReadLocal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "agent-source.json")
	st := Status{Side: "source", State: "serving", Units: 3, Rows: 100, Version: "test", StartedAt: time.Now()}
	if err := Write(path, st); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := ReadLocal(path)
	if err != nil {
		t.Fatalf("ReadLocal: %v", err)
	}
	if got.Side != "source" || got.State != "serving" || got.Units != 3 || got.Rows != 100 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if got.LastUpdate.IsZero() {
		t.Fatal("LastUpdate not set by Write")
	}
	// The tmp file must be gone after the atomic rename.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("leftover tmp file: %v", err)
	}
}

func TestWriteDisabledNoOp(t *testing.T) {
	if err := Write("", Status{}); err != nil {
		t.Fatalf("Write(empty) should be a no-op, got %v", err)
	}
}

func TestParse(t *testing.T) {
	st, err := Parse([]byte(`{"side":"dest","state":"released","units":7,"rows":56,"version":"v1"}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if st.Side != "dest" || st.State != "released" || st.Units != 7 || st.Rows != 56 {
		t.Fatalf("parse mismatch: %+v", st)
	}
	if _, err := Parse([]byte("{not json")); err == nil {
		t.Fatal("Parse(garbage) must fail")
	}
}

func TestAge(t *testing.T) {
	now := time.Now()
	st := &Status{LastUpdate: now.Add(-5 * time.Second)}
	if ag := Age(st, now); ag != 5*time.Second {
		t.Fatalf("Age = %v, want 5s", ag)
	}
	// A future LastUpdate (clock skew) clamps to 0.
	st = &Status{LastUpdate: now.Add(time.Minute)}
	if ag := Age(st, now); ag != 0 {
		t.Fatalf("Age(future) = %v, want 0", ag)
	}
	if ag := Age(nil, now); ag != 0 {
		t.Fatalf("Age(nil) = %v, want 0", ag)
	}
}

func TestWriterStartStop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent-dest.json")
	w := NewWriter(path, 10*time.Millisecond, func() Status {
		return Status{Side: "dest", State: "serving"}
	})
	w.Start()
	// The first write is immediate.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := ReadLocal(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no status file after Start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	w.Stop()
	// A disabled writer (empty path) is a no-op.
	off := NewWriter("", 0, func() Status { return Status{} })
	off.Start()
	off.Stop()
}

// Control carries the bound control listen address (remote agents self-pick
// their port; the controller discovers it from this field).
func TestControlFieldRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent-dest.json")
	st := Status{Side: "dest", State: "serving", Control: "[::]:43879"}
	if err := Write(path, st); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := ReadLocal(path)
	if err != nil {
		t.Fatalf("ReadLocal: %v", err)
	}
	if got.Control != "[::]:43879" {
		t.Fatalf("Control = %q, want [::]:43879", got.Control)
	}
	// omitempty: an agent without a control address must not emit the key,
	// so pre-fix files and local agents stay parseable either way.
	st2 := Status{Side: "source", State: "serving"}
	if err := Write(path, st2); err != nil {
		t.Fatalf("Write: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}
	if strings.Contains(string(raw), `"control"`) {
		t.Fatalf("empty Control must be omitted from JSON, got: %s", raw)
	}
}

// A persistently failing status path (the dir not writable by the agent
// user — the 26.11 r5 incident) must be reported, exactly once, instead of
// failing silently for the whole run (the heartbeat would otherwise repeat
// the error forever, and the controller's control-port discovery would
// time out with no trace of the cause on the agent side).
func TestWriterReportsWriteErrorOnce(t *testing.T) {
	ro := filepath.Join(t.TempDir(), "ro")
	if err := os.MkdirAll(ro, 0o500); err != nil {
		t.Fatalf("mkdir ro: %v", err)
	}
	path := filepath.Join(ro, "deep", "agent-dest.json")
	var mu sync.Mutex
	var errs []error
	w := NewWriter(path, 10*time.Millisecond, func() Status { return Status{Side: "dest"} })
	w.ErrLog = func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}
	w.Start()
	time.Sleep(80 * time.Millisecond) // >= 4 heartbeats
	w.Stop()
	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 1 {
		t.Fatalf("write error reported %d times, want exactly 1", len(errs))
	}
}
