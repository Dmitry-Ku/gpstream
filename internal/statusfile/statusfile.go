// Package statusfile is the 2D-C (architecture 08.10) live-agent status
// channel: each agent (source + destination) writes a small live status
// JSON file on its OWN coordinator host, and the controller's `status`
// command reads both (locally, or over the same SSH it already uses to drive
// the agents) to report the live agents' state.
//
// Why a file, not a control-socket poll: the agent's control socket is
// ONE-SHOT (a single Accept; the crash-recovery matrix depends on the agent
// exiting when that one connection drops), so a separate `status` process
// cannot dial a running agent. A file the agent heartbeats needs no new
// listener, survives the agent's death (the last heartbeat remains — `status`
// tells a dead agent from a live one by the file's age), and mirrors the
// existing runstats.json pattern (the manager already heartbeats a live file
// the controller reads).
//
// The file is a SNAPSHOT, not a log: it is rewritten atomically
// (tmp + rename) every heartbeat, so a reader never sees a torn write.
//
// The package imports nothing from internal/agent (the agents import it —
// the reverse would be an import cycle); the controller's SSH read is done
// by the caller (which imports both).
package statusfile

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Status is one agent's live state (the snapshot the file holds).
type Status struct {
	Side       string    `json:"side"` // "source" | "dest"
	State      string    `json:"state"`
	Units      int64     `json:"units,omitempty"`
	Rows       int64     `json:"rows,omitempty"`
	Version    string    `json:"version,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	LastUpdate time.Time `json:"last_update"`
}

// Snapshot supplies the current Status for one heartbeat tick.
type Snapshot func() Status

// Writer heartbeats a Status snapshot to a path (on the agent's own host).
type Writer struct {
	path     string
	interval time.Duration
	snap     Snapshot
	done     chan struct{}
	once     sync.Once
}

// NewWriter returns a writer for path (empty path = disabled: every method
// is a no-op, so one-shot / test agents that pass no --status-file are
// unaffected). interval <= 0 defaults to 2s.
func NewWriter(path string, interval time.Duration, snap Snapshot) *Writer {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Writer{path: path, interval: interval, snap: snap, done: make(chan struct{})}
}

// Start launches the heartbeat loop (a no-op when disabled). It writes once
// immediately so `status` shows a fresh file the moment the agent is up
// (before the first unit is served).
func (w *Writer) Start() {
	if w == nil || w.path == "" || w.snap == nil {
		return
	}
	go func() {
		_ = Write(w.path, w.snap())
		t := time.NewTicker(w.interval)
		defer t.Stop()
		for {
			select {
			case <-w.done:
				return
			case <-t.C:
				_ = Write(w.path, w.snap())
			}
		}
	}()
}

// Stop ends the heartbeat loop (idempotent).
func (w *Writer) Stop() {
	if w == nil {
		return
	}
	w.once.Do(func() { close(w.done) })
}

// Write atomically writes st to path (tmp in the same dir + rename),
// creating the directory if needed. Best-effort: a write error is returned
// but never fatal to the run (the status file is diagnostic, like runstats).
func Write(path string, st Status) error {
	if path == "" {
		return nil
	}
	st.LastUpdate = time.Now()
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadLocal reads a status file on the local host.
func ReadLocal(path string) (*Status, error) {
	if path == "" {
		return nil, fmt.Errorf("no status file path")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse decodes a status file's bytes (the controller uses it for the SSH
// read, where it fetches the file's contents itself).
func Parse(raw []byte) (*Status, error) {
	var st Status
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("parse status file: %w", err)
	}
	return &st, nil
}

// Age is how long ago the agent last heartbeated (the alive/stale verdict
// `status` renders: fresh = the agent is alive, old = it died and the last
// heartbeat remains).
func Age(st *Status, now time.Time) time.Duration {
	if st == nil || st.LastUpdate.IsZero() {
		return 0
	}
	if d := now.Sub(st.LastUpdate); d > 0 {
		return d
	}
	return 0
}
