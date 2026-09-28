// EventLog is the operator's diagnostic event stream (spec F-04 / F-14):
// "все диагностические события (снижение скорости, конфликты, ретраи) — в
// отдельный лог events.jsonl" (F-04) and "каждый сдвиг лимита —
// диагностическое сообщение в events.jsonl" (F-14). It appends one
// JSON object per line (ts, kind, then the event's fields) so the file is
// both human-readable and machine-consumable (jq -s, log tailers).
//
// The log is a best-effort diagnostic: a write failure must never fail the
// copy (the journal is the durable record, events are the operator view),
// so open/write errors are swallowed (F-04 does not require event
// durability). A nil path makes the log a no-op — the same convention as
// Journal's empty mirror — so tests and in-memory runs stay side-effect
// free.
package monitor

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// EventLog appends diagnostic events as JSON lines. It is safe for
// concurrent use (multiple copy workers log at once, FR-03).
type EventLog struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// NewEventLog opens (lazily) the events file. An empty path returns a
// no-op log: Log then does nothing, so callers can always invoke it.
// The file is created on first write, not here, so a run that emits no
// events leaves no empty file behind.
func NewEventLog(path string) *EventLog {
	return &EventLog{path: path}
}

// Log appends one event: {"ts": <RFC3339Nano>, "kind": <kind>, <fields...>}.
// fields must be alternating key, value pairs; an odd tail is dropped.
// It never fails (diagnostic only): open or write errors are swallowed.
func (l *EventLog) Log(kind string, fields ...any) {
	if l.path == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := map[string]any{"ts": time.Now().UTC().Format(time.RFC3339Nano), "kind": kind}
	for i := 0; i+1 < len(fields); i += 2 {
		if k, ok := fields[i].(string); ok {
			rec[k] = fields[i+1]
		}
	}
	f, err := l.openLocked()
	if err != nil {
		return
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return // unmarshalable field values are operator-side bugs; skip
	}
	line = append(line, '\n')
	if _, err := f.Write(line); err != nil {
		// The file may have been removed mid-run (operator cleanup):
		// forget it; the next Log re-opens.
		if l.f == f {
			l.f = nil
		}
	}
}

// openLocked lazily opens the events file in append mode. Callers hold l.mu.
func (l *EventLog) openLocked() (*os.File, error) {
	if l.f != nil {
		return l.f, nil
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	l.f = f
	return f, nil
}

// Close flushes and closes the events file. Safe to call on a no-op log
// and more than once.
func (l *EventLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
}
