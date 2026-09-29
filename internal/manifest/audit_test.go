package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// auditLines reads and decodes every line of an audit.log into maps.
func auditLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit.log: %v", err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("audit line not JSON: %v: %q", err, line)
		}
		out = append(out, rec)
	}
	return out
}

// TestAuditLogWritesJSONLines (F-13): actions land in audit.log as one
// JSON object per line with ts + action, and the file is 0600 (it names
// operator actions on the operation).
func TestAuditLogWritesJSONLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "audit.log")
	l := NewAuditLog(p)
	l.Log("run", "operation-id", "op1", "units", 3)
	l.Log("cleanup", "operation-id", "op1")
	l.Close()

	recs := auditLines(t, p)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2: %v", len(recs), recs)
	}
	if recs[0]["action"] != "run" || recs[0]["operation-id"] != "op1" || recs[0]["units"] != 3.0 {
		t.Errorf("record 0 = %v", recs[0])
	}
	if recs[1]["action"] != "cleanup" {
		t.Errorf("record 1 = %v", recs[1])
	}
	for _, r := range recs {
		ts, _ := r["ts"].(string)
		if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
			t.Errorf("ts %q not RFC3339Nano: %v", ts, err)
		}
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("audit.log mode = %v, want 0600", fi.Mode().Perm())
	}
}

// TestAuditLogNoop verifies an empty path is a no-op (no file, no panic):
// commands without a state dir must stay side-effect free.
func TestAuditLogNoop(t *testing.T) {
	l := NewAuditLog("")
	l.Log("run", "operation-id", "op1")
	l.Close()
	l.Close() // double close is safe
}

// TestJournalAuditPath is the path convention: audit.log alongside the
// journal mirror (like events.jsonl / conflicts.jsonl).
func TestJournalAuditPath(t *testing.T) {
	mirror := t.TempDir() + "/journal.jsonl"
	j := NewJournal(nil, mirror)
	want := filepath.Join(filepath.Dir(mirror), "audit.log")
	if got := j.AuditPath(); got != want {
		t.Errorf("AuditPath = %q, want %q", got, want)
	}
	if NewJournal(nil, "").AuditPath() != "" {
		t.Error("empty mirror must give an empty audit path")
	}
}
