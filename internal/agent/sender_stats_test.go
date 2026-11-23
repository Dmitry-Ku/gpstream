package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// pullStatsFiles (relay increment 2): parallel per-host fetch + line parse.

func TestPullStatsFilesParsesAllHosts(t *testing.T) {
	line := func(c int64) string {
		return fmt.Sprintf(`{"job":1,"unit":2,"content":%d,"rows":100,"retransmitted":3,"reconnects":1,"ack_lag_max_ms":42}`, c)
	}
	hosts := []string{"h1", "h2", "h3"}
	fetch := func(host string) (string, error) {
		switch host {
		case "h1":
			return line(1) + "\n", nil
		case "h2":
			return line(0) + "\n", nil
		case "h3":
			return line(2) + "\n", nil
		}
		return "", errors.New("unknown host")
	}
	st := pullStatsFiles(context.Background(), hosts, fetch)
	if len(st) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(st), st)
	}
	for i, want := range []int64{0, 1, 2} { // sorted by content
		if st[i].Content != want {
			t.Errorf("entry %d content = %d, want %d", i, st[i].Content, want)
		}
	}
	if st[0].Retransmitted != 3 || st[0].Reconnects != 1 || st[0].AckLagMaxMs != 42 || st[0].Rows != 100 {
		t.Errorf("fields = %+v, want retrans=3 reconns=1 lag=42ms rows=100", st[0])
	}
}

// Unreachable hosts do not fail the pull; corrupt lines are dropped, the
// rest kept.
func TestPullStatsFilesToleratesFailures(t *testing.T) {
	fetch := func(host string) (string, error) {
		switch host {
		case "ok":
			return `{"job":1,"unit":2,"content":7,"rows":1}` + "\ncorrupt-line" + "\n" +
				`{"job":1,"unit":2,"content":8,"rows":2}` + "\n", nil
		case "dead":
			return "", errors.New("ssh: no route to host")
		}
		return "", nil // no file for the unit: contributes nothing
	}
	st := pullStatsFiles(context.Background(), []string{"ok", "dead", "empty"}, fetch)
	if len(st) != 2 {
		t.Fatalf("got %d entries, want 2 (corrupt dropped, dead skipped): %+v", len(st), st)
	}
	if st[0].Content != 7 || st[1].Content != 8 {
		t.Errorf("contents = %d,%d, want 7,8", st[0].Content, st[1].Content)
	}
}

// localStatsFetch: the single-host layout — the per-(job,unit) glob matches
// this unit only (stale files of other units are excluded).
func TestLocalStatsFetchPerUnitGlob(t *testing.T) {
	dir := t.TempDir()
	w := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w("sender-5-6-seg0.json", `{"job":5,"unit":6,"content":0,"rows":9,"retransmitted":2,"reconnects":0,"ack_lag_max_ms":1}`)
	w("sender-5-6-seg1.json", `{"job":5,"unit":6,"content":1,"rows":9,"retransmitted":0,"reconnects":0,"ack_lag_max_ms":0}`)
	w("sender-5-7-seg0.json", `{"job":5,"unit":7,"content":0,"rows":99,"retransmitted":99,"reconnects":0,"ack_lag_max_ms":0}`) // other unit
	fetch := localStatsFetch(dir, "sender-5-6-seg*.json")
	data, err := fetch("local")
	if err != nil {
		t.Fatal(err)
	}
	st := pullStatsFiles(context.Background(), []string{"local"}, fetch)
	if len(st) != 2 {
		t.Fatalf("got %d entries, want 2 (stale unit excluded): %+v", len(st), st)
	}
	if st[0].Retransmitted != 2 || st[1].Content != 1 {
		t.Errorf("parsed = %+v", st)
	}
	if data == "" {
		t.Error("fetch returned no data")
	}
}

// StatsDirSuffix: the single choke point's validation (relay increment 2).
func TestStatsDirSuffix(t *testing.T) {
	if s, err := segstream.StatsDirSuffix(""); err != nil || s != "" {
		t.Errorf("empty dir: suffix=%q err=%v, want both empty", s, err)
	}
	got, err := segstream.StatsDirSuffix("/tmp/gpstream-stats-op-1")
	if err != nil || got != " --stats-dir /tmp/gpstream-stats-op-1" {
		t.Errorf("valid dir: suffix=%q err=%v", got, err)
	}
	for _, bad := range []string{
		"/etc/passwd",              // outside the fixed namespace
		"relative/path",            // not absolute
		"/tmp/gpstream-stats-;rm",  // shell metacharacter
		"/tmp/gpstream-stats-a b",  // whitespace
		"/tmp/gpstream-stats-$(x)", // command substitution
	} {
		if _, err := segstream.StatsDirSuffix(bad); err == nil {
			t.Errorf("bad dir %q accepted", bad)
		}
	}
	long := "/tmp/gpstream-stats-" + string(make([]byte, 300))
	if _, err := segstream.StatsDirSuffix(long); err == nil {
		t.Error("over-long dir accepted")
	}
}
