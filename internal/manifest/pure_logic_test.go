package manifest

// Pure-logic unit tests for the manifest package (P7). These exercise the
// control-plane logic (job-id derivation, EOF classification, journal metadata
// round-trip, the F-12 small-table compression decision, and the rate-limit
// shift event) with NO data path: the Manager is constructed but Run is never
// called, so no two-agent runner / source agent / PG data COPY is involved.
//
// P7 removed the in-controller data path (single topology) and the
// controller's snapshot machinery; the data COPY now runs only in the
// source/dest agents (two-agent). The fake-conn data-path unit tests that
// exercised the removed in-controller copy are deleted — their coverage is
// carried by the two-agent e2e (the gp6lab-tagged integration tests and the
// lab e2e). Only this pure control-plane logic remains as a default unit
// test (no build tag, no live cluster, no source agent binary).

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// ---- shared pure helpers (no data path) ------------------------------------

// configDefault returns a copy of the default config for a test to mutate.
func configDefault() *config.Config {
	c := config.Default()
	return &c
}

// newPair builds two fake streaming conns (source + destination). The source
// serves a binary COPY payload and the destination records COPY ... FROM STDIN
// and re-reads its own payload for verify. Pure construction (no Run).
func newPair(t *testing.T, srcPayload, dstPayload []byte) (src *pg.FakeConn, dst *pg.FakeConn) {
	t.Helper()
	src = pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	src.OnReadCopy("TO STDOUT", srcPayload)

	dst = pg.NewFakeConn()
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", dstPayload)
	dst.On("pg_class", [][]any{})
	return src, dst
}

// newMgr builds a Manager over the given (possibly fake) streaming conns and a
// file-mirrored journal. Pure construction (no Run); used by the pure logic
// tests and the bench orchestration test.
func newMgr(t *testing.T, srcQ, dstQ pg.StreamingQuerier, mutate func(*config.Config)) *Manager {
	t.Helper()
	c := configDefault()
	if mutate != nil {
		mutate(c)
	}
	return NewManager(c, srcQ, dstQ, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
}

// newManagerAt builds a Manager whose journal mirror (and thus its
// events.jsonl) lives under a fresh temp dir, and returns that dir so a test
// can read the events the run emitted. Pure construction (no Run).
func newManagerAt(t *testing.T, cfg *config.Config, srcPayload, dstPayload []byte) (*Manager, string) {
	t.Helper()
	src, dst := newPair(t, srcPayload, dstPayload)
	dir := t.TempDir()
	j := NewJournal(pg.NewFakeConn(), filepath.Join(dir, "journal.jsonl"))
	return NewManager(cfg, src, dst, j), dir
}

// eventsOf reads the events.jsonl under dir into a slice of records.
func eventsOf(t *testing.T, dir string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // a lazy-open EventLog with no events never creates the file
		}
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

// findEvent returns the records of the given kind.
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

// ---- pure logic tests ------------------------------------------------------

// TestJobID checks the stable job-id derivation (FR-10).
func TestJobID(t *testing.T) {
	if jobID(nil) != "gpstream" {
		t.Errorf("nil plan -> gpstream, got %q", jobID(nil))
	}
	if jobID(&plan.Plan{Hash: ""}) != "gpstream" {
		t.Errorf("empty hash -> gpstream")
	}
	if got := jobID(&plan.Plan{Hash: "abc123"}); got != "abc123" {
		t.Errorf("jobID = %q, want abc123", got)
	}
}

// TestIsEOF verifies the EOF classification used by the copy transfer.
func TestIsEOF(t *testing.T) {
	if !isEOF(io.EOF) {
		t.Error("io.EOF should be classified as EOF")
	}
	if isEOF(errors.New("other")) {
		t.Error("non-EOF error should not be classified as EOF")
	}
}

// TestJobMetaMirrorRoundTrip: the per-operation metadata (snapshot epoch +
// copy mode) round-trips through the journal mirror; a fresh operation has no
// metadata (nil, no error).
func TestJobMetaMirrorRoundTrip(t *testing.T) {
	mirror := t.TempDir() + "/journal.jsonl"
	j := NewJournal(nil, mirror)
	if err := j.SaveMeta("op1", &JobMeta{JobID: "op1", SnapEpoch: 4, CopyMode: "on_segment"}); err != nil {
		t.Fatalf("save meta: %v", err)
	}
	j2 := NewJournal(nil, mirror)
	meta, err := j2.LoadMeta("op1")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if meta == nil || meta.SnapEpoch != 4 || meta.CopyMode != "on_segment" {
		t.Fatalf("meta = %+v, want epoch=4 on_segment", meta)
	}
	j3 := NewJournal(nil, t.TempDir()+"/journal.jsonl")
	if m, err := j3.LoadMeta("op2"); err != nil || m != nil {
		t.Fatalf("fresh meta = %+v, err=%v; want nil, nil", m, err)
	}
}

// TestCompressionOffForDecision locks the F-12 small-table auto-disable
// predicate: a table below --small-table-threshold rides the transport with
// compression OFF (except in --compress none mode, where it is a no-op), and a
// zero threshold disables the auto-disable entirely.
func TestCompressionOffForDecision(t *testing.T) {
	cases := []struct {
		name      string
		comp      config.Compression
		threshold int64
		bytes     int64
		want      bool
	}{
		{"none-mode-never-off", config.CompNone, 1 << 20, 100, false},
		{"zstd-small-off", config.CompZstd, 1 << 20, 1 << 19, true},
		{"zstd-large-on", config.CompZstd, 1 << 20, 2 << 20, false},
		{"zstd-at-threshold-on", config.CompZstd, 1 << 20, 1 << 20, false},
		{"zlib-small-off", config.CompZlib, 1 << 20, 1024, true},
		{"zero-threshold-disabled", config.CompZstd, 0, 100, false},
		{"unknown-size-keeps-mode", config.CompZstd, 1 << 20, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Compression = tc.comp
			cfg.SmallTableThreshold = tc.threshold
			m, _ := newManagerAt(t, &cfg, []byte("data"), []byte("data"))
			got := m.compressionOffFor(plan.Table{Bytes: tc.bytes})
			if got != tc.want {
				t.Fatalf("compressionOffFor(%s, thr=%d, bytes=%d) = %v, want %v",
					tc.comp, tc.threshold, tc.bytes, got, tc.want)
			}
		})
	}
}

// TestUnitCompressorsSwap: a small table gets the none-mode pair; a large
// table keeps the operator's compression pair.
func TestUnitCompressorsSwap(t *testing.T) {
	cfg := config.Default()
	cfg.Compression = config.CompZstd
	cfg.SmallTableThreshold = 1 << 20
	m, _ := newManagerAt(t, &cfg, []byte("data"), []byte("data"))

	comp, dec := m.unitCompressors(plan.Table{Bytes: 1 << 19})
	if comp.Mode() != compress.ModeNone || dec.Mode() != compress.ModeNone {
		t.Fatalf("small table must get the none pair, got comp=%v dec=%v", comp.Mode(), dec.Mode())
	}
	comp, dec = m.unitCompressors(plan.Table{Bytes: 2 << 20})
	if comp.Mode() != compress.ModeZstd || dec.Mode() != compress.ModeZstd {
		t.Fatalf("large table must keep zstd, got comp=%v dec=%v", comp.Mode(), dec.Mode())
	}
}

// TestManagerOnRateChangeEmitsEvent: a rate shift is recorded in events.jsonl
// (rate_limit_shift) and reflected in the rate-limit metric (FR-04/F-14).
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

// TestReReadTablesRuleTable locks the T5 keep-rule: only tables whose
// completed units span more than one snapshot epoch - or that carry a
// non-terminal unit - are re-copied under the fresh resume snapshot; a table
// completed in a single epoch is kept (pure function, no Run).
func TestReReadTablesRuleTable(t *testing.T) {
	mk := func(state State, epoch int64) *Unit {
		return &Unit{DB: "db", Schema: "s", Table: "t", State: state, Epoch: epoch}
	}
	mkSame := func(state State, epoch int64) *Unit {
		// A SECOND unit of the SAME table (e.g. a partition) - the epoch
		// rules group by logical table, so two units of "t" in different
		// epochs break the table's single-snapshot invariant.
		return &Unit{DB: "db", Schema: "s", Table: "t", State: state, Epoch: epoch}
	}
	mkOther := func(state State, epoch int64) *Unit {
		return &Unit{DB: "db", Schema: "s", Table: "t2", State: state, Epoch: epoch}
	}
	tests := []struct {
		name   string
		units  []*Unit
		wantRR map[string]bool // expected re-read set
	}{
		{
			name:   "relaxed: one epoch completed -> keep",
			units:  []*Unit{mk(StateCompleted, 1)},
			wantRR: map[string]bool{},
		},
		{
			name:   "relaxed: swapped -> keep (swap moment)",
			units:  []*Unit{mk(StateSwapped, 1)},
			wantRR: map[string]bool{},
		},
		{
			name:   "relaxed: swapped + completed same epoch -> keep",
			units:  []*Unit{mk(StateSwapped, 1), mkOther(StateCompleted, 1)},
			wantRR: map[string]bool{},
		},
		{
			name:   "relaxed: two units of one table, mixed epochs -> re-read",
			units:  []*Unit{mk(StateCompleted, 1), mkSame(StateCompleted, 2)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name:   "relaxed: two units of one table, one epoch -> keep",
			units:  []*Unit{mk(StateCompleted, 1), mkSame(StateCompleted, 1)},
			wantRR: map[string]bool{},
		},
		{
			name:   "relaxed: streaming unit -> re-read",
			units:  []*Unit{mk(StateCopying, 1), mkOther(StateCompleted, 1)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name:   "relaxed: failed unit -> re-read",
			units:  []*Unit{mk(StateFailed, 1)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name: "relaxed: other table untouched",
			units: []*Unit{mk(StateCompleted, 1), mkSame(StateCompleted, 2),
				{DB: "db", Schema: "s", Table: "t3", State: StateCompleted, Epoch: 2}},
			wantRR: map[string]bool{"db|s|t": true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := reReadTables(tc.units)
			if len(got) != len(tc.wantRR) {
				t.Fatalf("re-read set = %v, want %v", got, tc.wantRR)
			}
			for k := range tc.wantRR {
				if !got[k] {
					t.Fatalf("re-read set = %v, want %v", got, tc.wantRR)
				}
			}
		})
	}
}

// TestEstablishEpochFreshThenResume: a fresh run starts at epoch 1; the next
// run over the same mirror continues at lastEpoch+1 (DESIGN §2.2).
func TestEstablishEpochFreshThenResume(t *testing.T) {
	cfg := config.Default()
	mirror := t.TempDir() + "/journal.jsonl"

	m1 := NewManager(&cfg, pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), mirror))
	m1.jobID = "op"
	e1, resume1, err := m1.establishEpoch()
	if err != nil {
		t.Fatalf("establish 1: %v", err)
	}
	if e1 != 1 || resume1 {
		t.Fatalf("fresh run: epoch=%d resume=%v, want 1/false", e1, resume1)
	}

	m2 := NewManager(&cfg, pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), mirror))
	m2.jobID = "op"
	if err := m2.journal.LoadFromMirror(); err != nil {
		t.Fatalf("load mirror: %v", err)
	}
	e2, resume2, err := m2.establishEpoch()
	if err != nil {
		t.Fatalf("establish 2: %v", err)
	}
	if e2 != 2 || !resume2 {
		t.Fatalf("resume run: epoch=%d resume=%v, want 2/true", e2, resume2)
	}
}

// TestEstablishEpochErrors covers establishEpoch's LoadMeta and SaveMeta
// failure returns.
func TestEstablishEpochErrors(t *testing.T) {
	bad := badParentFile(t)
	j := NewJournal(pg.NewFakeConn(), filepath.Join(bad, "journal.jsonl"))
	m := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), j)
	m.jobID = "x"
	if _, _, err := m.establishEpoch(); err == nil {
		t.Error("establishEpoch under a file-parent: want error")
	}
}

// TestConflictErrorString covers ConflictError.Error.
func TestConflictErrorString(t *testing.T) {
	e := &ConflictError{Schema: "s", Table: "t"}
	if !strings.Contains(e.Error(), "s.t") || !strings.Contains(e.Error(), "conflict") {
		t.Errorf("ConflictError.Error = %q; want it to name s.t and say conflict", e.Error())
	}
}

// badParentFile creates a directory whose child "f" is a REGULAR FILE, so any
// MkdirAll/OpenFile/WriteFile under f/ fails (OS error branch coverage).
func badParentFile(t *testing.T) string {
	t.Helper()
	base := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(base, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed parent file: %v", err)
	}
	return base
}
