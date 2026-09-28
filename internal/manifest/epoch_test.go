package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// twoUnitPlan is a two-table plan with a stable hash (the operation identity).
func t5TwoTablePlan() *plan.Plan {
	return &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100},
			{DB: "db", Schema: "public", Name: "orders2", DstSchema: "public", DstName: "orders2", Bytes: 100},
		},
		Hash: "ht5",
	}
}

// runT5 drives one Manager run over fresh fakes but the SHARED journal mirror
// (a crash leaves the mirror behind; the next run re-opens it).
func runT5(t *testing.T, mirror string, cfg config.Config) (*Manager, *pg.FakeConn, *pg.FakeConn) {
	t.Helper()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	j := NewJournal(pg.NewFakeConn(), mirror)
	m := NewManager(&cfg, src, dst, j)
	if err := m.Run(context.Background(), t5TwoTablePlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return m, src, dst
}

// TestReReadTablesRuleTable is the DESIGN §2.2 decision table.
func TestReReadTablesRuleTable(t *testing.T) {
	mk := func(state State, epoch int64) *Unit {
		return &Unit{DB: "db", Schema: "s", Table: "t", State: state, Epoch: epoch}
	}
	mkSame := func(state State, epoch int64) *Unit {
		// A SECOND unit of the SAME table (e.g. a partition) — the epoch
		// rules group by logical table, so two units of "t" in different
		// epochs break the table's single-snapshot invariant.
		return &Unit{DB: "db", Schema: "s", Table: "t", State: state, Epoch: epoch}
	}
	mkOther := func(state State, epoch int64) *Unit {
		return &Unit{DB: "db", Schema: "s", Table: "t2", State: state, Epoch: epoch}
	}
	tests := []struct {
		name    string
		mode    string
		current int64
		units   []*Unit
		wantRR  map[string]bool // expected re-read set
	}{
		{
			name: "relaxed: one epoch completed -> keep",
			mode: "relaxed", current: 2,
			units:  []*Unit{mk(StateCompleted, 1)},
			wantRR: map[string]bool{},
		},
		{
			name: "relaxed: swapped -> keep (swap moment)",
			mode: "relaxed", current: 2,
			units:  []*Unit{mk(StateSwapped, 1)},
			wantRR: map[string]bool{},
		},
		{
			name: "relaxed: swapped + completed same epoch -> keep",
			mode: "relaxed", current: 2,
			units:  []*Unit{mk(StateSwapped, 1), mkOther(StateCompleted, 1)},
			wantRR: map[string]bool{},
		},
		{
			name: "relaxed: two units of one table, mixed epochs -> re-read",
			mode: "relaxed", current: 3,
			units:  []*Unit{mk(StateCompleted, 1), mkSame(StateCompleted, 2)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name: "relaxed: two units of one table, one epoch -> keep",
			mode: "relaxed", current: 2,
			units:  []*Unit{mk(StateCompleted, 1), mkSame(StateCompleted, 1)},
			wantRR: map[string]bool{},
		},
		{
			name: "relaxed: streaming unit -> re-read",
			mode: "relaxed", current: 2,
			units:  []*Unit{mk(StateCopying, 1), mkOther(StateCompleted, 1)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name: "relaxed: failed unit -> re-read",
			mode: "relaxed", current: 2,
			units:  []*Unit{mk(StateFailed, 1)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name: "relaxed: other table untouched",
			mode: "relaxed", current: 3,
			units: []*Unit{mk(StateCompleted, 1), mkSame(StateCompleted, 2),
				{DB: "db", Schema: "s", Table: "t3", State: StateCompleted, Epoch: 2}},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name: "strict: completed older epoch -> re-read",
			mode: "strict", current: 3,
			units:  []*Unit{mk(StateCompleted, 1)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name: "strict: even current-epoch completed -> re-read (full DB point in time)",
			mode: "strict", current: 3,
			units:  []*Unit{mk(StateCompleted, 3)},
			wantRR: map[string]bool{"db|s|t": true},
		},
		{
			name: "strict: swapped -> re-read and re-swap (DESIGN §2.2)",
			mode: "strict", current: 3,
			units:  []*Unit{mk(StateSwapped, 1)},
			wantRR: map[string]bool{"db|s|t": true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := reReadTables(tc.mode, tc.current, tc.units)
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

// TestJobMetaMirrorRoundTrip is the FR-07 durable part of T5: the epoch
// survives in the jobmeta.json mirror and is read back by a new Journal.
func TestJobMetaMirrorRoundTrip(t *testing.T) {
	mirror := t.TempDir() + "/journal.jsonl"
	j := NewJournal(nil, mirror)
	if err := j.SaveMeta("op1", &JobMeta{JobID: "op1", SnapEpoch: 4, CopyMode: "on_segment", ResumeCons: "strict"}); err != nil {
		t.Fatalf("save meta: %v", err)
	}
	j2 := NewJournal(nil, mirror)
	meta, err := j2.LoadMeta("op1")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if meta == nil || meta.SnapEpoch != 4 || meta.CopyMode != "on_segment" || meta.ResumeCons != "strict" {
		t.Fatalf("meta = %+v, want epoch=4 on_segment strict", meta)
	}
	// No metadata yet (fresh operation): nil, no error.
	j3 := NewJournal(nil, t.TempDir()+"/journal.jsonl")
	if m, err := j3.LoadMeta("op2"); err != nil || m != nil {
		t.Fatalf("fresh meta = %+v, err=%v; want nil, nil", m, err)
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

// TestResumeKeepsSingleEpochTable: run 1 completes both tables (epoch 1);
// run 2 over the surviving mirror must NOT re-copy them (keep rule) — the
// destination sees no new COPY data — and the job's epoch advances to 2.
func TestResumeKeepsSingleEpochTable(t *testing.T) {
	cfg := config.Default()
	mirror := t.TempDir() + "/journal.jsonl"

	m1, _, _ := runT5(t, mirror, cfg)
	if m1.Units != 2 || m1.Skipped != 0 {
		t.Fatalf("run1: units=%d skipped=%d, want 2/0", m1.Units, m1.Skipped)
	}
	for _, u := range m1.journal.Units() {
		if u.State != StateSwapped || u.Epoch != 1 {
			t.Fatalf("run1 unit %d: state=%s epoch=%d, want swapped/1", u.UnitID, u.State, u.Epoch)
		}
	}

	// "Crash + re-run": a fresh Manager over the same mirror (new fakes — the
	// new process sees no in-memory state, only the durable journal).
	m2, _, dst2 := runT5(t, mirror, cfg)
	if m2.Units != 0 || m2.Skipped != 2 || m2.Rows != 0 {
		t.Fatalf("run2: units=%d skipped=%d rows=%d, want 0/2/0 (keep)", m2.Units, m2.Skipped, m2.Rows)
	}
	// No data was re-copied in run 2: the destination wrote nothing.
	if w := dst2.WrittenCopy("FROM STDIN"); len(w) != 0 {
		t.Fatalf("run2 re-copied %d bytes of COPY data, want 0 (keep rule)", len(w))
	}
	for _, u := range m2.journal.Units() {
		if u.State != StateSwapped || u.Epoch != 1 {
			t.Fatalf("run2 unit %d: state=%s epoch=%d, want swapped/1 (kept from run1)", u.UnitID, u.State, u.Epoch)
		}
	}
	meta, err := m2.journal.LoadMeta("ht5")
	if err != nil || meta == nil || meta.SnapEpoch != 2 {
		t.Fatalf("run2 meta = %+v err=%v, want snap_epoch=2", meta, err)
	}
}

// TestResumeMixedEpochsReReadsTable: run 1 completes orders (epoch 1) but
// orders2 is left in flight (crash mid-copy). Run 2 (relaxed) re-reads ONLY
// orders2 under the fresh snapshot (epoch 2) and keeps orders.
func TestResumeMixedEpochsReReadsTable(t *testing.T) {
	cfg := config.Default()
	mirror := t.TempDir() + "/journal.jsonl"

	m1, _, _ := runT5(t, mirror, cfg)
	// Simulate the crash: orders2 (unit 2) was verified in neither run —
	// its last journal state is "copying" (in flight when the job died).
	u2 := m1.journal.Get(2)
	if err := m1.journal.MarkUnit(u2, StateCopying); err != nil {
		t.Fatalf("mark copying: %v", err)
	}

	m2, _, dst2 := runT5(t, mirror, cfg)
	if m2.Units != 1 || m2.Skipped != 1 {
		t.Fatalf("run2: units=%d skipped=%d, want 1/1 (orders kept, orders2 re-read)", m2.Units, m2.Skipped)
	}
	if len(dst2.WrittenCopy("FROM STDIN")) == 0 {
		t.Fatalf("run2 copied no data, want the re-read of orders2")
	}
	u1, u2 := m2.journal.Get(1), m2.journal.Get(2)
	if u1.State != StateSwapped || u1.Epoch != 1 {
		t.Fatalf("run2 orders: state=%s epoch=%d, want swapped/1 (kept)", u1.State, u1.Epoch)
	}
	if u2.State != StateSwapped || u2.Epoch != 2 {
		t.Fatalf("run2 orders2: state=%s epoch=%d, want swapped/2 (re-read)", u2.State, u2.Epoch)
	}
}

// TestResumeStrictReReadsOldEpoch: with --resume-consistency strict, a table
// verified under an OLDER snapshot epoch is re-read under the fresh snapshot
// even though relaxed would keep it.
func TestResumeStrictReReadsOldEpoch(t *testing.T) {
	cfg := config.Default()
	mirror := t.TempDir() + "/journal.jsonl"

	// Run 1 (relaxed): both tables completed, epoch 1.
	m1, _, _ := runT5(t, mirror, cfg)
	if m1.Units != 2 {
		t.Fatalf("run1: units=%d, want 2", m1.Units)
	}

	// Run 2 (strict): the fresh snapshot is epoch 2; both tables were
	// verified under epoch 1 -> strict re-reads them.
	cfg2 := config.Default()
	cfg2.ResumeConsistency = "strict"
	m2, _, dst2 := runT5(t, mirror, cfg2)
	if m2.Units != 2 || m2.Skipped != 0 {
		t.Fatalf("run2 strict: units=%d skipped=%d, want 2/0 (re-read all)", m2.Units, m2.Skipped)
	}
	if len(dst2.WrittenCopy("FROM STDIN")) == 0 {
		t.Fatalf("run2 strict copied no data, want both tables re-read")
	}
	for _, u := range m2.journal.Units() {
		if u.State != StateSwapped || u.Epoch != 2 {
			t.Fatalf("run2 unit %d: state=%s epoch=%d, want swapped/2", u.UnitID, u.State, u.Epoch)
		}
	}
}

// newPairWithIndex builds a pair where the source reports one index on
// orders, so runUnit enqueues a background index build (FR-17) and the
// T5 IndexDone marker is exercised.
func newPairWithIndex(t *testing.T) (*pg.FakeConn, *pg.FakeConn) {
	t.Helper()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	// The source reports an index on orders (pg_get_indexdef marker).
	src.On("pg_get_indexdef", [][]any{
		{"idx_orders", false, false, "CREATE INDEX idx_orders ON public.orders (id)"},
	})
	// The destination has no index yet (indexExists -> empty), so the build
	// issues CREATE INDEX; the swap's target guard (relkind IN ('r','p'))
	// stays registered as "pg_class" (empty) from newPair.
	dst.On(`relkind='i'`, [][]any{})
	return src, dst
}

// TestKeepDoesNotRebuildIndexes: run 1 swaps orders and builds its index
// (IndexDone marked). Run 2 (keep, single epoch) must NOT re-enqueue the
// index build — no CREATE INDEX on the run-2 destination (FR-17 + T5 keep).
func TestKeepDoesNotRebuildIndexes(t *testing.T) {
	cfg := config.Default()
	mirror := t.TempDir() + "/journal.jsonl"

	// Run 1: fresh fakes with an index; the build is enqueued after the swap.
	src1, dst1 := newPairWithIndex(t)
	j1 := NewJournal(pg.NewFakeConn(), mirror)
	m1 := NewManager(&cfg, src1, dst1, j1)
	if err := m1.Run(context.Background(), t5TwoTablePlan()); err != nil {
		t.Fatalf("run1: %v", err)
	}
	created1 := 0
	for _, q := range dst1.Queried() {
		if strings.Contains(q, "CREATE INDEX") {
			created1++
		}
	}
	if created1 == 0 {
		t.Fatalf("run1 built no index, want the background build to run")
	}
	// The kept unit's index build must be marked done in the journal.
	u1 := j1.Get(1)
	if u1 == nil || !u1.IndexDone {
		t.Fatalf("run1 orders IndexDone = %v, want true", u1 != nil && u1.IndexDone)
	}

	// Run 2: a FRESH process (new fakes) over the surviving mirror. Keep
	// rule: orders is kept, so its index must NOT be re-built.
	src2, dst2 := newPairWithIndex(t)
	j2 := NewJournal(pg.NewFakeConn(), mirror)
	m2 := NewManager(&cfg, src2, dst2, j2)
	if err := m2.Run(context.Background(), t5TwoTablePlan()); err != nil {
		t.Fatalf("run2: %v", err)
	}
	created2 := 0
	for _, q := range dst2.Queried() {
		if strings.Contains(q, "CREATE INDEX") {
			created2++
		}
	}
	if created2 != 0 {
		t.Fatalf("run2 re-built %d index(es), want 0 (keep must not reindex)", created2)
	}
	if m2.Skipped != 2 {
		t.Fatalf("run2 skipped = %d, want 2 (both kept)", m2.Skipped)
	}
}
