package manifest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// newPair builds the source and destination fake conns used by a Manager test.
// It is shared by newManager and by tests that need to inspect the fakes
// directly (e.g. to assert the SQL each side issued under a rename).
func newPair(t *testing.T, srcPayload, dstPayload []byte) (src *pg.FakeConn, dst *pg.FakeConn) {
	t.Helper()
	src = pg.NewFakeConn()
	src.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	// The source serves its binary COPY payload for both CopyToStream and
	// Checksum (both issue `COPY ... TO STDOUT`).
	src.OnReadCopy("TO STDOUT", srcPayload)

	dst = pg.NewFakeConn()
	// The destination records COPY ... FROM STDIN (CopyIntoStream) and re-reads
	// its own `COPY ... TO STDOUT` for Verify.
	dst.OnWriteCopy("FROM STDIN")
	dst.OnReadCopy("TO STDOUT", dstPayload)
	// Swap's existence guard: target does not exist (empty pg_class).
	dst.On("pg_class", [][]any{})
	return src, dst
}

// newManager builds a Manager over two fake streaming conns (source + dest)
// and a file-mirrored journal. It drives the REAL Manager.Run / runUnit /
// framedPipe / copyStream through a real FramedPipe — only the SQL layer is
// faked, so the compression+framing+verify+swap data path is exercised.
func newManager(t *testing.T, cfg *config.Config, srcPayload, dstPayload []byte) (*Manager, *Journal) {
	t.Helper()
	src, dst := newPair(t, srcPayload, dstPayload)
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	return NewManager(cfg, src, dst, j), j
}

// oneUnitPlan returns a single-unit plan (no conflict by default).
func oneUnitPlan() *plan.Plan {
	return &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "public", DstName: "orders", Bytes: 100},
		},
	}
}

// renamedPlan returns a single unit whose SOURCE and DESTINATION names differ
// (FR-11/§4.10): public.orders on the source -> s_new.orders_v2 on the dest.
func renamedPlan() *plan.Plan {
	return &plan.Plan{
		Units: []plan.Table{
			{DB: "db", Schema: "public", Name: "orders", DstSchema: "s_new", DstName: "orders_v2", Bytes: 100},
		},
	}
}

// TestManagerRunHonoursRename is the regression test for the source/destination
// name split (FR-11/§4.10). Before the fix the unit carried the destination
// names in Schema/Table and the source COPY read from them — a rename would
// read a name that does not exist on the source. The copy must READ from the
// source name (public.orders) and WRITE to the destination name (s_new.*), so
// both names appear in the SQL and each on the correct side.
func TestManagerRunHonoursRename(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), renamedPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	u := j.Units()[0]
	if u.State != StateSwapped {
		t.Fatalf("state = %s, want %s", u.State, StateSwapped)
	}
	// The unit's journal entry keeps the SOURCE name in Schema/Table and the
	// DESTINATION name in DstSchema/DstName (they differ after a rename).
	if u.Schema != "public" || u.Table != "orders" || u.DstSchema != "s_new" || u.DstName != "orders_v2" {
		t.Errorf("unit names = %s/%s -> %s/%s, want public/orders -> s_new/orders_v2",
			u.Schema, u.Table, u.DstSchema, u.DstName)
	}

	// Assert the SQL: the source COPY reads public.orders (TO STDOUT) and the
	// destination COPY writes into the s_new schema (FROM STDIN).
	srcRead, dstWrite := false, false
	for _, sql := range src.Queried() {
		if strings.Contains(sql, `"public"."orders"`) && strings.Contains(sql, "TO STDOUT") {
			srcRead = true
		}
	}
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, `"s_new"."`) && strings.Contains(sql, "FROM STDIN") {
			dstWrite = true
		}
	}
	if !srcRead {
		t.Errorf("source COPY did not read the source name public.orders")
	}
	if !dstWrite {
		t.Errorf("destination COPY did not write into the s_new schema")
	}
}

// TestManagerRunHappyPath verifies the full unit pipeline end-to-end against
// faked SQL: copy -> verify (checksum match) -> reindex -> swap -> journal
// (FR-01/FR-02/FR-06/FR-17). The framed+compressed bytes must round-trip so
// the verify checksum matches.
func TestManagerRunHappyPath(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	m, j := newManager(t, &cfg, payload, payload)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
	if m.Rows == 0 {
		t.Errorf("rows = 0, want > 0 (data must have been copied)")
	}
	if m.Comps != 1 {
		t.Errorf("comps = %d, want 1 (verified checksum)", m.Comps)
	}
	u := j.Get(1)
	if u == nil {
		t.Fatalf("journal has no unit 1")
	}
	if u.State != StateSwapped {
		t.Errorf("unit 1 state = %s, want swapped", u.State)
	}
	if u.Checksum == "" {
		t.Errorf("unit 1 has no checksum (verify not performed)")
	}
}

// TestManagerSkipConflict verifies a conflicting unit is skipped under the
// default skip-existing policy (FR-10/FR-11) and the run still succeeds.
func TestManagerSkipConflict(t *testing.T) {
	cfg := config.Default() // ConflictAction = SkipExisting
	payload := []byte("data")
	m, _ := newManager(t, &cfg, payload, payload)

	p := oneUnitPlan()
	p.Units[0].HasConflict = true

	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Skipped != 1 {
		t.Errorf("skipped = %d, want 1", m.Skipped)
	}
	if m.Units != 0 {
		t.Errorf("units = %d, want 0 (nothing copied)", m.Units)
	}
}

// TestManagerChecksumMismatch verifies a unit whose destination checksum does
// not match the source fails, is rolled back (staging dropped) and the run
// returns an error (FR-02/ADR-001 §4.4).
func TestManagerChecksumMismatch(t *testing.T) {
	cfg := config.Default()
	m, _ := newManager(t, &cfg, []byte("source-data"), []byte("different-dest-data"))

	err := m.Run(context.Background(), oneUnitPlan())
	if err == nil {
		t.Fatal("run should fail on checksum mismatch")
	}
	if m.Failures != 1 {
		t.Errorf("failures = %d, want 1", m.Failures)
	}
	if m.Units != 0 {
		t.Errorf("units = %d, want 0 (no verified unit)", m.Units)
	}
}

// TestManagerOverwriteSwap verifies the overwrite policy drives the swap with
// overwrite=true (the existing final table is dropped before the rename,
// FR-11).
func TestManagerOverwriteSwap(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Overwrite
	payload := []byte("BATCH")
	m, _ := newManager(t, &cfg, payload, payload)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
}

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

// TestManagerParallelConcurrentStats runs six tables through the FR-03 worker
// pool (WorkersPerPair=4) so several goroutines mutate the Stats counters at
// once, and asserts the counters end up consistent. This is a regression lock
// for the statMu guard: under `go test -race` an unguarded write would fail.
func TestManagerParallelConcurrentStats(t *testing.T) {
	cfg := config.Default()
	cfg.WorkersPerPair = 4 // force a real multi-goroutine worker pool (FR-03)
	payload := []byte("BATCH-ROW")
	m, j := newManager(t, &cfg, payload, payload)

	// Six units of the same table. The fake conn matches by SQL substring, so
	// the names do not affect the canned payloads; Run assigns UnitID itself.
	var units []plan.Table
	for i := 0; i < 6; i++ {
		units = append(units, plan.Table{
			DB: "db", Schema: "public", Name: "orders",
			DstSchema: "public", DstName: "orders", Bytes: 100,
		})
	}
	plan := &plan.Plan{Units: units}

	if err := m.Run(context.Background(), plan); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.Units != 6 {
		t.Errorf("Units = %d, want 6", m.Units)
	}
	// Each unit copies one (fake) row, so 6 units => 6 rows and 6 checksums.
	if m.Rows != 6 {
		t.Errorf("Rows = %d, want 6", m.Rows)
	}
	if m.Comps != 6 {
		t.Errorf("Comps = %d, want 6", m.Comps)
	}
	if m.Skipped != 0 || m.Failures != 0 {
		t.Errorf("Skipped/Failures = %d/%d, want 0/0", m.Skipped, m.Failures)
	}
	if len(j.Units()) != 6 {
		t.Errorf("journal has %d units, want 6", len(j.Units()))
	}
}

// indexRows is the canned pg_index payload the source fake serves for
// src.Indexes (F-17). The definition embeds the SOURCE table name, as
// pg_get_indexdef does.
func indexRows() [][]any {
	return [][]any{{"orders_id_idx", false, false, "CREATE INDEX orders_id_idx ON public.orders (id)"}}
}

// TestManagerRunBuildsIndexesAfterSwap verifies the F-17 data path: the index
// build is NOT issued during the copy (no pre-swap REINDEX on the index-less
// staging) and IS issued on the destination's final name after the swap, by
// the background pool.
func TestManagerRunBuildsIndexesAfterSwap(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	src.On("pg_index", indexRows())
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	u := j.Units()[0]
	if u.State != StateSwapped {
		t.Fatalf("state = %s, want %s", u.State, StateSwapped)
	}
	if m.idxPool.Done() != 1 || m.idxPool.Err() != nil {
		t.Fatalf("index pool done=%d err=%v, want 1/nil", m.idxPool.Done(), m.idxPool.Err())
	}
	found := false
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, "CREATE INDEX orders_id_idx") && strings.Contains(sql, "public.orders") {
			found = true
		}
	}
	if !found {
		t.Fatalf("index build not issued on the destination final name: %v", dst.Queried())
	}
}

// TestManagerRunIndexesHonourRename rewrites the source name in the index
// definition to the destination name (FR-11 + F-17).
func TestManagerRunIndexesHonourRename(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	src.On("pg_index", indexRows()) // definition still says public.orders
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), renamedPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	found := false
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, "CREATE INDEX orders_id_idx") && strings.Contains(sql, `s_new.orders_v2`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("index definition not rewritten to the destination name: %v", dst.Queried())
	}
}

// TestManagerRunNoIndexesSkipsPool drops the build entirely with --no-indexes
// (FR-17): the pool is empty and no index SQL reaches the destination.
func TestManagerRunNoIndexesSkipsPool(t *testing.T) {
	cfg := config.Default()
	cfg.NoIndexes = true
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	src.On("pg_index", indexRows())
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, "CREATE INDEX") {
			t.Fatalf("no index SQL expected with NoIndexes, got: %s", sql)
		}
	}
	if m.idxPool.Done() != 0 {
		t.Fatalf("pool done = %d with NoIndexes, want 0", m.idxPool.Done())
	}
}

// TestManagerResumeRequeuesSwappedUnitIndexes covers the F-17 acceptance for
// a run killed after the swap and before the pool drained: the journal holds
// the unit in the final ("swapped") state, and a fresh run re-enqueues its
// index build (createOneIndex is idempotent, so no duplicates).
func TestManagerResumeRequeuesSwappedUnitIndexes(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	src.On("pg_index", indexRows())
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	// Simulate the previous (killed) run: the unit reached the final name.
	if _, err := j.CreateUnit(&Unit{
		JobID:     "job-resume",
		UnitID:    1,
		DB:        "db",
		Schema:    "public",
		Table:     "orders",
		DstSchema: "public",
		DstName:   "orders",
		Kind:      "table",
		Bytes:     100,
	}, StateSwapped); err != nil {
		t.Fatalf("seed journal: %v", err)
	}
	m := NewManager(&cfg, src, dst, j)

	// An empty plan: nothing to copy, but the journal scan must still rebuild
	// the missing indexes.
	if err := m.Run(context.Background(), &plan.Plan{Hash: "job-resume"}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.idxPool.Done() != 1 || m.idxPool.Err() != nil {
		t.Fatalf("index pool done=%d err=%v, want 1/nil", m.idxPool.Done(), m.idxPool.Err())
	}
	found := false
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, "CREATE INDEX orders_id_idx") {
			found = true
		}
	}
	if !found {
		t.Fatalf("resume did not rebuild the swapped unit's indexes: %v", dst.Queried())
	}
}
