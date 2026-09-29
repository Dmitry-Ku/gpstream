package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// registerSequences wires a fake source conn to expose one serial sequence
// (F-11): discovery row + last_value. The destination fake (newPair) has an
// empty pg_class, so the sequence-existence check returns "not exists" and
// a CREATE is issued.
func registerSequences(f *pg.FakeConn, last int64) {
	f.On("pg_get_serial_sequence", [][]any{
		{"public.orders_id_seq", "id"},
	})
	f.On("last_value", [][]any{{last}})
}

// TestManagerSyncSequences is the F-11 acceptance: after the unit swaps, the
// source sequence is recreated on the destination (CREATE SEQUENCE) and its
// value resynchronised to the source last_value (setval(seq, 42, true) — the
// next nextval returns 43, like on the source). The run must still complete
// (the sequence sync is post-swap and non-fatal), and events.jsonl records
// the sequence_synced event (F-04).
func TestManagerSyncSequences(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	registerSequences(src, 42)
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	u := j.Units()[0]
	if u.State != StateSwapped {
		t.Fatalf("state = %s, want %s", u.State, StateSwapped)
	}

	created, setval := false, false
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, `CREATE SEQUENCE "public"."orders_id_seq"`) {
			created = true
		}
		if strings.Contains(sql, "setval($1::regclass, $2, true)") &&
			strings.Contains(sql, "[args: public.orders_id_seq, 42]") {
			setval = true
		}
	}
	if !created {
		t.Errorf("destination never issued CREATE SEQUENCE: %v", dst.Queried())
	}
	if !setval {
		t.Errorf("destination never issued setval to the source last_value 42: %v", dst.Queried())
	}
}

// TestManagerSyncSequencesNone verifies a table without serial sequences
// issues no sequence SQL (the common case is untouched).
func TestManagerSyncSequencesNone(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	src.On("pg_get_serial_sequence", [][]any{}) // no serial columns
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, "SEQUENCE") {
			t.Fatalf("sequence SQL must not be issued for a sequence-less table: %v", sql)
		}
	}
}

// TestManagerSyncSequencesFailureNonFatal verifies a sequence-sync fault
// (the source last_value read fails) does NOT fail the run: the data is
// already in place, and the failure is recorded in events.jsonl as a
// sequence_sync_failed event with the root cause (F-04).
func TestManagerSyncSequencesFailureNonFatal(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	// Discovery returns a serial column, but last_value is UNREGISTERED ->
	// Query error: models a fault in the value read.
	src.On("pg_get_serial_sequence", [][]any{{"public.orders_id_seq", "id"}})
	dir := t.TempDir()
	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run must survive a sequence-sync fault: %v", err)
	}
	u := j.Units()[0]
	if u.State != StateSwapped {
		t.Fatalf("state = %s, want %s (sequence sync is non-fatal)", u.State, StateSwapped)
	}
	evs := eventsOf(t, dir)
	fails := findEvent(t, evs, "sequence_sync_failed")
	if len(fails) != 1 {
		t.Fatalf("got %d sequence_sync_failed events, want 1", len(fails))
	}
	if fails[0]["reason"] != "read source last_value (F-11)" {
		t.Fatalf("reason = %v, want the last_value-read root cause", fails[0]["reason"])
	}
}

// TestManagerSyncSequencesRenamed verifies a renamed unit (FR-11) still
// syncs: the destination table is created under the new name, and the
// sequence keeps its source name (the table's serial default was copied
// verbatim, so the default still resolves).
func TestManagerSyncSequencesRenamed(t *testing.T) {
	cfg := config.Default()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	registerSequences(src, 7)
	j := NewJournal(pg.NewFakeConn(), t.TempDir()+"/journal.jsonl")
	m := NewManager(&cfg, src, dst, j)

	if err := m.Run(context.Background(), renamedPlan()); err != nil {
		t.Fatalf("run: %v", err)
	}
	setval := false
	for _, sql := range dst.Queried() {
		if strings.Contains(sql, "setval($1::regclass, $2, true)") &&
			strings.Contains(sql, "[args: public.orders_id_seq, 7]") {
			setval = true
		}
	}
	if !setval {
		t.Fatalf("renamed unit: sequence not resynced: %v", dst.Queried())
	}
	// The renamed unit's table still landed (the rename is unaffected by
	// the sequence sync).
	if u := j.Units()[0]; u.DstName != "orders_v2" || u.State != StateSwapped {
		t.Fatalf("renamed unit = %+v, want s_new.orders_v2 swapped", u)
	}
}
