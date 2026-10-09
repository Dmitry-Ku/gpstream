package journaldb

import (
	"context"
	"strings"
	"testing"
)

// execRecorder is a fake pg.Querier that records every Exec/Query call so a
// test can assert the exact SQL + args the journal helpers issue (PG 9.4 has
// no ON CONFLICT: the upsert is a SELECT-by-PK + UPDATE/INSERT, and the
// operation_id column must ride both write paths).
type execRecorder struct {
	execs   []string
	exArgs  [][]any
	queries []string
	// rowResult: the value pg.Row returns for the SELECT-by-PK probe
	// (nil = row absent -> INSERT; non-nil = row present -> UPDATE).
	rowResult any
}

func (r *execRecorder) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	r.queries = append(r.queries, sql)
	if r.rowResult != nil {
		return [][]any{{r.rowResult}}, nil
	}
	return nil, nil
}
func (r *execRecorder) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	r.execs = append(r.execs, sql)
	r.exArgs = append(r.exArgs, args)
	return 0, nil
}
func (r *execRecorder) Ping(ctx context.Context) error { return nil }
func (r *execRecorder) Close()                         {}

func TestInitAddsOperationIDBackfill(t *testing.T) {
	r := &execRecorder{}
	if err := Init(context.Background(), r); err != nil {
		t.Fatalf("Init: %v", err)
	}
	var create, backfill *string
	for i, sql := range r.execs {
		if strings.Contains(sql, "CREATE TABLE IF NOT EXISTS gpstream_units") {
			s := sql
			create = &s
			if !strings.Contains(sql, "operation_id text") {
				t.Errorf("CREATE TABLE must declare operation_id:\n%s", sql)
			}
		}
		if strings.Contains(sql, "ADD COLUMN operation_id") {
			s := sql
			backfill = &s
			if !strings.Contains(sql, "EXCEPTION WHEN duplicate_column") {
				t.Errorf("backfill must be a guarded DO block (PG 9.4):\n%s", sql)
			}
			_ = i
		}
	}
	if create == nil {
		t.Fatal("no CREATE TABLE for gpstream_units recorded")
	}
	if backfill == nil {
		t.Fatal("no operation_id backfill DO block recorded (pre-existing tables need it)")
	}
}

func TestSetStateCarriesOperationID(t *testing.T) {
	// Row absent -> the INSERT branch must carry the operation_id (16 args).
	r := &execRecorder{}
	if err := SetState(context.Background(), r, &Unit{
		JobID: "jobhash", UnitID: 1, DB: "appdb", Schema: "public",
		Table: "alpha", Kind: "table", State: "swapped",
		Checksum: "5:abc", RowCount: 5, OperationID: "op-123",
	}); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	var ins *string
	var insArgs []any
	for i, sql := range r.execs {
		if strings.Contains(sql, "INSERT INTO gpstream_units") {
			s := sql
			ins = &s
			insArgs = r.exArgs[i]
		}
	}
	if ins == nil {
		t.Fatal("no INSERT INTO gpstream_units recorded")
	}
	if !strings.Contains(*ins, "operation_id") {
		t.Errorf("INSERT must include the operation_id column:\n%s", *ins)
	}
	if len(insArgs) != 16 {
		t.Fatalf("INSERT args = %d, want 16 (15 + operation_id): %v", len(insArgs), insArgs)
	}
	if insArgs[13] != "op-123" {
		t.Errorf("INSERT arg[13] (operation_id) = %v, want op-123", insArgs[13])
	}
}

func TestSetStateUpdateCarriesOperationID(t *testing.T) {
	// Row present -> the UPDATE branch must carry the operation_id (11 args).
	r := &execRecorder{rowResult: "1"}
	if err := SetState(context.Background(), r, &Unit{
		JobID: "jobhash", UnitID: 1, State: "swapped", OperationID: "op-123",
	}); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	var upd *string
	var updArgs []any
	for i, sql := range r.execs {
		if strings.Contains(sql, "UPDATE gpstream_units SET") {
			s := sql
			upd = &s
			updArgs = r.exArgs[i]
		}
	}
	if upd == nil {
		t.Fatal("no UPDATE gpstream_units recorded (row present)")
	}
	if !strings.Contains(*upd, "operation_id = $9") {
		t.Errorf("UPDATE must set operation_id:\n%s", *upd)
	}
	if len(updArgs) != 11 {
		t.Fatalf("UPDATE args = %d, want 11: %v", len(updArgs), updArgs)
	}
	if updArgs[8] != "op-123" {
		t.Errorf("UPDATE arg[8] (operation_id) = %v, want op-123", updArgs[8])
	}
}

func TestSetIndexDoneScoped(t *testing.T) {
	// With an operation id: the UPDATE is scoped on operation_id.
	r := &execRecorder{}
	if err := SetIndexDone(context.Background(), r, "jobhash", 1, "op-123"); err != nil {
		t.Fatalf("SetIndexDone: %v", err)
	}
	if len(r.execs) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(r.execs))
	}
	if !strings.Contains(r.execs[0], "operation_id = $3") {
		t.Errorf("scoped SetIndexDone must filter on operation_id:\n%s", r.execs[0])
	}
	if len(r.exArgs[0]) != 3 || r.exArgs[0][2] != "op-123" {
		t.Errorf("scoped SetIndexDone args = %v, want [jobhash 1 op-123]", r.exArgs[0])
	}

	// Empty operation id (legacy row): degrades to the job_id-only scope.
	r2 := &execRecorder{}
	if err := SetIndexDone(context.Background(), r2, "jobhash", 1, ""); err != nil {
		t.Fatalf("SetIndexDone (legacy): %v", err)
	}
	if len(r2.execs) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(r2.execs))
	}
	if strings.Contains(r2.execs[0], "operation_id") {
		t.Errorf("legacy SetIndexDone must not filter on operation_id:\n%s", r2.execs[0])
	}
	if len(r2.exArgs[0]) != 2 {
		t.Errorf("legacy SetIndexDone args = %v, want [jobhash 1]", r2.exArgs[0])
	}
}
