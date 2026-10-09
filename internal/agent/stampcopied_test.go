package agent

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/plan"
)

// copiedQuerier is a fake destination querier for the 2E-2 already-copied
// stamp: it returns the gpstream_units terminal rows for the plan's job_id
// and records the SQL it saw.
type copiedQuerier struct {
	rows    [][]any
	queried bool
	sql     string
	args    []any
}

func (q *copiedQuerier) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	q.queried = true
	q.sql = sql
	q.args = args
	return q.rows, nil
}
func (q *copiedQuerier) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return 0, nil
}
func (q *copiedQuerier) Ping(ctx context.Context) error { return nil }
func (q *copiedQuerier) Close()                         {}

func TestStampCopiedStates(t *testing.T) {
	p := &plan.Plan{
		Hash: "jobhash123",
		Units: []plan.Table{
			{DB: "appdb", Schema: "public", Name: "alpha", DstSchema: "public", DstName: "alpha"},
			{DB: "appdb", Schema: "public", Name: "bravo", DstSchema: "public", DstName: "bravo"},
			{DB: "appdb", Schema: "public", Name: "charlie", DstSchema: "public", DstName: "charlie"},
		},
	}
	dq := &copiedQuerier{rows: [][]any{
		{"appdb", "public", "alpha", "swapped"},
		{"appdb", "public", "bravo", "completed"},
		{"appdb", "public", "delta", "swapped"}, // not in the plan: ignored
	}}
	stampCopiedStates(context.Background(), dq, p, "op-123")

	if !dq.queried {
		t.Fatal("the destination journal must be queried (2E-2: source->dest, no controller)")
	}
	// The query is scoped to the plan's job_id AND the operation id (2E-2
	// operation-scoped journal: a plan-hash shared by several operations must
	// never be cross-adopted).
	if len(dq.args) != 2 || dq.args[0] != "jobhash123" || dq.args[1] != "op-123" {
		t.Fatalf("query args = %v, want [jobhash123 op-123]", dq.args)
	}
	if !contains(dq.sql, "operation_id = $2") {
		t.Fatalf("scoped query must filter on operation_id, got: %s", dq.sql)
	}
	if got := p.Units[0].CopiedState; got != "swapped" {
		t.Errorf("alpha CopiedState = %q, want swapped", got)
	}
	if got := p.Units[1].CopiedState; got != "completed" {
		t.Errorf("bravo CopiedState = %q, want completed", got)
	}
	if got := p.Units[2].CopiedState; got != "" {
		t.Errorf("charlie CopiedState = %q, want empty (never copied)", got)
	}
}

func TestStampCopiedStatesNoDest(t *testing.T) {
	p := &plan.Plan{Hash: "h", Units: []plan.Table{{DB: "d", Schema: "s", Name: "t"}}}
	// No destination (a source-only plan): no query, no stamp, no panic.
	stampCopiedStates(context.Background(), nil, p, "")
	if p.Units[0].CopiedState != "" {
		t.Fatalf("no-dest: CopiedState must stay empty, got %q", p.Units[0].CopiedState)
	}
	// No hash (a partial plan): no query.
	dq := &copiedQuerier{}
	stampCopiedStates(context.Background(), dq, &plan.Plan{Units: []plan.Table{{DB: "d", Schema: "s", Name: "t"}}}, "")
	if dq.queried {
		t.Fatal("no hash: the destination must not be queried")
	}
}

// TestStampCopiedStatesLegacyScope: an empty operation id (a legacy /
// pre-2E-2 destination, or a run without an operation id) degrades to the
// job_id-only query — no operation_id filter, a single arg.
func TestStampCopiedStatesLegacyScope(t *testing.T) {
	p := &plan.Plan{
		Hash:  "jobhash123",
		Units: []plan.Table{{DB: "appdb", Schema: "public", Name: "alpha"}},
	}
	dq := &copiedQuerier{rows: [][]any{{"appdb", "public", "alpha", "swapped"}}}
	stampCopiedStates(context.Background(), dq, p, "")
	if !dq.queried {
		t.Fatal("the destination journal must be queried")
	}
	if len(dq.args) != 1 || dq.args[0] != "jobhash123" {
		t.Fatalf("legacy-scope query args = %v, want [jobhash123]", dq.args)
	}
	if contains(dq.sql, "operation_id") {
		t.Fatalf("legacy scope must not filter on operation_id, got: %s", dq.sql)
	}
	if got := p.Units[0].CopiedState; got != "swapped" {
		t.Errorf("alpha CopiedState = %q, want swapped", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestCellString(t *testing.T) {
	if got := cellString(nil); got != "" {
		t.Errorf("cellString(nil) = %q, want empty", got)
	}
	if got := cellString("x"); got != "x" {
		t.Errorf("cellString(string) = %q, want x", got)
	}
	if got := cellString([]byte("y")); got != "y" {
		t.Errorf("cellString([]byte) = %q, want y", got)
	}
}
