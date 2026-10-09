package dest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestDropObjectsOrder covers the FR-02 cleanup core (2D-B): the OBJECTS
// (staging + tables) are dropped first, the created SCHEMAS last and only
// when empty, public is never dropped, and a per-object failure does not
// abort the pass (the remaining objects are still dropped).
func TestDropObjectsOrder(t *testing.T) {
	f := pg.NewFakeConn()
	// s_new is empty (count 0 -> droppable); the pg_class probe answers 0
	// for every schema it is asked (only s_new is ever asked — public is
	// never reported as a drop target).
	f.On("pg_class", [][]any{{int64(0)}})
	a := mkAgent(f)

	targets := []DropTarget{
		// unit 1: same-name, unconflicted -> staging + table t1.
		{Schema: "public", Name: "_gs_op_1", Kind: "staging"},
		{Schema: "public", Name: "t1", Kind: "table"},
		// unit 2: renamed-in -> staging + table t2_v2 + schema s_new.
		{Schema: "s_new", Name: "_gs_op_2", Kind: "staging"},
		{Schema: "s_new", Name: "t2_v2", Kind: "table"},
		{Schema: "s_new", Name: "s_new", Kind: "schema"},
		// unit 3: conflicted -> staging only (the table is operator data;
		// the controller never reports it, so it never reaches DropObjects).
		{Schema: "public", Name: "_gs_op_3", Kind: "staging"},
	}

	out, err := a.DropObjects(context.Background(), targets)
	if err != nil {
		t.Fatalf("DropObjects: %v", err)
	}
	if out.Dropped != 6 {
		t.Fatalf("dropped = %d, want 6", out.Dropped)
	}

	var drops []string
	for _, sql := range f.Queried() {
		if strings.Contains(sql, "DROP TABLE IF EXISTS") || strings.Contains(sql, "DROP SCHEMA IF EXISTS") {
			drops = append(drops, sql)
		}
	}
	mustHave := []string{
		`DROP TABLE IF EXISTS "public"."_gs_op_1"`,
		`DROP TABLE IF EXISTS "s_new"."_gs_op_2"`,
		`DROP TABLE IF EXISTS "public"."t1"`,
		`DROP TABLE IF EXISTS "s_new"."t2_v2"`,
		`DROP TABLE IF EXISTS "public"."_gs_op_3"`,
		`DROP SCHEMA IF EXISTS "s_new"`,
	}
	for _, m := range mustHave {
		found := false
		for _, sql := range drops {
			if strings.Contains(sql, m) {
				found = true
			}
		}
		if !found {
			t.Errorf("missing drop %q in %v", m, drops)
		}
	}
	// public is never dropped (it pre-exists on every cluster).
	for _, sql := range drops {
		if strings.Contains(sql, `DROP SCHEMA IF EXISTS "public"`) {
			t.Errorf("public schema was dropped: %q", sql)
		}
	}
	// Order: the schema drop comes after every table drop.
	schemaAt, lastTable := -1, -1
	for i, sql := range drops {
		if strings.Contains(sql, `DROP SCHEMA IF EXISTS "s_new"`) {
			schemaAt = i
		}
		if strings.Contains(sql, "DROP TABLE IF EXISTS") {
			lastTable = i
		}
	}
	if schemaAt < lastTable {
		t.Errorf("schema drop (idx %d) must come after the last table drop (idx %d)", schemaAt, lastTable)
	}
}

// TestDropObjectsFailure covers the error path: every DROP fails, the
// failures are collected into the returned error AND the outcome (no
// dropped objects, per-object errors) — the caller (the agent) reports
// them in its JSON result without aborting the pass.
func TestDropObjectsFailure(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{{int64(0)}})
	f.SetExecError(errors.New("boom: drop failed"))
	a := mkAgent(f)

	targets := []DropTarget{
		{Schema: "public", Name: "_gs_op_1", Kind: "staging"},
		{Schema: "public", Name: "t1", Kind: "table"},
		{Schema: "s_new", Name: "s_new", Kind: "schema"},
	}
	out, err := a.DropObjects(context.Background(), targets)
	if err == nil {
		t.Fatal("want error when drops fail")
	}
	if out.Dropped != 0 {
		t.Errorf("dropped = %d, want 0", out.Dropped)
	}
	if !strings.Contains(err.Error(), "object(s) not dropped") {
		t.Errorf("error = %q, want aggregated drop failures", err)
	}
	if len(out.Errors) != 3 {
		t.Errorf("errors = %d, want 3 (one per target): %v", len(out.Errors), out.Errors)
	}
}

// TestDropObjectsUnknownKind covers the defensive path: a target with an
// unrecognized kind is reported as an error (never dropped), and the rest
// of the pass continues.
func TestDropObjectsUnknownKind(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	targets := []DropTarget{
		{Schema: "public", Name: "weird", Kind: "bogus"},
		{Schema: "public", Name: "t1", Kind: "table"},
	}
	out, err := a.DropObjects(context.Background(), targets)
	if err == nil {
		t.Fatal("want error for the unknown kind")
	}
	if out.Dropped != 1 {
		t.Errorf("dropped = %d, want 1 (the valid table)", out.Dropped)
	}
	if len(out.Errors) != 1 || !strings.Contains(out.Errors[0], "unknown drop kind") {
		t.Errorf("errors = %v, want one unknown-kind error", out.Errors)
	}
}
