package pg

import (
	"strings"
	"testing"
)

// TestConstraintDefBuildDefCheck verifies that a CHECK constraint renders its
// verbatim expression (it references no other table, so nothing is rewritten).
func TestConstraintDefBuildDefCheck(t *testing.T) {
	d := ConstraintDef{
		Name: "ch_pos",
		Kind: "c",
		Def:  "CHECK ((id > 0))",
	}
	got := d.BuildDef()
	if got != "CHECK ((id > 0))" {
		t.Fatalf("BuildDef(check) = %q, want the verbatim check", got)
	}
}

// TestConstraintDefBuildDefFK verifies that an FK renders a deterministic,
// schema-qualified REFERENCES clause with the ON DELETE/ON UPDATE actions, and
// that the default NO ACTION is omitted (the destination applies its own
// default, so the DDL stays minimal and portable).
func TestConstraintDefBuildDefFK(t *testing.T) {
	d := ConstraintDef{
		Name:         "fk_child_parent",
		Kind:         "f",
		LocalColumns: `"parent_id"`,
		RefSchema:    "s_new",
		RefTable:     "parents_v2",
		RefColumns:   `"id"`,
		DelAction:    "CASCADE",
		UpdAction:    "NO ACTION",
	}
	got := d.BuildDef()
	want := `FOREIGN KEY ("parent_id") REFERENCES "s_new"."parents_v2"("id") ON DELETE CASCADE`
	if got != want {
		t.Fatalf("BuildDef(fk) =\n  %q\nwant\n  %q", got, want)
	}
}

// TestConstraintDefBuildDefFKNoAction verifies the default action is omitted.
func TestConstraintDefBuildDefFKNoAction(t *testing.T) {
	d := ConstraintDef{
		Name:         "fk_plain",
		Kind:         "f",
		LocalColumns: `"p"`,
		RefSchema:    "public",
		RefTable:     "parents",
		RefColumns:   `"id"`,
		DelAction:    "NO ACTION",
		UpdAction:    "NO ACTION",
	}
	got := d.BuildDef()
	want := `FOREIGN KEY ("p") REFERENCES "public"."parents"("id")`
	if got != want {
		t.Fatalf("BuildDef(fk, default actions) = %q, want %q (no ON DELETE/UPDATE)", got, want)
	}
}

// TestConstraintDefBuildDefFKMultiCol verifies a multi-column FK and that both
// key actions render.
func TestConstraintDefBuildDefFKMultiCol(t *testing.T) {
	d := ConstraintDef{
		Name:         "fk_multi",
		Kind:         "f",
		LocalColumns: `"parent_id","code2"`,
		RefSchema:    "public",
		RefTable:     "parents",
		RefColumns:   `"id","code"`,
		DelAction:    "CASCADE",
		UpdAction:    "RESTRICT",
	}
	got := d.BuildDef()
	if !strings.Contains(got, `REFERENCES "public"."parents"("id","code")`) {
		t.Fatalf("BuildDef(fk multi) missing referenced cols: %q", got)
	}
	if !strings.Contains(got, "ON DELETE CASCADE") || !strings.Contains(got, "ON UPDATE RESTRICT") {
		t.Fatalf("BuildDef(fk multi) missing key actions: %q", got)
	}
}

// TestConstraintDefBuildDefFKUnqualifiedRef verifies an FK whose referenced
// table has no schema (RefSchema == "") still renders, unquoted-table form.
func TestConstraintDefBuildDefFKUnqualifiedRef(t *testing.T) {
	d := ConstraintDef{
		Name:         "fk_unq",
		Kind:         "f",
		LocalColumns: `"p"`,
		RefSchema:    "",
		RefTable:     "parents",
		RefColumns:   `"id"`,
		DelAction:    "NO ACTION",
		UpdAction:    "NO ACTION",
	}
	got := d.BuildDef()
	want := `FOREIGN KEY ("p") REFERENCES "parents"("id")`
	if got != want {
		t.Fatalf("BuildDef(fk unqualified) = %q, want %q", got, want)
	}
}
