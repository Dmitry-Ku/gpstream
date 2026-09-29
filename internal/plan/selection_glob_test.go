package plan

import "testing"

func TestGlobSegment(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"public", "public", true},
		{"public", "public2", false},
		{"audit_*", "audit_2024", true},
		{"audit_*", "audit", false},
		{"audit_*", "billing_2024", false},
		{"t?", "t1", true},
		{"t?", "t12", false},
		{"*", "anything", true},
		{"", "", true},
	}
	for _, c := range cases {
		if got := globSeg(c.pattern, c.s); got != c.want {
			t.Errorf("globSeg(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestGlobDotted(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"db.public", "db.public", true},
		{"db.public", "db.public2", false},
		{"db.*", "db.public", true},
		{"db.*", "db.public.t", false}, // * never crosses a dot
		{"db.*.*", "db.public.t", true},
		{"*.*", "db.public", true},
		{"*.*", "db.public.t", false},
		{"db.pub*", "db.public", true},
	}
	for _, c := range cases {
		if got := globDotted(c.pattern, c.s); got != c.want {
			t.Errorf("globDotted(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestMatchTableGlob(t *testing.T) {
	// 2-segment schema.name
	if !matchTable("public.orders_*", "", "public", "orders_2024") {
		t.Error("public.orders_* should match orders_2024")
	}
	if matchTable("public.orders_*", "", "public", "invoices_2024") {
		t.Error("public.orders_* must not match invoices_2024")
	}
	// Literal behavior is unchanged (no wildcard => exact).
	if !matchTable("public.orders", "", "public", "orders") {
		t.Error("literal public.orders must still match exactly")
	}
	if matchTable("public.orders", "", "public", "orders2") {
		t.Error("literal public.orders must not match orders2")
	}
	// 3-segment db.schema.table
	if !matchTable("app.*.logs_*", "app", "audit", "logs_2026") {
		t.Error("app.*.logs_* should match app.audit.logs_2026")
	}
	if matchTable("app.*.logs_*", "web", "audit", "logs_2026") {
		t.Error("app.*.logs_* must not match a different db")
	}
}

func TestMatchSchemaGlob(t *testing.T) {
	if !matchSchema("audit_*", "audit_2024") {
		t.Error("audit_* should match audit_2024")
	}
	if matchSchema("audit_*", "billing_2024") {
		t.Error("audit_* must not match billing_2024")
	}
	// db.schema form: the db prefix is advisory, the schema segment globs.
	if !matchSchema("app.audit_*", "audit_2024") {
		t.Error("app.audit_* should match audit_2024")
	}
}

func TestMatchAnyGlob(t *testing.T) {
	if !matchAny("orders_*", "app", "public", "orders_2024") {
		t.Error("orders_* should match the table name")
	}
	if matchAny("orders_*", "app", "public", "invoices_2024") {
		t.Error("orders_* must not match invoices_2024")
	}
	if !matchAny("*.*", "app", "public", "t1") {
		t.Error("*.* should match app.public (db.schema compound)")
	}
	if matchAny("db.*", "app", "public", "t1") {
		t.Error("db.* must not match app.public (the db name differs)")
	}
}
