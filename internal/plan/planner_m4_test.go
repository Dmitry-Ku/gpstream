package plan

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestSelectionNarrowsGraph verifies FR-11 selection: only the requested table
// is in the plan, and excluded tables are dropped.
func TestSelectionNarrowsGraph(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	cfg.Tables = []string{"s.orders"} // only orders (2-segment schema.name form)
	src := mkFake()
	// dest has no conflicting tables here (empty conflict answers).
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(p.Units) != 1 {
		t.Fatalf("selection produced %d units, want 1", len(p.Units))
	}
	if p.Units[0].Name != "orders" {
		t.Errorf("selected unit = %q, want orders", p.Units[0].Name)
	}
}

// TestExcludeDropsUnit verifies an exclude-* entry removes a table.
func TestExcludeDropsUnit(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	cfg.Exclude = []string{"audit"}
	src := mkFake()
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, u := range p.Units {
		if u.Name == "audit" {
			t.Errorf("excluded table audit still present")
		}
	}
	if len(p.Units) != 1 {
		t.Errorf("got %d units after exclude, want 1 (only orders)", len(p.Units))
	}
}

// TestRenameAvoidsConflict verifies FR-11/§4.10 through the real Build path:
// a schema rename and a table rename each move the destination name, so the
// conflict check (evaluated against the destination name) no longer matches
// the original name — a rename avoids a would-be overwrite. (Before the fix
// applyRename keyed every map by the table name and never applied
// RenameTable, so these renames were non-functional.)
func TestRenameAvoidsConflict(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	// Both tables live in schema "s"; rename the schema and one table.
	cfg.RenameSchema = map[string]string{"s": "s_new"}
	cfg.RenameTable = map[string]string{"s.orders": "s_new.orders_v2"}

	src := mkFake()
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})
	// No gs-conflict-check registered: the renamed destination names do not
	// exist on the destination, so no conflict is detected.

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, u := range p.Units {
		if u.Name == "orders" {
			if u.DstSchema != "s_new" || u.DstName != "orders_v2" {
				t.Errorf("orders rename: DstSchema/DstName = %q/%q, want s_new/orders_v2", u.DstSchema, u.DstName)
			}
			continue
		}
		if u.Name == "audit" {
			// audit: schema renamed, table name unchanged.
			if u.DstSchema != "s_new" || u.DstName != "audit" {
				t.Errorf("audit rename: DstSchema/DstName = %q/%q, want s_new/audit", u.DstSchema, u.DstName)
			}
			continue
		}
	}
}

// TestApplyRenameMaps verifies applyRename directly for all three map types
// (FR-11/§4.10), including db-rename which the Build path cannot exercise
// (pg_class has no DB column, so a plan never carries a source DB name).
func TestApplyRenameMaps(t *testing.T) {
	cfg := config.Default()
	cfg.RenameDB = map[string]string{"orders": "orders_v2"}
	cfg.RenameSchema = map[string]string{"s": "s_new"}
	cfg.RenameTable = map[string]string{"s.orders": "s_new.orders_v2"}
	tabs := []Table{
		{DB: "orders", Schema: "s", Name: "orders"},
		{DB: "audit", Schema: "s", Name: "audit"}, // schema renamed, table + db not
		{DB: "other", Schema: "x", Name: "plain"}, // nothing renamed
	}
	applyRename(&cfg, tabs)

	// orders: all three renames apply.
	if tabs[0].DstDB != "orders_v2" || tabs[0].DstSchema != "s_new" || tabs[0].DstName != "orders_v2" {
		t.Errorf("orders = %q/%q/%q, want orders_v2/s_new/orders_v2", tabs[0].DstDB, tabs[0].DstSchema, tabs[0].DstName)
	}
	// audit: schema renamed only; its own db + table name unchanged.
	if tabs[1].DstSchema != "s_new" || tabs[1].DstName != "audit" || tabs[1].DstDB != "audit" {
		t.Errorf("audit = %q/%q/%q, want audit/s_new/audit", tabs[1].DstDB, tabs[1].DstSchema, tabs[1].DstName)
	}
	// plain: nothing renamed -> Dst* equal the source names.
	if tabs[2].DstDB != "other" || tabs[2].DstSchema != "x" || tabs[2].DstName != "plain" {
		t.Errorf("plain = %q/%q/%q, want other/x/plain", tabs[2].DstDB, tabs[2].DstSchema, tabs[2].DstName)
	}
}


// TestSelectionBySchema verifies a bare schema selection matches by schema.
func TestSelectionBySchema(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	cfg.Schemas = []string{"s"}
	src := mkFake()
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(p.Units) != 2 {
		t.Fatalf("schema 's' produced %d units, want 2 (orders+audit)", len(p.Units))
	}
}
