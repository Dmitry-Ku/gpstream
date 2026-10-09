package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestRenameTableBareKey guards the first live A->A mesh bug: the CLI
// documents --map-table src=dst (bare table name) and stores that bare key in
// RenameTable, but applyRename looked up only "schema.table". The bare key was
// silently ignored, the unit self-copied orders -> orders, and --overwrite
// dropped the source. Both key forms must work; the qualified key wins.
func TestRenameTableBareKey(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src dbname=d"
	cfg.Dest = "host=dst dbname=d"
	// Bare key only (what --map-table orders=orders_copy produces).
	cfg.RenameTable = map[string]string{"orders": "orders_copy"}

	tabs := []Table{
		{DB: "d", Schema: "s", Name: "orders"},
		{DB: "d", Schema: "s", Name: "audit"},
	}
	applyRename(&cfg, tabs)
	if tabs[0].DstName != "orders_copy" || tabs[0].DstSchema != "s" {
		t.Errorf("bare key: Dst = %s.%s, want s.orders_copy", tabs[0].DstSchema, tabs[0].DstName)
	}
	if tabs[1].DstName != "audit" || tabs[1].DstSchema != "s" {
		t.Errorf("audit must be untouched, got %s.%s", tabs[1].DstSchema, tabs[1].DstName)
	}
}

// TestRenameTableQualifiedKeyWins: when both the bare and the qualified key
// are present, the qualified one (spec example) wins.
func TestRenameTableQualifiedKeyWins(t *testing.T) {
	cfg := config.Default()
	cfg.RenameTable = map[string]string{
		"s.orders": "s2.orders_q",
		"orders":   "orders_bare",
	}
	tabs := []Table{{DB: "d", Schema: "s", Name: "orders"}}
	applyRename(&cfg, tabs)
	if tabs[0].DstName != "orders_q" || tabs[0].DstSchema != "s2" {
		t.Errorf("qualified key must win: Dst = %s.%s, want s2.orders_q", tabs[0].DstSchema, tabs[0].DstName)
	}
}

// TestRenameTableBareKeyBuildPath exercises the bare key through the REAL
// Build path (mkFake: two tables in schema "s").
func TestRenameTableBareKeyBuildPath(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src dbname=d"
	cfg.Dest = "host=dst dbname=d"
	cfg.RenameTable = map[string]string{"orders": "orders_copy"}

	src := mkFake()
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	found := false
	for _, u := range p.Units {
		if u.Name == "orders" {
			found = true
			if u.DstName != "orders_copy" || u.DstSchema != "s" {
				t.Errorf("orders Dst = %s.%s, want s.orders_copy", u.DstSchema, u.DstName)
			}
		}
	}
	if !found {
		t.Fatalf("orders unit missing from plan")
	}
}

// TestSelfCopyRejected: source and destination are the SAME cluster (identical
// DSN) and a unit would land on the same table -> the plan must be rejected
// (with --overwrite the swap drops the source: the first live A->A run).
func TestSelfCopyRejected(t *testing.T) {
	cfg := config.Default()
	same := "host=127.0.0.1 port=5432 dbname=t_live"
	cfg.Source = same
	cfg.Dest = same

	src := mkFake()
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})

	_, err := Build(context.Background(), &cfg, src, dst)
	if err == nil {
		t.Fatalf("self-copy plan must be rejected, got success")
	}
	if !strings.Contains(err.Error(), "self-copy") {
		t.Errorf("error = %q, want it to mention self-copy", err)
	}
}

// TestSelfCopyAllowedWithRename: the A->A dev scenario. Same cluster, but the
// table is renamed to a DIFFERENT destination name (orders -> orders_copy) —
// a normal in-cluster copy, which must pass the guard.
func TestSelfCopyAllowedWithRename(t *testing.T) {
	cfg := config.Default()
	same := "host=127.0.0.1 port=5432 dbname=t_live"
	cfg.Source = same
	cfg.Dest = same
	cfg.RenameTable = map[string]string{"orders": "orders_copy"} // bare key
	// Select ONLY orders: mkFake also lists audit, which the operator did
	// not rename — self-copying it would (correctly) trip the guard.
	cfg.Tables = []string{"orders"}

	src := mkFake()
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("in-cluster rename copy must pass the guard: %v", err)
	}
	for _, u := range p.Units {
		if u.Name == "orders" && u.DstName != "orders_copy" {
			t.Errorf("orders DstName = %q, want orders_copy", u.DstName)
		}
	}
}

// TestSelfCopyDifferentClusters: the same table name on TWO different clusters
// is a normal cross-cluster copy (the common A->D case) and must pass.
func TestSelfCopyDifferentClusters(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=10.0.0.1 port=5432 dbname=t_live"
	cfg.Dest = "host=10.0.0.2 port=8432 dbname=t_live"

	src := mkFake()
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"orders", "C"}})

	if _, err := Build(context.Background(), &cfg, src, dst); err != nil {
		t.Fatalf("cross-cluster same-name copy must pass: %v", err)
	}
}
