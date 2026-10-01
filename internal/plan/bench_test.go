package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// mkBenchFake returns a source fake with two tables in schema "s".
func mkBenchFake() pg.Querier {
	f := pg.NewFakeConn()
	f.On("pg_class", [][]any{
		{"s", "orders", "r", int64(1000), int64(0), int64(1), "srcdb"},
		{"s", "audit", "r", int64(500), int64(0), int64(0), "srcdb"},
	})
	return f
}

func TestBenchSelectsSourceTables(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	tabs, err := Bench(context.Background(), &cfg, mkBenchFake())
	if err != nil {
		t.Fatalf("bench: %v", err)
	}
	if len(tabs) != 2 {
		t.Fatalf("units = %d, want 2", len(tabs))
	}
	if tabs[0].Name != "audit" || tabs[1].Name != "orders" {
		t.Errorf("order = %s, %s (want audit, orders — planner order)", tabs[0].Name, tabs[1].Name)
	}
}

func TestBenchHonorsSelectionFlags(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Tables = []string{"srcdb.s.orders"}
	tabs, err := Bench(context.Background(), &cfg, mkBenchFake())
	if err != nil {
		t.Fatalf("bench: %v", err)
	}
	if len(tabs) != 1 || tabs[0].Name != "orders" {
		t.Fatalf("selection = %d unit(s), want only orders", len(tabs))
	}
}

func TestBenchEmptySelection(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Tables = []string{"srcdb.s.nope"}
	_, err := Bench(context.Background(), &cfg, mkBenchFake())
	if err == nil || !strings.Contains(err.Error(), "selection empty") {
		t.Fatalf("err = %v, want selection-empty diagnostic", err)
	}
}

func TestBenchSummaryLine(t *testing.T) {
	tabs := []Table{{Schema: "s", Name: "a", Bytes: 2048, Indexes: 2}, {Schema: "s", Name: "b", Bytes: 1024}}
	got := BenchSummaryLine(tabs)
	if got != "2 table(s), estimated 3.0 KiB" {
		t.Errorf("summary = %q", got)
	}
}
