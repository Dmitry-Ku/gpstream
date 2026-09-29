package dest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestCreateStagingPartitionSpec verifies (Fix 1) that a partitioned parent's
// spec is reproduced in the destination DDL, and that the distribution clause
// precedes the PARTITION BY clause (GP6 requires "DISTRIBUTED BY" before
// "PARTITION BY"; live-verified 29.09 — the reverse order is a syntax error).
func TestCreateStagingPartitionSpec(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	cols := []pg.Column{
		{Name: "id", Type: "integer"},
		{Name: "day", Type: "text"},
	}
	dist := pg.Distribution{Kind: "hash", Keys: []string{"id"}}
	spec := pg.ParsePartitionSpec(
		`PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, DEFAULT PARTITION z )`)
	if err := a.CreateStaging(context.Background(), "public", cols, "_gs_j1_1", dist, spec); err != nil {
		t.Fatalf("create staging: %v", err)
	}
	last := f.Queried()[len(f.Queried())-1]
	// The distribution clause must come BEFORE the partition spec.
	di := strings.Index(last, "DISTRIBUTED BY (\"id\")")
	pi := strings.Index(last, "PARTITION BY RANGE")
	if di == -1 || pi == -1 {
		t.Fatalf("DDL missing clauses: %q", last)
	}
	if di > pi {
		t.Fatalf("DISTRIBUTED BY must precede PARTITION BY, got %q", last)
	}
	if !strings.Contains(last, "DEFAULT PARTITION z") {
		t.Fatalf("default partition missing: %q", last)
	}
}

// TestCreateStagingNoPartitionSpec verifies that a plain (non-partitioned)
// table emits no PARTITION BY clause (zero spec).
func TestCreateStagingNoPartitionSpec(t *testing.T) {
	f := pg.NewFakeConn()
	a := mkAgent(f)
	cols := []pg.Column{{Name: "id", Type: "integer"}}
	if err := a.CreateStaging(context.Background(), "public", cols, "_gs_j1_2", pg.Distribution{}, pg.PartitionSpec{}); err != nil {
		t.Fatalf("create staging: %v", err)
	}
	last := f.Queried()[len(f.Queried())-1]
	if strings.Contains(last, "PARTITION") {
		t.Fatalf("no PARTITION BY expected for a zero spec, got %q", last)
	}
}
