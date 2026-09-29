package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// partSpec is the GP6 RANGE spec with a default partition (live shape, 29.09).
const partSpecWithDefault = `PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, DEFAULT PARTITION z )`
const partSpecNoDefault = `PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, PARTITION b START ('02') INCLUSIVE END ('03') EXCLUSIVE)`

// partitionedSourceFake returns a fake that models a Greenplum source with one
// partitioned parent (2 leaves) and one plain table. The gs-gp-probe answers
// non-nil (Greenplum), so collectTables sets Partitioned for the parent.
func partitionedSourceFake(spec string, keyType string) *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("gs-gp-probe", [][]any{{int32(16384)}})
	f.On("pg_class", [][]any{
		{"s", "events", "r", int64(458752), int64(2), int64(0), "srcdb"},
		{"s", "orders", "r", int64(1000), int64(0), int64(1), "srcdb"},
	})
	if spec != "" {
		f.On("pg_get_partition_def", [][]any{{spec}})
	}
	f.On("gs-partkey-type", [][]any{{"day", keyType}})
	f.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})
	return f
}

// TestPlannerMarksPartitionedParent verifies (Fix 1) that on a Greenplum
// source a parent with pg_inherits children is flagged Partitioned, and a
// plain table is not.
func TestPlannerMarksPartitionedParent(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := partitionedSourceFake(partSpecWithDefault, "text")
	dst := pg.NewFakeConn()
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var events, orders *Table
	for i := range p.Units {
		switch p.Units[i].Name {
		case "events":
			events = &p.Units[i]
		case "orders":
			orders = &p.Units[i]
		}
	}
	if events == nil || orders == nil {
		t.Fatalf("units = %+v, want events+orders", p.Units)
	}
	if !events.Partitioned {
		t.Fatal("events (2 children, GP source) must be Partitioned")
	}
	if events.Partition.Spec == "" || len(events.Partition.KeyColumns) != 1 || events.Partition.KeyColumns[0] != "day" {
		t.Fatalf("events partition spec = %+v", events.Partition)
	}
	if !events.Partition.HasDefault {
		t.Fatal("events HasDefault = false, want true")
	}
	if orders.Partitioned {
		t.Fatal("orders (no children) must not be Partitioned")
	}
}

// TestPlannerPartitionGuardNoDefault verifies the Fix-1 guard: a string-keyed
// partitioned parent WITHOUT a default partition on a collation-DIFFERENT
// destination is flagged with a diagnostic (dry-run warning / run-time early
// failure). The collation difference is detected behaviorally (the probe
// signatures differ), not by locale name.
func TestPlannerPartitionGuardNoDefault(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := partitionedSourceFake(partSpecNoDefault, "text")
	dst := pg.NewFakeConn()
	// The destination sorts the probe set differently (a different glibc).
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{"1",1,2}`}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for i := range p.Units {
		if p.Units[i].Name != "events" {
			continue
		}
		if p.Units[i].PartitionGuard == "" {
			t.Fatal("events (string key, no default, collation differs) must carry a PartitionGuard")
		}
		if !strings.Contains(p.Units[i].PartitionGuard, "DEFAULT partition") {
			t.Fatalf("guard = %q, want the DEFAULT-partition diagnostic", p.Units[i].PartitionGuard)
		}
		return
	}
	t.Fatal("events unit not found")
}

// TestPlannerPartitionGuardWithDefault verifies that the SAME parent WITH a
// default partition is NOT guarded: a row that matches no leaf bound lands in
// the default partition, so the copy is safe (re-routing is the intended
// Fix-1 behavior).
func TestPlannerPartitionGuardWithDefault(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := partitionedSourceFake(partSpecWithDefault, "text")
	dst := pg.NewFakeConn()
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{"1",1,2}`}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for i := range p.Units {
		if p.Units[i].Name == "events" && p.Units[i].PartitionGuard != "" {
			t.Fatalf("events (has default) must not be guarded, got %q", p.Units[i].PartitionGuard)
		}
	}
}

// TestPlannerPartitionGuardNumericKey verifies the guard does not apply to a
// numeric partition key (collation is irrelevant for numeric/date partitioning).
func TestPlannerPartitionGuardNumericKey(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := partitionedSourceFake(partSpecNoDefault, "integer")
	dst := pg.NewFakeConn()
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{"1",1,2}`}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for i := range p.Units {
		if p.Units[i].Name == "events" && p.Units[i].PartitionGuard != "" {
			t.Fatalf("events (integer key) must not be guarded, got %q", p.Units[i].PartitionGuard)
		}
	}
}

// TestPlannerPartitionGuardSameCollation verifies the guard does not fire when
// the source and destination collation behavior matches (same probe
// signature), even without a default partition.
func TestPlannerPartitionGuardSameCollation(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := partitionedSourceFake(partSpecNoDefault, "text")
	dst := pg.NewFakeConn()
	// Same behavioral signature as the source.
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for i := range p.Units {
		if p.Units[i].Name == "events" && p.Units[i].PartitionGuard != "" {
			t.Fatalf("events (same collation) must not be guarded, got %q", p.Units[i].PartitionGuard)
		}
	}
}

// TestPlannerPlainPGNoPartitionedFlag verifies (Fix 1) that on a plain
// PostgreSQL source (the Greenplum probe returns NULL) a parent with
// pg_inherits children is NOT flagged Partitioned: plain-PG pg_inherits is
// user inheritance, not partitioning, and its children stay in the plan.
func TestPlannerPlainPGNoPartitionedFlag(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := pg.NewFakeConn()
	// No gs-gp-probe entry -> the probe errors -> gpSource = false.
	src.On("pg_class", [][]any{
		{"s", "events", "r", int64(458752), int64(2), int64(0), "srcdb"},
	})
	dst := pg.NewFakeConn()

	p, err := Build(context.Background(), &cfg, src, dst)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(p.Units) != 1 {
		t.Fatalf("units = %d, want 1 (plain PG: no partition concept)", len(p.Units))
	}
	if p.Units[0].Partitioned {
		t.Fatal("plain-PG parent must not be flagged Partitioned")
	}
}

// collectTablesQuery returns the planner's table-collection query among the
// recorded fake queries. It is the only query that calls current_database()
// (a leaf partition's relfilenode carries the parent's rows, so the collection
// must enumerate parents, not leaves).
func collectTablesQuery(t *testing.T, src *pg.FakeConn) string {
	t.Helper()
	for _, q := range src.Queried() {
		if strings.Contains(q, "current_database()") {
			return q
		}
	}
	t.Fatalf("no collectTables query recorded (marker current_database()), got: %v", src.Queried())
	return ""
}

// TestPlannerLeafExclusionOnGP pins the Fix-1 mechanism that makes a
// partitioned unit parent-only: on a Greenplum source the planner's
// table-collection query must carry the leaf-exclusion filter, so a parent's
// leaf partitions are NOT planned as separate units (their rows ride the
// parent's single COPY; planning them separately would duplicate data and,
// under a differing glibc, route rows by the wrong collation). lab e2e
// (29.09): 7 leaves excluded, 3 parents = 3 units.
func TestPlannerLeafExclusionOnGP(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := partitionedSourceFake(partSpecWithDefault, "text")
	dst := pg.NewFakeConn()
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})

	if _, err := Build(context.Background(), &cfg, src, dst); err != nil {
		t.Fatalf("build: %v", err)
	}
	q := collectTablesQuery(t, src)
	if !strings.Contains(q, "NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)") {
		t.Fatalf("GP-source collectTables must exclude leaf partitions (NOT EXISTS pg_inherits), got:\n%s", q)
	}
}

// TestPlannerNoLeafExclusionOnPlainPG pins the Greenplum-CONDITIONAL nature of
// the exclusion: on a plain-PostgreSQL source pg_inherits is user inheritance,
// not partitioning, so the leaves are NOT a separate concern and must stay in
// the plan (their rows are not in the parent's COPY). A blanket exclusion
// would silently drop user-inheritance children on plain PG.
func TestPlannerNoLeafExclusionOnPlainPG(t *testing.T) {
	cfg := config.Default()
	cfg.Source = "host=src"
	cfg.Dest = "host=dst"
	src := pg.NewFakeConn()
	// No gs-gp-probe entry -> the probe errors -> gpSource = false.
	src.On("pg_class", [][]any{
		{"s", "events", "r", int64(458752), int64(2), int64(0), "srcdb"},
	})
	dst := pg.NewFakeConn()

	if _, err := Build(context.Background(), &cfg, src, dst); err != nil {
		t.Fatalf("build: %v", err)
	}
	q := collectTablesQuery(t, src)
	if strings.Contains(q, "NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)") {
		t.Fatalf("plain-PG collectTables must NOT exclude pg_inherits children (user inheritance), got:\n%s", q)
	}
}
