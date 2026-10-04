package manifest

import (
	"context"
	"fmt"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// directDecisionRunner records which data path streamTwoAgentDirect selected
// for a unit: StreamSegment (the ON SEGMENT dispatch read, P8-1 direct_1to1
// fast-path / on_segment) or StreamDirect (coordinator COPY + RowRouter read,
// the router_full / no-seg-config path). It implements TwoAgentRunner with the
// minimum the decision under test touches; every other method panics.
type directDecisionRunner struct {
	segCalled    bool
	segDirect    bool // the segment request carried DirectWrite (ServeDirect write)
	directCalled bool
	directD      int32
	directKeys   int
}

func (f *directDecisionRunner) Start(context.Context) error   { return nil }
func (f *directDecisionRunner) Release(context.Context) error { return nil }
func (f *directDecisionRunner) Checksum(context.Context, string, string) (string, error) {
	return "", nil
}
func (f *directDecisionRunner) Bench(context.Context, uint32, uint32, string, string, string) (int64, uint64, uint64, error) {
	return 0, 0, 0, fmt.Errorf("unused")
}
func (f *directDecisionRunner) Stream(context.Context, unitStreamRequest) (int64, uint64, uint64, error) {
	return 0, 0, 0, fmt.Errorf("unused")
}
func (f *directDecisionRunner) StreamSegment(_ context.Context, req unitSegmentRequest) (int64, uint64, uint64, error) {
	f.segCalled = true
	f.segDirect = req.DirectWrite
	return 42, 0, 0, nil
}
func (f *directDecisionRunner) StreamDirect(_ context.Context, req unitDirectRequest) (int64, uint64, uint64, error) {
	f.directCalled = true
	f.directD = req.D
	f.directKeys = len(req.KeyIdx)
	return 42, 0, 0, nil
}

// directTestConns builds a source fake (Greenplum: 2 up primaries) and a
// destination fake (Greenplum: 2 up primaries, dense content 0..1) so
// primarySegmentCount/destSegments resolve the way a real GP6 does.
func directTestConns(t *testing.T) (src, dst *pg.FakeConn) {
	t.Helper()
	src = pg.NewFakeConn()
	src.On("count(DISTINCT content)", [][]any{{int64(2)}})
	dst = pg.NewFakeConn()
	dst.On("gp_segment_configuration", [][]any{
		{int32(0), "host0", int32(7433)},
		{int32(1), "host1", int32(7434)},
	})
	return src, dst
}

// directTestSetup builds a Manager in direct copy-mode over the fakes and
// returns it plus the decision recorder.
func directTestSetup(t *testing.T, mutate func(*config.Config)) (*Manager, *directDecisionRunner) {
	t.Helper()
	src, dst := directTestConns(t)
	cfg := config.Default()
	cfg.Dest = "postgres://u@destco:5432/db"
	cfg.CopyMode = config.CopyDirect
	if mutate != nil {
		mutate(&cfg)
	}
	m := NewManager(&cfg, src, dst, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	fake := &directDecisionRunner{}
	m.SetTwoAgentRunner(fake)
	return m, fake
}

func directTestUnit(mode route.Mode) *Unit {
	return &Unit{
		JobID:     "job",
		UnitID:    1,
		Schema:    "public",
		Table:     "orders",
		DstSchema: "public",
		DstName:   "orders",
		RouteMode: mode,
	}
}

func directTestTable() plan.Table {
	return plan.Table{Schema: "public", Name: "orders"}
}

var directTestCols = []pg.Column{
	{Name: "id", Type: "integer", Oid: 23},
	{Name: "customer", Type: "text", Oid: 25},
}
var directTestDist = pg.Distribution{Kind: "hash", Keys: []string{"id"}}

// TestDirect1to1ReadFastPathRouting is the P8-1 (direct-branch read fast-path)
// acceptance at the controller level: when the plan proves a table's route is
// direct_1to1 AND the ON SEGMENT dispatch is available (--seg-binary +
// --seg-listen), streamTwoAgentDirect must select the ON SEGMENT read
// (StreamSegment) instead of the coordinator COPY + RowRouter read
// (StreamDirect) — the per-row hash is pure overhead for direct_1to1 (d==s for
// every row). The write side is the same ServeDirect per-segment utility COPYs
// (DirectWrite on the segment request), preserving the R5 endgame. A
// router_full table, or a direct run without the seg config, keeps the
// coordinator + RowRouter path.
func TestDirect1to1ReadFastPathRouting(t *testing.T) {
	ctx := context.Background()

	t.Run("direct_1to1 + seg-config -> ON SEGMENT read (fast-path)", func(t *testing.T) {
		m, fake := directTestSetup(t, func(c *config.Config) {
			c.SegBinary = "/usr/local/bin/gpstream-segreceiver"
			c.SegListen = "10.0.0.5:9432"
		})
		rows, _, _, err := m.streamTwoAgentDirect(ctx, directTestTable(), directTestUnit(route.Direct1to1),
			"_gs_orders_1", "public", "orders", directTestCols, directTestDist, pg.PartitionSpec{})
		if err != nil {
			t.Fatalf("streamTwoAgentDirect: %v", err)
		}
		if !fake.segCalled {
			t.Fatalf("expected the ON SEGMENT read (StreamSegment), got the coordinator+RowRouter read (StreamDirect)")
		}
		if fake.directCalled {
			t.Fatalf("direct_1to1 + seg-config must not fall through to the coordinator+RowRouter path")
		}
		// The write side is the per-segment utility COPYs (R5 endgame preserved):
		// the segment request carries DirectWrite.
		if !fake.segDirect {
			t.Fatalf("expected DirectWrite on the segment request (ServeDirect per-segment COPYs), got merge path")
		}
		if rows != 42 {
			t.Fatalf("rows = %d, want the segment path's 42", rows)
		}
	})

	t.Run("direct_1to1 WITHOUT seg-config -> coordinator + RowRouter read", func(t *testing.T) {
		m, fake := directTestSetup(t, nil) // no SegBinary/SegListen
		rows, _, _, err := m.streamTwoAgentDirect(ctx, directTestTable(), directTestUnit(route.Direct1to1),
			"_gs_orders_1", "public", "orders", directTestCols, directTestDist, pg.PartitionSpec{})
		if err != nil {
			t.Fatalf("streamTwoAgentDirect: %v", err)
		}
		if fake.segCalled {
			t.Fatalf("no seg-config: must not use the ON SEGMENT read")
		}
		if !fake.directCalled {
			t.Fatalf("expected the coordinator + RowRouter read (StreamDirect)")
		}
		if fake.directD != 2 {
			t.Fatalf("RowRouter D = %d, want 2 (the destination's up primaries)", fake.directD)
		}
		if fake.directKeys != 1 {
			t.Fatalf("RowRouter key columns = %d, want 1 (id)", fake.directKeys)
		}
		_ = rows
	})

	t.Run("router_full -> coordinator + RowRouter read (no fast-path)", func(t *testing.T) {
		m, fake := directTestSetup(t, func(c *config.Config) {
			c.SegBinary = "/usr/local/bin/gpstream-segreceiver"
			c.SegListen = "10.0.0.5:9432"
		})
		_, _, _, err := m.streamTwoAgentDirect(ctx, directTestTable(), directTestUnit(route.RouterFull),
			"_gs_orders_1", "public", "orders", directTestCols, directTestDist, pg.PartitionSpec{})
		if err != nil {
			t.Fatalf("streamTwoAgentDirect: %v", err)
		}
		if fake.segCalled {
			t.Fatalf("router_full must not take the direct_1to1 ON SEGMENT fast-path")
		}
		if !fake.directCalled {
			t.Fatalf("expected the coordinator + RowRouter read (StreamDirect)")
		}
	})
}
