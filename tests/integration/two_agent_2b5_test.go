//go:build gp6lab

package integration

import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestTwoAgent2B5VanishedTable is the 2B-5 acceptance (architecture 08.10):
// the per-unit schema reads (columns, the zero-columns existence guard,
// distribution) run on the SOURCE AGENT, not the controller's own m.srcQ.
// The zero-columns guard (a source table dropped during the run) must fail
// ONLY that unit early — with the unit marked failed so a resume retries it —
// while the rest of the batch copies through. The guard's probe now rides the
// source agent's "table-exists" catalog op (its local pool, never the pinned
// FR-02 snapshot), so the test drives the REAL production process runner
// (coordinator mode) end to end:
//
//  1. two tables are seeded on the source; the PLAN is built (from the
//     source agent) while both exist;
//  2. one table is DROPPED from the source (simulating a concurrent drop);
//  3. mgr.Run must copy the surviving table in full and fail the vanished
//     one with the "does not exist" diagnostic (unit state failed) — without
//     aborting the batch and without the controller touching m.srcQ on the
//     two-agent path (it holds no direct source connection for these reads).
func TestTwoAgent2B5VanishedTable(t *testing.T) {
	const (
		srcPort = 5760
		dstPort = 5761
	)
	ctx := context.Background()

	bins := buildTwoAgentBinaries(t)

	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { dst.Close() })

	// Seed two tables.
	for _, name := range []string{"orders", "vanishing"} {
		if _, err := src.Exec(ctx, "CREATE TABLE "+name+" (id integer, note text)"); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		if _, err := src.Exec(ctx, "INSERT INTO "+name+" SELECT g, 'row' || g FROM generate_series(1, 500) g"); err != nil {
			t.Fatalf("seed %s rows: %v", name, err)
		}
	}

	cfg := config.Default()
	cfg.Source = srcCl.URL()
	cfg.Dest = dstCl.URL()
	cfg.ConflictAction = config.Overwrite
	cfg.WorkersPerPair = 1
	cfg.SourceAgentBinary = bins.srcAgent
	cfg.DestAgentBinary = bins.destAgent
	cfg.FrameAck = "on"
	cfg.AckTimeout = "30s"
	cfg.MaxReconnects = 20
	cfg.RetransmitBufferMax = 64 << 20

	// The REFERENCE plan (controller-side, local pools) is built while BOTH
	// tables exist — it must include both units.
	refCtx, refCancel := context.WithTimeout(ctx, 60*time.Second)
	ref, err := plan.Build(refCtx, &cfg, src, dst)
	refCancel()
	if err != nil {
		t.Fatalf("reference plan.Build: %v", err)
	}
	if len(ref.Units) != 2 {
		t.Fatalf("reference plan has %d units, want 2", len(ref.Units))
	}

	// The SOURCE-AGENT plan (the production path) is built while BOTH tables
	// still exist — it must include both units.
	journalPath := filepath.Join(t.TempDir(), "journal.jsonl")
	mgr := manifest.NewManager(&cfg, manifest.NewJournal(dst, journalPath))
	planCtx, planCancel := context.WithTimeout(ctx, 120*time.Second)
	defer planCancel()
	p, err := mgr.Plan(planCtx)
	if err != nil {
		t.Fatalf("mgr.Plan (source-agent plan): %v", err)
	}
	defer mgr.ReleaseAgents()
	if len(p.Units) != 2 {
		t.Fatalf("source-agent plan has %d units, want 2", len(p.Units))
	}

	// Simulate a concurrent drop: the source table vanishes AFTER the plan
	// (before the run) — the FR-02 snapshot is pinned by Plan, so the data
	// COPY for vanishing would stream its (pre-drop) rows, and it is the
	// zero-columns guard (a POOL read, not the snapshot) that must fail the
	// unit early, before any DDL or data.
	if _, err := src.Exec(ctx, "DROP TABLE vanishing"); err != nil {
		t.Fatalf("drop vanishing: %v", err)
	}

	runCtx, runCancel := context.WithTimeout(ctx, 180*time.Second)
	defer runCancel()
	runErr := mgr.Run(runCtx, p)
	// The vanished unit fails the batch (reported after in-flight units
	// finish); the surviving unit must still complete.
	if runErr == nil {
		t.Fatalf("run succeeded, want a unit failure (vanishing was dropped mid-run)")
	}

	// The surviving table was copied in full.
	rows, err := dst.Query(ctx, "SELECT count(*) FROM orders")
	if err != nil {
		t.Fatalf("dest orders: %v", err)
	}
	var n int64
	if len(rows) > 0 && len(rows[0]) > 0 {
		switch v := rows[0][0].(type) {
		case int64:
			n = v
		case int:
			n = int64(v)
		case float64:
			n = int64(v)
		}
	}
	if n != 500 {
		t.Fatalf("dest orders has %d rows, want 500 (the batch must not abort on one failed unit)", n)
	}

	// The journal: orders completed, vanishing failed (retryable on resume).
	states := map[string]string{}
	for _, u := range mgr.Journal().Units() {
		states[u.Schema+"."+u.Table] = string(u.State)
	}
	if states["public.orders"] != string(manifest.StateSwapped) {
		t.Errorf("journal: orders state = %q, want swapped (the destination agent performs the atomic rename; states=%v)", states["public.orders"], states)
	}
	if states["public.vanishing"] != "failed" {
		t.Errorf("journal: vanishing state = %q, want failed (the zero-columns guard marks it failed for a retryable resume; states=%v)", states["public.vanishing"], states)
	}
	if !strings.Contains(runErr.Error(), "does not exist") {
		t.Errorf("run error %q: want the 'does not exist' diagnostic from the zero-columns guard", runErr.Error())
	}
	t.Logf("2B-5: batch survived one dropped table (orders=completed, vanishing=failed; runErr=%v)", runErr)
}

// TestCatalogActionsWireRoundTrip (2B-5, architecture 08.10) proves the new
// SOURCE-AGENT catalog ops over the REAL control socket (the production
// process-runner path): table-layout (columns + distribution), table-exists
// (the zero-columns guard probe), and segment-count (the segreceiver count).
// A local pgtest postgres has no gp_segment_configuration, so the segment
// catalog is created with the rows a real Greenplum cluster would report
// (the same convention as the coordinator-guard tests).
func TestCatalogActionsWireRoundTrip(t *testing.T) {
	const srcPort = 5762
	ctx := context.Background()

	cl, err := pgtest.Start(srcPort, "gp_cat_2b5")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(cl.Stop)
	q, err := pg.NewPool(ctx, cl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	mustExec2B5(t, q, `CREATE TABLE cat_probe (id integer PRIMARY KEY, note text)`)
	mustExec2B5(t, q, `CREATE INDEX cat_probe_note_idx ON cat_probe(note)`)
	mustExec2B5(t, q, `CREATE TABLE gp_segment_configuration (
		hostname text, address text, port int, content int,
		role char(1), status char(1))`)
	mustExec2B5(t, q, `INSERT INTO gp_segment_configuration VALUES
		('localhost','127.0.0.1',`+itoa2B5(srcPort)+`, -1, 'p', 'u'),
		('localhost','127.0.0.1',`+itoa2B5(srcPort)+`, 0, 'p', 'u'),
		('localhost','127.0.0.1',`+itoa2B5(srcPort)+`, 1, 'p', 'u')`)

	// A free control port for the source agent.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctrlAddr := l.Addr().String()
	l.Close()

	srv := agent.NewSourceServer(agent.ServerOptions{
		SrcConn:  cl.URL(),
		Snapshot: agent.SnapNone, // no FR-02 snapshot: catalog reads only
		Listen:   ctrlAddr,
	})
	if _, err := srv.Start(ctx); err != nil {
		t.Fatalf("start source agent: %v", err)
	}
	t.Cleanup(srv.Close)
	client, err := agent.DialControl(ctx, ctrlAddr)
	if err != nil {
		t.Fatalf("dial control: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	// table-layout: the columns + distribution the manager used to read on
	// its own m.srcQ (format_type-faithful, value-identical over the wire).
	lay, err := client.Catalog(agent.CatalogReq{Action: "table-layout", Schema: "public", Table: "cat_probe"})
	if err != nil {
		t.Fatalf("catalog table-layout: %v", err)
	}
	if !lay.Ok {
		t.Fatalf("table-layout not ok: %s", lay.Error)
	}
	if len(lay.Columns) != 2 {
		t.Fatalf("table-layout columns = %d, want 2 (%+v)", len(lay.Columns), lay.Columns)
	}
	if lay.Columns[0].Name != "id" || lay.Columns[0].Type != "integer" {
		t.Errorf("column 0 = %+v, want id integer", lay.Columns[0])
	}
	if lay.Columns[1].Name != "note" || lay.Columns[1].Type != "text" {
		t.Errorf("column 1 = %+v, want note text", lay.Columns[1])
	}
	// Not a Greenplum source: the distribution degrades to the empty policy
	// (the same convention the in-process read has).
	if lay.Dist.Kind != "" {
		t.Errorf("distribution kind = %q, want \"\" (plain PostgreSQL)", lay.Dist.Kind)
	}

	// table-exists: the zero-columns guard probe (present + absent).
	ex, err := client.Catalog(agent.CatalogReq{Action: "table-exists", Schema: "public", Table: "cat_probe"})
	if err != nil {
		t.Fatalf("catalog table-exists: %v", err)
	}
	if !ex.Ok || !ex.Exists {
		t.Errorf("table-exists cat_probe = ok=%v exists=%v, want ok=true exists=true", ex.Ok, ex.Exists)
	}
	gone, err := client.Catalog(agent.CatalogReq{Action: "table-exists", Schema: "public", Table: "no_such_table"})
	if err != nil {
		t.Fatalf("catalog table-exists (absent): %v", err)
	}
	if !gone.Ok || gone.Exists {
		t.Errorf("table-exists no_such_table = ok=%v exists=%v, want ok=true exists=false", gone.Ok, gone.Exists)
	}

	// segment-count: the up primary CONTENTS (2 — the master content=-1 row
	// is excluded, exactly as the controller's old read did).
	sc, err := client.Catalog(agent.CatalogReq{Action: "segment-count"})
	if err != nil {
		t.Fatalf("catalog segment-count: %v", err)
	}
	if !sc.Ok {
		t.Fatalf("segment-count not ok: %s", sc.Error)
	}
	if sc.SegmentCount != 2 {
		t.Errorf("segment-count = %d, want 2 (the master row must not count)", sc.SegmentCount)
	}
	t.Logf("2B-5 source-agent catalog ops: table-layout=%d cols, exists probe ok, segment-count=%d", len(lay.Columns), sc.SegmentCount)
}

// TestDestSegmentsWireRoundTrip (2B-5, architecture 08.10) proves the new
// DESTINATION-AGENT "segments" ddl op over the real control socket: the
// up-primary enumeration + the dense-content validation move from the
// controller's m.dstQ to the agent (a fresh LOCAL coordinator connection).
func TestDestSegmentsWireRoundTrip(t *testing.T) {
	const (
		goodPort = 5763
		gapPort  = 5764
	)
	ctx := context.Background()

	good, err := pgtest.Start(goodPort, "gp_segs_ok")
	if err != nil {
		t.Fatalf("start good: %v", err)
	}
	t.Cleanup(good.Stop)
	gap, err := pgtest.Start(gapPort, "gp_segs_gap")
	if err != nil {
		t.Fatalf("start gap: %v", err)
	}
	t.Cleanup(gap.Stop)

	segDDL := `CREATE TABLE gp_segment_configuration (
		hostname text, address text, port int, content int,
		role char(1), status char(1))`
	// Good: contents 0,1 dense (plus the master row the query filters out).
	mustExec2B5(t, mustPool(t, ctx, good.URL()), segDDL)
	mustExec2B5(t, mustPool(t, ctx, good.URL()), `INSERT INTO gp_segment_configuration VALUES
		('localhost','127.0.0.1',`+itoa2B5(goodPort)+`, -1, 'p', 'u'),
		('localhost','127.0.0.1',`+itoa2B5(goodPort)+`, 0, 'p', 'u'),
		('localhost','127.0.0.1',`+itoa2B5(goodPort+1)+`, 1, 'p', 'u')`)
	// Gap: content 1 is DOWN (status 'd') -> contents 0,2 are not dense.
	mustExec2B5(t, mustPool(t, ctx, gap.URL()), segDDL)
	mustExec2B5(t, mustPool(t, ctx, gap.URL()), `INSERT INTO gp_segment_configuration VALUES
		('localhost','127.0.0.1',`+itoa2B5(gapPort)+`, -1, 'p', 'u'),
		('localhost','127.0.0.1',`+itoa2B5(gapPort)+`, 0, 'p', 'u'),
		('localhost','127.0.0.1',`+itoa2B5(gapPort+1)+`, 1, 'p', 'd'),
		('localhost','127.0.0.1',`+itoa2B5(gapPort+2)+`, 2, 'p', 'u')`)

	// Dense cluster: 2 up primaries, content 0,1.
	resp, err := destSegmentsOverControl(t, ctx, good.URL())
	if err != nil {
		t.Fatalf("segments (dense): %v", err)
	}
	if len(resp.Segments) != 2 {
		t.Fatalf("segments = %d, want 2 (%+v)", len(resp.Segments), resp.Segments)
	}
	if resp.Segments[0].Content != 0 || resp.Segments[1].Content != 1 {
		t.Errorf("segment contents = [%d, %d], want [0, 1]", resp.Segments[0].Content, resp.Segments[1].Content)
	}

	// Gapped cluster: the dense validation must REJECT it (a missing content
	// breaks slice coverage) — the manager degrades to the coordinator path.
	if resp, err := destSegmentsOverControl(t, ctx, gap.URL()); err == nil {
		t.Errorf("segments (gapped) succeeded with %d segments, want a dense-content error", len(resp.Segments))
	} else {
		t.Logf("2B-5 dest-agent segments op: dense ok (2 segments), gapped rejected: %v", err)
	}
}

// destSegmentsOverControl starts a long-lived in-process dest agent against
// dsn, issues the "segments" ddl op over its control socket, and returns the
// agent's answer (or the error the runner would surface).
func destSegmentsOverControl(t *testing.T, ctx context.Context, dsn string) (agent.DDResp, error) {
	t.Helper()
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dataAddr := l1.Addr().String()
	l1.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctrlAddr := l2.Addr().String()
	l2.Close()

	srv, err := agent.NewDestServer(agent.DestServerOptions{
		DataListen:    dataAddr,
		ControlListen: ctrlAddr,
		DestConn:      dsn,
		AcceptTimeout: 2 * time.Second,
		StreamTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDestServer: %v", err)
	}
	t.Cleanup(srv.Close)
	client, err := agent.DialDestControl(ctx, ctrlAddr)
	if err != nil {
		t.Fatalf("DialDestControl: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	resp, err := client.DDL(agent.DDReq{Action: "segments"})
	if err != nil {
		return resp, err
	}
	if !resp.Ok {
		return resp, &respError{msg: resp.Error}
	}
	return resp, nil
}

type respError struct{ msg string }

func (e *respError) Error() string { return e.msg }

func mustPool(t *testing.T, ctx context.Context, dsn string) *pg.Pool {
	t.Helper()
	p, err := pg.NewPool(ctx, dsn, "", "", "", "", 4)
	if err != nil {
		t.Fatalf("pool %s: %v", dsn, err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func mustExec2B5(t *testing.T, q *pg.Pool, sql string) {
	t.Helper()
	if _, err := q.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func itoa2B5(n int) string { return strconv.Itoa(n) }
