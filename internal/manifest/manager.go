// Package job also provides the Job Manager (Manager) that drives a real copy
// end-to-end (FR-01/FR-02/FR-06/FR-07/FR-17): for each planned unit it reads
// the schema off the source, creates a staging table on the destination,
// streams the data off the source (`COPY ... TO STDOUT`) over the network
// into the staging table (`COPY ... FROM STDIN`) without touching local
// storage (FR-01), verifies the content checksum (FR-02), rebuilds indexes
// (FR-17), and only then atomically renames the staging table to its final
// name (FR-02). Every transition is recorded in the operation journal so a
// crash can be resumed (FR-06/FR-07).
//
// The Manager is exercised by the integration tests against a real PostgreSQL
// server (two embedded clusters acting as source and destination), so the
// data path is verified against real COPY FROM/TO rather than fakes.
package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/index"
	"github.com/gpstream/gpstream/internal/monitor"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/ratelimit"
	"github.com/gpstream/gpstream/internal/route"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// Manager orchestrates a copy across the planned units.
type Manager struct {
	cfg     *config.Config
	srcQ    pg.StreamingQuerier
	dstQ    pg.StreamingQuerier
	src     *source.Agent
	dst     *dest.Agent
	journal *Journal
	index   *index.Builder
	idxPool *index.Pool // background index pool (FR-17); started before the
	// data path so each unit's index build overlaps the rest of the copy,
	// drained (Close + Wait) after it
	// idxSeen marks units whose index build this run has enqueued, so the
	// journal scan does not enqueue them twice (FR-17 resume).
	idxSeen map[uint32]bool
	idxMu   sync.Mutex // guards idxSeen
	monitor *monitor.Metrics
	comp    *compress.Compressor
	dec     *compress.Decompressor
	// compMode is the operator's --compress mode (FR-12). compNone/decNone
	// are the mode-none pair built when compMode != none; the F-12 small-table
	// auto-disable (F-12: "very small tables — compression is disabled
	// automatically") swaps a unit's transport onto them.
	compMode compress.Mode
	compNone *compress.Compressor
	decNone  *compress.Decompressor
	lim      *ratelimit.Limiter // adaptive rate limiter (FR-14); nil when unlimited
	statMu   sync.Mutex         // guards Stats, which multiple workers mutate under the FR-03 pool
	// mu_runstats guards the runstats.json refresh (FR-04): the heartbeat
	// goroutine and the stop hook both call write().
	mu_runstats sync.Mutex
	// jobID is the operation identity this Manager run journals under
	// (T5: the gpstream_jobs metadata key; set by Run before the units are
	// processed).
	jobID string
	// epoch is this run's snapshot generation (T5, DESIGN §2.2): stamped on
	// every unit the run verifies (runUnit step 8).
	epoch int64
	// reRead holds the logical-table keys that a resume must re-copy under
	// the fresh snapshot (T5, DESIGN §2.2); empty on a fresh run.
	reRead map[string]bool
	// twoAgent is the control-plane runner for the two-agent data path (ADR-002
	// remediation): non-nil only in that topology, where it drives the source /
	// destination agent binaries and the data COPY runs only in the agents.
	// nil in the default single topology (the data path stays in-process).
	twoAgent TwoAgentRunner
	// events is the F-04/F-14 diagnostic event stream (events.jsonl): rate
	// shifts, conflicts, unit failures and re-reads. No-op when the journal
	// has no file mirror.
	events *monitor.EventLog
	// Stats accumulates the observed result of the run.
	Stats
}

// Stats is the observed result of a Manager run (used for reporting; wire
// bytes are exposed via Metrics(), not here).
type Stats struct {
	Rows     int64 // rows copied (FR-02)
	Units    int   // units processed
	Comps    int   // units verified with --verify on (FR-02)
	Skipped  int   // units skipped by --resolve-conflicts skip (FR-10/FR-11)
	Failures int   // units that failed after retries
	// Balances is the F-15 live segment-fill balance measured per copied
	// table on the destination (max/avg over gp_segment_id). Empty on a
	// non-Greenplum destination (no segments to measure).
	Balances []dest.Balance
}

// Metrics returns the collector so the caller can expose it (FR-04). It is the
// same collector the limiter/compression degrade against, so a rate-limit or
// compression change is observable as a metric (FR-04/FR-12/FR-14).
func (m *Manager) Metrics() *monitor.Metrics { return m.monitor }

// NewManager creates a Manager for a copy between two streaming connections.
func NewManager(cfg *config.Config, srcQ, dstQ pg.StreamingQuerier, j *Journal) *Manager {
	mode, level, err := compress.Parse(string(cfg.Compression))
	if err != nil {
		mode, level = compress.ModeZstd, 3
	}
	m := &Manager{
		cfg:      cfg,
		srcQ:     srcQ,
		dstQ:     dstQ,
		src:      source.New(srcQ),
		dst:      dest.New(dstQ),
		journal:  j,
		index:    index.New(cfg),
		idxPool:  index.NewPool(cfg.IndexWorkers, cfg.NoIndexes, cfg.IndexPriority == "size"),
		idxSeen:  map[uint32]bool{},
		monitor:  monitor.New(),
		events:   monitor.NewEventLog(j.EventsPath()),
		comp:     compress.New(mode, level),
		dec:      compress.NewDec(mode),
		compMode: mode,
	}
	// 07.10: primary-journal (dest DB) write errors are no longer swallowed
	// (a GP6/PG9.4 upsert failure used to die silently, leaving
	// gpstream_units/gpstream_jobs empty) — they land in events.jsonl so an
	// operator sees a broken pg journal. The file mirror stays the resume
	// source, so the job still completes (FR-07).
	if j != nil {
		j.Note(func(msg string) {
			m.events.Log("journal_pg_error", "msg", msg)
		})
	}
	// F-12 small-table auto-disable: when the operator chose a real
	// compression mode, prebuild the none-mode pair so a small unit's
	// transport (per unit, decided in the two-agent data path).
	if mode != compress.ModeNone {
		m.compNone = compress.New(compress.ModeNone, 0)
		m.decNone = compress.NewDec(compress.ModeNone)
	}
	// FR-17/FR-04: expose the background index pool's progress as metrics.
	m.idxPool.SetOnProgress(func(queueDepth, done int) {
		m.monitor.SetIndexProgress(queueDepth, done)
	})
	// FR-14: when a rate-limit is set, build the adaptive limiter over the host
	// metrics provider. It tracks the controller host's load and publishes
	// rate_shift diagnostics (FR-04) as a metric + event. NOTE: the
	// token-bucket pacer (internal/ratelimit) is NOT wired into the two-agent
	// data path yet (backlog) — so the limiter is diagnostic-only for now and
	// the value's concrete effect is the ETA baseline; the flow is paced by
	// backpressure only.
	if cfg.RateLimit > 0 {
		lim := ratelimit.New(ratelimit.Options{
			Baseline: cfg.RateLimit,
			Provider: ratelimit.NewProcProvider(),
		})
		lim.SetOnChange(m.onRateChange)
		m.lim = lim
		m.monitor.SetRateLimit(cfg.RateLimit)
	}
	return m
}

// onRateChange is the limiter's diagnostic callback (FR-14). A rate shift is
// observable two ways: as a metric (FR-04) and as an event in events.jsonl
// (F-14: "every limit shift — a diagnostic message in events.jsonl
// (pair, old/new, cause: cpu %, disk latency, queue)").
func (m *Manager) onRateChange(from, to ratelimit.Gbps, reason string) {
	m.monitor.SetRateLimit(to)
	m.events.Log("rate_limit_shift",
		"from_gbps", float64(from), "to_gbps", float64(to), "reason", reason)
}

// compressionOffFor implements the F-12 small-table auto-disable: a table
// whose estimated size (plan.Table.Bytes = pg_total_relation_size) is below
// --small-table-threshold is copied with compression OFF — the per-stream
// compressor's fixed cost (encoder init, block framing, CRC) does not pay
// back on tiny payloads. An unknown size (0) or a disabled threshold (0)
// keeps the operator's mode. The decision is per unit, so a run copies small
// tables uncompressed and large ones compressed, each with its own pipe.
func (m *Manager) compressionOffFor(t plan.Table) bool {
	return m.compMode != compress.ModeNone &&
		m.cfg.SmallTableThreshold > 0 &&
		t.Bytes > 0 && t.Bytes < m.cfg.SmallTableThreshold
}

// coordinatorFallbackFor reports whether a small table (below
// --small-table-threshold) must take the coordinator data path even when
// --copy-mode is on_segment (P1b). One knob drives both small-table
// behaviors: the same table for which per-slice compression is not worth
// the encoder cost (F-12) is also one for which a segment-to-segment
// pipeline is not worth the S segreceivers + S destination connections.
// The metric is t.Bytes (pg_total_relation_size: a cheap local coordinator
// catalog read, done once per table at planning). An unknown size (0) or a
// disabled threshold (0) keeps the operator's mode; the boundary is strict
// (a table exactly at the threshold stays on the segment path).
func (m *Manager) coordinatorFallbackFor(t plan.Table) bool {
	return m.cfg.CopyMode == config.CopyMesh &&
		!t.SrcReplicated &&
		m.cfg.SmallTableThreshold > 0 &&
		t.Bytes > 0 && t.Bytes < m.cfg.SmallTableThreshold
}

// unitCompressors returns the compressor/decompressor pair the unit's
// transport must use: the operator's mode, or the none pair when the F-12
// small-table auto-disable applies. The destination side must use the SAME
// mode as the source side of the pipe, so both come from one decision.
func (m *Manager) unitCompressors(t plan.Table) (comp *compress.Compressor, dec *compress.Decompressor) {
	if m.compressionOffFor(t) {
		return m.compNone, m.decNone
	}
	return m.comp, m.dec
}

// Run executes the copy for the plan (FR-01/FR-02). Units are processed through
// a bounded worker pool of cfg.WorkersPerPair (FR-03). The flow is paced by
// backpressure; when a rate-limit is set the shared adaptive limiter runs
// diagnostic-only (the token-bucket pacer is not wired into the data path yet —
// backlog, FR-14). Each unit is fully journaled so the run is resumable
// (FR-06/FR-07). A first unit failure is reported after the in-flight units
// finish (the pool does not cancel the batch).
// SetTwoAgentRunner injects the two-agent control-plane runner (the
// integration test injects the in-process runner so the Manager's two-agent
// orchestration runs against the agent package without subprocesses). Only
// meaningful with cfg.Topology == TopologyTwoAgent; Run uses the injected
// runner as-is (it does not build the process runner when one is set).
func (m *Manager) SetTwoAgentRunner(r TwoAgentRunner) {
	m.twoAgent = r
}

// --- 2B-2 (architecture 08.10): destination DDL delegated to the dest agent. ---
// The controller holds no direct destination connection in the two-agent
// topology: the destination DDL it needs (the conflict exists-check, the
// metadata-only table create, the resume swap, the on-error staging cleanup)
// runs on the destination agent over the "ddl" control op. In the
// single-process topology (dead in production — Run is ALWAYS two-agent) the
// helpers fall back to the controller's own destination pool; 2B-7 removes
// that fallback together with m.dst/m.dstQ.

// dstExists reports whether the final table exists on the destination.
func (m *Manager) dstExists(ctx context.Context, schema, table string) (bool, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "exists", Schema: schema, Table: table})
		if err != nil {
			return false, err
		}
		return resp.Exists, nil
	}
	return m.dst.Exists(ctx, schema, table), nil
}

// dstCreateTable creates the destination table from the source layout.
func (m *Manager) dstCreateTable(ctx context.Context, schema string, cols []pg.Column, table string, dist pg.Distribution, part pg.PartitionSpec) error {
	if m.twoAgent != nil {
		_, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "create-table", Schema: schema, Table: table, Cols: cols, Dist: dist, Part: part})
		return err
	}
	return m.dst.CreateTable(ctx, schema, cols, table, dist, part)
}

// dstDropStaging drops the unit's staging table (best-effort on-error cleanup).
func (m *Manager) dstDropStaging(ctx context.Context, schema, staging string) error {
	if m.twoAgent != nil {
		_, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "drop-staging", Schema: schema, Staging: staging})
		return err
	}
	return m.dst.DropStaging(ctx, schema, staging)
}

// dstSwap atomically renames the staging table to its final name.
func (m *Manager) dstSwap(ctx context.Context, schema, table, staging string, overwrite bool) error {
	if m.twoAgent != nil {
		_, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "swap", Schema: schema, Table: table, Staging: staging, Overwrite: overwrite})
		return err
	}
	return m.dst.Swap(ctx, schema, table, staging, overwrite)
}

// --- 2B-3 (architecture 08.10): per-unit catalog ops delegated to the agents. ---
// The source catalog reads (sequence discovery, sequence last_value,
// constraint definitions) run on the SOURCE agent (its local source pool —
// the catalog-querier convention, not the pinned snapshot); the destination
// writes/reads (create/setval sequence, add constraints, segment balance)
// run on the DESTINATION agent. The controller keeps only the pure
// rename-rewrite logic (PK names, FK refs) — no DB. Single-process fallback
// (dead in production) uses the controller's own pools; 2B-7 removes it.

// srcSequences lists the table's serial sequences (F-11 metadata phase).
func (m *Manager) srcSequences(ctx context.Context, schema, table string) ([]source.SequenceDef, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.Catalog(ctx, agent.CatalogReq{Action: "sequences", Schema: schema, Table: table})
		if err != nil {
			return nil, err
		}
		return resp.Sequences, nil
	}
	return source.New(m.srcQ).Sequences(ctx, schema, table)
}

// srcSequenceLastValue reads a sequence's last_value (F-11 post-load sync).
func (m *Manager) srcSequenceLastValue(ctx context.Context, seq string) (int64, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.Catalog(ctx, agent.CatalogReq{Action: "sequence-last-value", Seq: seq})
		if err != nil {
			return 0, err
		}
		return resp.LastValue, nil
	}
	schema, base := seq, seq
	if i := strings.LastIndex(seq, "."); i >= 0 {
		schema, base = seq[:i], seq[i+1:]
	}
	return source.New(m.srcQ).SequenceLastValue(ctx, schema, base)
}

// srcConstraints reads the table's constraint definitions (F-11).
func (m *Manager) srcConstraints(ctx context.Context, schema, table string) ([]pg.ConstraintDef, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.Catalog(ctx, agent.CatalogReq{Action: "constraints", Schema: schema, Table: table})
		if err != nil {
			return nil, err
		}
		return resp.Constraints, nil
	}
	return source.New(m.srcQ).Constraints(ctx, schema, table)
}

// dstCreateSequence recreates a source sequence on the destination (F-11).
func (m *Manager) dstCreateSequence(ctx context.Context, name string) error {
	if m.twoAgent != nil {
		_, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "create-sequence", Sequence: name})
		return err
	}
	return m.dst.CreateSequence(ctx, name)
}

// dstSetSequenceValue resynchronises a destination sequence (F-11).
func (m *Manager) dstSetSequenceValue(ctx context.Context, schema, seq string, last int64) error {
	if m.twoAgent != nil {
		_, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "set-sequence-value", Sequence: schema, Seq: seq, LastValue: last})
		return err
	}
	return m.dst.SetSequenceValue(ctx, schema, seq, last)
}

// dstAddConstraints applies the (already destination-rewritten) constraint
// definitions on the destination (F-11).
func (m *Manager) dstAddConstraints(ctx context.Context, schema, table string, defs []pg.ConstraintDef) error {
	if m.twoAgent != nil {
		_, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "add-constraints", Schema: schema, Table: table, Constraints: defs})
		return err
	}
	return m.dst.Constraints(ctx, schema, table, defs)
}

// dstBalance measures the destination table's per-segment fill (F-15).
func (m *Manager) dstBalance(ctx context.Context, schema, table string) (dest.Balance, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "balance", Schema: schema, Table: table, Threshold: dest.DefaultBalanceThreshold})
		if err != nil {
			return dest.Balance{}, err
		}
		return resp.Balance, nil
	}
	return m.dst.BalanceTable(ctx, schema, table, dest.DefaultBalanceThreshold), nil
}

// --- 2B-4 (architecture 08.10): FR-17 index ops delegated to the agents. ---
// The source-catalog read (the table's index definitions) runs on the SOURCE
// agent; the build runs on the DESTINATION agent. The controller keeps only
// the pure poolIndexDefs filter (PK exclusion — the post-swap constraint pass
// builds the PK index atomically) — no DB. Single-process fallback (dead in
// production) uses the controller's own pools; 2B-7 removes it.

// srcIndexes reads the table's index definitions (FR-17) via the source
// agent's catalog op (its local source pool — the catalog-querier
// convention, not the pinned snapshot).
func (m *Manager) srcIndexes(ctx context.Context, schema, table string) ([]pg.IndexDef, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.Catalog(ctx, agent.CatalogReq{Action: "indexes", Schema: schema, Table: table})
		if err != nil {
			return nil, err
		}
		return resp.Indexes, nil
	}
	return source.New(m.srcQ).Indexes(ctx, schema, table)
}

// dstCreateIndexes builds the (already destination-rewritten) index
// definitions on the destination (FR-17) via the destination agent's
// create-indexes ddl op (fresh LOCAL coordinator connection, idempotent
// per index: existing -> REINDEX, so a resume re-run never duplicates).
func (m *Manager) dstCreateIndexes(ctx context.Context, schema, table, srcSchema, srcTable string, defs []pg.IndexDef) error {
	if m.twoAgent != nil {
		_, err := m.twoAgent.DDL(ctx, agent.DDReq{
			Action:    "create-indexes",
			Schema:    schema,
			Table:     table,
			SrcSchema: srcSchema,
			SrcTable:  srcTable,
			Indexes:   defs,
		})
		return err
	}
	return m.dst.CreateIndexes(ctx, schema, table, srcSchema, srcTable, defs)
}

// --- 2B-5 (architecture 08.10): the last per-unit DB reads delegated. ---
// The per-unit schema reads (the table's columns + distribution policy, the
// zero-columns existence guard, the source segment count) run on the SOURCE
// agent (its local source pool — the catalog-querier convention, not the
// pinned FR-02 snapshot); the destination segment enumeration (the
// mesh/coordinator topology probe) runs on the DESTINATION agent. The
// controller keeps no DB of its own on the two-agent topology — only pure
// logic (the partition-guard decision, the DSN rewrite, the route decision).
// Single-process fallback (dead in production) uses the controller's own
// pools; 2B-7 removes it.

// srcColumns reads the table's column definitions (FR-02/FR-03: the
// destination DDL is built from the source's own column layout) via the
// source agent's table-layout catalog op.
func (m *Manager) srcColumns(ctx context.Context, schema, table string) ([]pg.Column, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.Catalog(ctx, agent.CatalogReq{Action: "table-layout", Schema: schema, Table: table})
		if err != nil {
			return nil, err
		}
		return resp.Columns, nil
	}
	return source.New(m.srcQ).Columns(ctx, schema, table)
}

// srcTableExists is the zero-columns guard's probe (a vanished source table
// yields ZERO columns — the guard must distinguish that from a catalog
// fault and fail the unit early, before any DDL or data, so the error stays
// contained on a pool connection and cannot abort the FR-02 snapshot).
func (m *Manager) srcTableExists(ctx context.Context, schema, table string) bool {
	if m.twoAgent != nil {
		return m.twoAgent.TableExists(ctx, schema, table)
	}
	return source.New(m.srcQ).TableExists(ctx, schema, table)
}

// srcDistribution reads the table's distribution policy (F-11) via the
// source agent's table-layout catalog op. A catalog fault there degrades to
// the empty policy (the destination uses its own default) — the same
// non-fatal convention the in-process Distribution read had.
func (m *Manager) srcDistribution(ctx context.Context, schema, table string) pg.Distribution {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.Catalog(ctx, agent.CatalogReq{Action: "table-layout", Schema: schema, Table: table})
		if err != nil {
			return pg.Distribution{}
		}
		return resp.Dist
	}
	return source.New(m.srcQ).Distribution(ctx, schema, table)
}

// srcPrimarySegmentCount returns the number of up primary CONTENTS on the
// source (the segreceiver count for the ON SEGMENT dispatch) via the source
// agent's segment-count catalog op. It requires a Greenplum source
// (gp_segment_configuration); on plain PostgreSQL it errors, which the
// caller surfaces as a clear "segment-direct requires Greenplum" failure.
func (m *Manager) srcPrimarySegmentCount(ctx context.Context) (int, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.Catalog(ctx, agent.CatalogReq{Action: "segment-count"})
		if err != nil {
			return 0, err
		}
		if resp.SegmentCount <= 0 {
			return 0, fmt.Errorf("cannot enumerate segments (segment-direct requires a Greenplum source)")
		}
		return int(resp.SegmentCount), nil
	}
	return m.primarySegmentCount(ctx)
}

// Plan (Phase 2, 2A) builds the copy plan ON THE SOURCE AGENT: it brings up
// the two agents (Start), asks the source agent to run plan.Build (its LOCAL
// source pool + a destination querier proxied over the source->dest peer
// channel), and returns the finished plan. The controller opens NO direct
// destination connection for planning (architecture 08.10). On SUCCESS the
// agents are left running (Run reuses them — Start is idempotent); on FAILURE
// the agents are released. The production run flow is:
//
//	mgr.Plan(ctx) -> p  (agents up, plan from the source agent)
//	mgr.Run(ctx, p)     (agents already up; the copy)
//
// A test may inject an in-process runner (SetTwoAgentRunner) whose Plan builds
// the plan in-process (no subprocesses).
func (m *Manager) Plan(ctx context.Context) (*plan.Plan, error) {
	if m.twoAgent == nil {
		m.twoAgent = newProcessTwoAgentRunner(m.cfg)
	}
	if err := m.twoAgent.Start(ctx); err != nil {
		return nil, fmt.Errorf("two-agent: %w", err)
	}
	p, err := m.twoAgent.Plan(ctx)
	if err != nil {
		// Plan failed: release the agents (Run is not reached on a hard
		// error; on a PARTIAL plan Run may still be called — the runner is
		// idempotent, so a second Start/Plan/Run is safe).
		_ = m.twoAgent.Release(context.Background())
		return p, fmt.Errorf("two-agent: plan: %w", err)
	}
	return p, nil
}

// ReleaseAgents tears down the two agents if they were started (by Plan). It
// is idempotent (the runner's Release nil-guards), so calling it before Run
// (the empty-plan case) is safe. Run's own deferred Release is the normal
// teardown; this is for the paths that never reach Run.
func (m *Manager) ReleaseAgents() {
	if m.twoAgent != nil {
		_ = m.twoAgent.Release(context.Background())
	}
}

// Journal exposes the run's journal (the in-memory unit states + the file
// mirror). Tests use it to assert the per-unit states after a run; the
// production paths use the journal directly.
func (m *Manager) Journal() *Journal { return m.journal }

func (m *Manager) Run(ctx context.Context, p *plan.Plan) error {
	// Ensure the primary journal table exists (ADR-001 §4.3); the file
	// mirror is the durable fallback if this fails (FR-07).
	if err := m.journal.Init(ctx); err != nil {
		// Non-fatal: the file mirror still makes the job resumable.
	}

	// T5 (DESIGN §2.2): a run may CONTINUE an earlier run of the same
	// operation — the operator re-invokes `gpstream run` with the same
	// --operation-id after a crash. Load the durable journal mirror first so
	// the run sees the states (and the snapshot epochs) the earlier run
	// recorded; without this the in-memory journal is empty and the run would
	// blindly re-copy verified data.
	if err := m.journal.LoadFromMirror(); err != nil {
		return fmt.Errorf("load journal mirror: %w", err)
	}
	// F-04/F-14: the diagnostic event stream is open for the whole run;
	// close it (and flush the file) when the run ends, whatever the outcome.
	defer m.events.Close()
	m.jobID = jobID(p)

	// F-04/F-16: the detected collation differences (DB-level and
	// column-level) are diagnostic events — they change string sort/index
	// semantics on the destination, so they belong in events.jsonl like any
	// other operator-facing diagnostic (the warning is also printed in the
	// final report, main.go).
	for _, cd := range p.Collations {
		switch {
		case cd.IsColumn():
			m.events.Log("collation_diff", "level", "column",
				"table", cd.Table, "column", cd.Column,
				"source", cd.SrcCollation, "destination", cd.DstCollation)
		case cd.IsEncoding():
			m.events.Log("encoding_diff", "level", "db",
				"db", cd.DB, "encoding", cd.Encoding)
		default:
			m.events.Log("collation_diff", "level", "db",
				"db", cd.DB, "lc_collate", cd.LCCollate, "lc_ctype", cd.LCType)
		}
	}
	// F-11 metadata phase: dependent extensions used by the copied columns but
	// missing on the destination (MADlib/PostGIS/orafce, ...). Their column
	// DDL will fail at run time until installed in the receiver; log the
	// finding up front (F-04 diagnostic stream) so the operator sees it before
	// the first unit fails.
	if len(p.MissingExtensions) > 0 {
		m.events.Log("extension_missing",
			"level", "db",
			"extensions", strings.Join(p.MissingExtensions, ","),
			"reason", "source column types depend on extensions not installed in the destination (F-11 metadata phase); CREATE EXTENSION in the receiver before the DDL of those columns")
	}

	// T5 (DESIGN §2.2): determine this run's snapshot epoch. A fresh
	// operation starts at epoch 1; a resume (journal records or metadata
	// already exist) continues at lastEpoch+1 — the snapshot cannot survive
	// the death of its session, so every resume reads under a FRESH snapshot
	// and the epoch records the generation. The metadata is persisted before
	// any unit is processed, so the epoch the run's snapshot actually was is
	// recorded no matter where the run dies.
	epoch, isResume, err := m.establishEpoch()
	if err != nil {
		return err
	}
	m.epoch = epoch

	// T5 (DESIGN §2.2): the resume re-read decision. Only tables whose
	// completed units span more than one snapshot epoch — or that have a
	// non-terminal unit — are re-copied under the fresh snapshot; a table
	// completed in a single epoch is kept (its data is verified under one
	// consistent snapshot). Fresh runs decide nothing.
	m.reRead = map[string]bool{}
	if isResume {
		m.reRead = reReadTables(m.journal.Units())
	}

	// FR-02 consistency: establish the source read snapshot BEFORE any unit is
	// copied so every COPY of the database reads from the same point-in-time
	// (see establishSnapshot). A snapshot error is non-fatal (the copy still
	// proceeds) but is logged so the operator can see the consistency mode.
	//
	// P7: the data path is ALWAYS two-agent. Launch the long-lived source
	// agent (owns the source pg pool + the run-wide FR-02 snapshot) and
	// connect the control client. The data COPY runs only in the agents; the
	// controller holds only control connections (DDL, journal, verify, swap).
	// A test may pre-inject an in-process runner (m.twoAgent != nil) to drive
	// the agent package without subprocesses.
	if m.twoAgent == nil {
		m.twoAgent = newProcessTwoAgentRunner(m.cfg)
	}
	if err := m.twoAgent.Start(ctx); err != nil {
		return fmt.Errorf("two-agent: %w", err)
	}
	defer func() { _ = m.twoAgent.Release(context.Background()) }()

	// FR-14: sample controller-host load periodically so the limiter publishes
	// rate-shift diagnostics. No-op when no rate-limit is set; the limiter is
	// diagnostic-only until the pacer is wired into the data path (backlog).
	stopSampler := m.startLimiterSampler(ctx)
	if stopSampler != nil {
		defer stopSampler()
	}

	// Build the unit list (unit-id and schema fallback, FR-02/FR-11).
	units := make([]*Unit, 0, len(p.Units))
	for i, t := range p.Units {
		unit := &Unit{
			JobID:     jobID(p),
			UnitID:    int64(i + 1),
			DB:        t.DB,
			Schema:    t.Schema,
			Table:     t.Name,
			DstSchema: t.DstSchema,
			DstName:   t.DstName,
			Kind:      "table",
			Bytes:     t.Bytes,
			Conflict:  t.HasConflict,
			// Fix 1: the partition spec (reproduced on the destination) and
			// the planner's guard (a non-empty guard fails the unit early).
			Partition:      t.Partition,
			PartitionGuard: t.PartitionGuard,
			RouteMode:      t.RouteMode,
			SrcReplicated:  t.SrcReplicated,
			RouteReason:    t.RouteReason,
		}
		// Fall back to the source names when a rename did not supply a
		// destination (applyRename defaults them, but a hand-built plan may
		// leave them empty).
		if unit.DstSchema == "" {
			unit.DstSchema = unit.Schema
		}
		if unit.DstName == "" {
			unit.DstName = unit.Table
		}
		// T5 (DESIGN §2.2): inherit the state the earlier run of this
		// operation recorded in the journal mirror (loaded at Run start),
		// so processUnit can apply the keep / re-read decision. A fresh
		// operation has no record for the unit; an earlier run's state is
		// what the epoch rules reason about (completed in one epoch ->
		// keep; mixed epochs or strict -> re-read).
		if prev := m.journal.Get(unit.UnitID); prev != nil {
			unit.State = prev.State
			unit.Epoch = prev.Epoch
			unit.Checksum = prev.Checksum
			unit.RowCount = prev.RowCount
			unit.Attempts = prev.Attempts
		}
		units = append(units, unit)
	}

	// FR-04: seed the metrics with the planned volume (for the ETA), the host
	// pair label, and the initial state of every unit (so the per-status gauge
	// and active-flows gauge start complete rather than from zero).
	m.monitor.SetTotalBytes(p.TotalBytes)
	if pair := hostPairLabel(m.cfg.Source, m.cfg.Dest); pair != "" {
		m.monitor.SetPair(pair)
	}
	for _, u := range units {
		m.monitor.SetUnitState(unitTableKey(u), string(u.State))
	}

	// FR-04: refresh runstats.json so `gpstream status` can show a live table
	// (throughput/ETA/per-table) for a running operation. No-op without a
	// journal file mirror.
	stopStats := m.startRunStatsWriter(ctx)
	if stopStats != nil {
		defer stopStats()
	}

	// Bounded worker pool (FR-03). WorkersPerPair is validated >= 1.
	workers := m.cfg.WorkersPerPair
	if workers < 1 {
		workers = 1
	}

	var (
		wg       sync.WaitGroup
		work     = make(chan *Unit, len(units))
		errMu    sync.Mutex
		firstErr error
		// stopConflict is set once a unit hits a strict conflict (FR-10 §8,
		// default): the run must STOP so the operator sees the collision.
		// In-flight units finish (their work is already underway and is safe),
		// but no new unit starts; the run returns the ConflictError.
		stopConflict atomic.Bool
		// abortMode (spec F-06, --on-unit-failure=abort): when enabled, the
		// FIRST unit failure stops the batch. The DEFAULT (continue) is the
		// opposite: the run keeps going so the operator gets the full failure
		// summary, then `gpstream resume` re-queues the failed units.
		abortMode = m.cfg.OnUnitFailure == "abort"
		// stopOnFailure is set once a unit fails while abortMode is on: no
		// new unit starts, in-flight units finish, and the run returns the
		// first failure.
		stopOnFailure atomic.Bool
	)
	dispatch := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range work {
				// A strict conflict stops the batch (FR-10 §8): drain the
				// channel without processing so the run can return the
				// conflict error instead of silently copying the rest.
				if stopConflict.Load() || stopOnFailure.Load() {
					continue
				}
				// A panic while processing a unit is a genuine fault (CR-7):
				// recover it so the worker does not die. The unit is recorded
				// as a failure (a resume can retry it, FR-06/FR-07) and the
				// worker continues with the next unit, so a dead worker's loss
				// of one unit does not kill the batch (FR-06).
				if err := m.processUnitSafe(ctx, u); err != nil {
					var ce *ConflictError
					if errors.As(err, &ce) {
						stopConflict.Store(true)
					} else if abortMode {
						// spec F-06 --on-unit-failure=abort: the first
						// genuine unit failure stops the batch (a strict
						// conflict is a stop of its own, FR-10 §8).
						stopOnFailure.Store(true)
					}
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}
	// FR-17: start the background index pool BEFORE the data path so each
	// unit's index build overlaps the remaining copies (size-priority
	// ordering keeps the longest build ahead of the tail). The pool shares
	// the destination pool, which is sized for workers + index workers +
	// journal (main.go: maxConns), so concurrent index DDL and COPYs are
	// safe.
	m.idxPool.Start(ctx)
	for i := 0; i < workers; i++ {
		dispatch()
	}
	for _, u := range units {
		work <- u
	}
	close(work)
	wg.Wait()

	// FR-17: drain the background index pool. First re-enqueue units that
	// reached the final name in an earlier (killed) run but never got their
	// index build — the journal is the source of truth, and createOneIndex is
	// idempotent per index name, so a re-enqueue is duplicate-safe.
	m.requeueIndexes(ctx)
	m.idxPool.Close()
	m.idxPool.Wait(ctx)
	if err := m.idxPool.Err(); err != nil && firstErr == nil {
		firstErr = err
	}
	// T5/FR-17: once the pool drained cleanly, mark the swapped units'
	// index build done in the journal, so a later T5 resume that KEEPS
	// them (DESIGN §2.2) does not re-enqueue (and REINDEX) their indexes.
	if m.idxPool.Err() == nil {
		for _, u := range m.journal.Units() {
			if u.State == StateSwapped && !u.IndexDone {
				u.IndexDone = true
				_ = m.journal.SetState(u)
			}
		}
	}

	// F-11 (metadata phase, "constraints"): restore the source table-level
	// constraints (foreign keys and user CHECK constraints) on the destination
	// tables. This runs AFTER the data path and AFTER the FR-17 index pool
	// drains: a foreign key must reference a destination table whose unique
	// index is in place (GP6 accepts an FK over a plain unique index — no PK
	// constraint required, verified live), and a restored constraint creates a
	// pg_depend entry that would otherwise block a later re-swap (DROP) of the
	// referenced table. Only StateSwapped units qualify: the in-place
	// --truncate/--append paths kept the existing destination table (same OID),
	// whose constraints are already present. Non-fatal by design (like the
	// index build): a fault is logged to events.jsonl (F-04) and the run
	// returns it as the first error only when no data-path error occurred; a
	// resume re-runs the pass and the idempotent constraintExists guard skips
	// what is already restored.
	if !m.cfg.NoConstraints {
		if err := m.restoreConstraints(ctx, units); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// The source agent owns the FR-02 snapshot; it is released when the run
	// ends (m.twoAgent.Release, deferred in Run). Nothing to release here.
	return firstErr
}

// restoreConstraints applies the source table-level constraints (FK + user
// CHECK) to the destination tables of the swapped units (F-11). The source
// definitions reference SOURCE table names; a foreign key's referenced table
// is rewritten to its destination name via the plan's rename map (a table
// outside the migration set keeps its source name — if it does not exist on
// the destination the ALTER fails, is logged as a warning event, and the pass
// continues with the remaining constraints, exactly like the collation
// warnings: the operator decides).
func (m *Manager) restoreConstraints(ctx context.Context, units []*Unit) error {
	// Rename map: source schema+table -> destination schema+table.
	rm := make(map[string]string, len(units))
	for _, u := range units {
		rm[u.Schema+"."+u.Table] = u.DstSchema + "." + u.DstName
	}
	// Read every swapped unit's constraints up front (one catalog pass per
	// unit), so the apply phase below can run in two ordered passes.
	type unitDefs struct {
		u    *Unit
		defs []pg.ConstraintDef
	}
	all := make([]unitDefs, 0, len(units))
	for _, u := range units {
		if u.State != StateSwapped {
			continue // in-place paths kept the existing table (constraints intact)
		}
		defs, err := m.srcConstraints(ctx, u.Schema, u.Table)
		if err != nil {
			// A catalog fault must not sink the run: the data is in place and
			// verified; log and continue (F-04), a resume retries.
			m.events.Log("constraint_restore_skipped",
				"table", u.Schema+"."+u.Table, "reason", err.Error())
			continue
		}
		if len(defs) == 0 {
			continue
		}
		all = append(all, unitDefs{u: u, defs: defs})
	}

	// apply rewrites each selected definition to the destination and issues
	// "ALTER TABLE ... ADD CONSTRAINT" per unit. It returns the first error
	// when --on-unit-failure=abort, otherwise it logs and continues.
	apply := func(pred func(pg.ConstraintDef) bool) error {
		for _, ud := range all {
			u := ud.u
			var sel []pg.ConstraintDef
			for _, d := range ud.defs {
				if !pred(d) {
					continue
				}
				if d.Kind == "p" && (u.Schema != u.DstSchema || u.Table != u.DstName) {
					// PK on a renamed table: rewrite the constraint name to the
					// destination table's naming (index names are unique per
					// schema, so the source name may be taken by another table
					// already on the destination). The PK index ADD CONSTRAINT
					// builds takes its name from the constraint.
					d.Name = dest.DestIndexName(d.Name, u.Table, u.DstName)
				}
				if d.Kind == "f" && d.RefSchema != "" && d.RefTable != "" {
					// Rewrite the referenced source table to its destination name
					// via the plan's rename map. The FK is captured structurally
					// (not as a pg_get_constraintdef string, whose
					// referenced-table qualification depends on the capture
					// session's search_path), so this is a deterministic field
					// rewrite; BuildDef re-renders it with schema-qualified names
					// on the destination.
					if dstRef, ok := rm[d.RefSchema+"."+d.RefTable]; ok {
						d.RefSchema, d.RefTable = splitDot(dstRef)
					} else {
						// Referenced table is outside the migration set: it keeps
						// its source name (if it does not exist on the destination
						// the ALTER below fails, is logged, and the pass continues
						// — like the collation warnings, the operator decides).
						m.events.Log("constraint_external_ref",
							"table", u.DstSchema+"."+u.DstName,
							"constraint", d.Name,
							"references", d.RefSchema+"."+d.RefTable)
					}
				}
				sel = append(sel, d)
			}
			if len(sel) == 0 {
				continue
			}
			if err := m.dstAddConstraints(ctx, u.DstSchema, u.DstName, sel); err != nil {
				// Non-fatal: the data is in place; log the cause (F-04) and keep
				// going with the other tables. A resume retries the whole pass;
				// the idempotency guard skips what already applied.
				m.events.Log("constraint_restore_failed",
					"table", u.DstSchema+"."+u.DstName, "error", err.Error())
				if m.cfg.OnUnitFailure == "abort" {
					return err
				}
			}
		}
		return nil
	}

	// Pass 1: PRIMARY KEY constraints. "ADD CONSTRAINT ... PRIMARY KEY" builds
	// the PK's unique index atomically; doing this BEFORE the FKs lets an FK
	// that references a migrated table's PK column find the unique index.
	if err := apply(func(d pg.ConstraintDef) bool { return d.Kind == "p" }); err != nil {
		return err
	}
	// Pass 2: foreign keys + user CHECK constraints.
	return apply(func(d pg.ConstraintDef) bool { return d.Kind != "p" })
}

// splitDot splits a "schema.table" pair (no embedded dots are possible here:
// the planner builds it from catalog identifiers).
func splitDot(s string) (string, string) {
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[:i], s[i+1:]
	}
	return "", s
}

// processUnitSafe wraps processUnit with a panic recovery so a genuine fault in
// one unit (CR-7) does not kill the worker (FR-06). On panic it records the
// unit as a failure (a resume can retry it, FR-06/FR-07) and returns an error
// so the caller sets firstErr; the worker itself continues with the next unit
// (the deferred recovery here keeps the worker goroutine alive).
func (m *Manager) processUnitSafe(ctx context.Context, u *Unit) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			// Record the failed unit so a resume can retry it (FR-06/FR-07),
			// then surface the panic as a failure for the run.
			_ = m.setUnitState(u, StateFailed)
			m.addFailure()
			// F-04: a panicking unit is a failure event like any other.
			// (The FR-02 snapshot is held by the source agent, which heals it.)
			m.events.Log("unit_failed", "schema", u.Schema, "table", u.Table,
				"attempts", u.Attempts, "error", fmt.Sprintf("panic: %v", r))
			retErr = fmt.Errorf("unit %s.%s panic: %v", u.Schema, u.Table, r)
		}
	}()
	return m.processUnit(ctx, u)
}

// finishCompleted completes a unit that a previous (crashed) run left in the
// "completed" state — verified data in the staging table, but the atomic
// rename to the final name never ran (the FR-02 crash window between the
// "completed" journal write and the swap). It finishes that swap so the
// destination actually receives the table, closing the resume gap where such
// a unit was silently skipped as terminal (FR-06/FR-07: "resume completes to
// the end"). It reuses the same atomic dst.Swap as runUnit step 9, with the
// same overwrite decision, so the semantics match the crashed run exactly.
//
// Three cases:
//  1. staging present -> perform the rename (the common crash window).
//  2. staging absent, final present -> the rename already landed but the
//     "swapped" journal write was lost; just record it (idempotent).
//  3. staging absent, final absent -> the verified staging data was removed
//     (e.g. an operator cleanup) and the rename never ran; the data is gone,
//     so mark the unit failed (a resume re-copies it) and report the anomaly
//     rather than claiming success.
func (m *Manager) finishCompleted(ctx context.Context, u *Unit) error {
	ds, dn := u.DstSchema, u.DstName
	if ds == "" {
		ds = u.Schema
	}
	if dn == "" {
		dn = u.Table
	}
	target := dest.StagingName(u.JobID, u.UnitID)

	stagingExists, err := m.dstExists(ctx, ds, target)
	if err != nil {
		return fmt.Errorf("resume: exists staging: %w", err)
	}
	if !stagingExists {
		// The staging table is gone: either the rename already landed (case 2)
		// or the verified data was dropped without a rename (case 3).
		finalExists, err := m.dstExists(ctx, ds, dn)
		if err != nil {
			return fmt.Errorf("resume: exists final: %w", err)
		}
		if finalExists {
			m.events.Log("resume_swap_recorded",
				"schema", ds, "table", dn,
				"reason", "staging absent, final present (rename landed; journal write lost)")
			if err := m.setUnitState(u, StateSwapped); err != nil {
				return fmt.Errorf("resume journal swapped: %w", err)
			}
			m.statMu.Lock()
			m.Units++
			m.statMu.Unlock()
			return nil
		}
		// Both gone: the verified data is lost. Mark failed so a resume
		// re-copies the table; report the anomaly (do not claim success).
		m.events.Log("resume_swap_data_lost",
			"schema", ds, "table", dn,
			"reason", "staging and final both absent; verified data lost — re-copy required")
		_ = m.setUnitState(u, StateFailed)
		return fmt.Errorf("resume: unit %s.%s was completed but its staging and final tables are both absent (verified data lost; re-copying)", ds, dn)
	}

	// Case 1: the staging table holds the verified data; complete the atomic
	// rename. Same overwrite decision as runUnit step 9.
	overwrite := m.cfg.ConflictAction == config.Overwrite || u.Conflict
	if err := m.dstSwap(ctx, ds, dn, target, overwrite); err != nil {
		// A swap fault on resume: the verified data is still in the staging
		// table (safe), but mark the unit failed so it is counted and
		// retried on the next resume (consistent with a mid-copy failure).
		_ = m.setUnitState(u, StateFailed)
		m.addFailure()
		m.events.Log("resume_swap_failed",
			"schema", ds, "table", dn, "error", err.Error())
		return fmt.Errorf("resume swap: %w", err)
	}
	// Post-swap: the same side effects as runUnit steps 9b/9c/10, so the
	// recovered table is as complete as a normally-swapped one (sequences,
	// segment balance, background index build).
	m.syncSequenceValues(ctx, u)
	m.measureBalance(ctx, u)
	m.enqueueIndexes(ctx, u)
	if err := m.setUnitState(u, StateSwapped); err != nil {
		return fmt.Errorf("resume journal swapped: %w", err)
	}
	m.statMu.Lock()
	m.Units++ // a completed unit; no bytes were transferred THIS run (rows were copied by the crashed run)
	m.statMu.Unlock()
	m.events.Log("resume_swap_completed",
		"schema", ds, "table", dn, "rows", u.RowCount)
	return nil
}

// processUnit runs a single unit through the pipeline (FR-01/FR-02/FR-17),
// honouring the conflict policy (FR-10/FR-11). It is safe to call from
// multiple workers (FR-03): stats are guarded by statMu and the journal by its
// own mutex.
func (m *Manager) processUnit(ctx context.Context, u *Unit) error {
	// T5 (DESIGN §2.2): a unit verified in a previous run (state completed
	// or swapped, recorded in the journal mirror) is not re-copied — its
	// data is verified under one snapshot (the keep rule). The exception is
	// a resume that must re-read the unit's table (mixed epochs, or strict
	// mode): the unit is reset to pending and re-copied under the fresh
	// snapshot; runUnit starts from a clean staging table (a56be8b), so the
	// re-read replaces rather than appends. A re-read unit is marked
	// conflicting: the destination holds the PREVIOUS result and the swap
	// must overwrite it (step 9), which is why it bypasses the stop/skip
	// gate below.
	reReading := false
	if u.State == StateCompleted || u.State == StateSwapped {
		if !m.reRead[tableKey(u)] {
			// A completed (verified) unit that never reached swapped is the
			// crash-window: the previous run recorded "completed" (data
			// verified in the staging table) but died before the atomic
			// rename to the final name. Do NOT skip it — complete the swap so
			// the destination actually gets the table (the FR-06/FR-07
			// "resume completes to the end" guarantee). A swapped unit is
			// already terminal and is kept.
			if u.State == StateCompleted {
				return m.finishCompleted(ctx, u)
			}
			m.addSkipped()
			// The unit is terminal (a previous run verified it); keep the
			// journal state as-is (completed/swapped).
			return nil
		}
		u.State = StatePending
		u.Checksum = ""
		u.RowCount = 0
		u.Conflict = true
		_ = m.setUnitState(u, StatePending)
		// F-04: auto-recovery event — the resume re-reads this table under
		// the fresh snapshot (T5, DESIGN §2.2).
		m.events.Log("unit_reread", "schema", u.Schema, "table", u.Table,
			"reason", "resume re-read", "epoch", m.epoch)
		// FR-04 "auto-recovery events".
		m.monitor.RecordAutoRecovery()
		reReading = true
	}
	// Conflict policy (FR-10/FR-11, spec F-10 §8). Re-read units are exempt:
	// their conflict IS the previous result being replaced. A conflict is
	// present when the planner flagged it (u.Conflict) or a live existence
	// check confirms the destination already holds the target.
	//
	//   stop (default): --resolve-conflicts stop. A conflict STOPS the run:
	//     the unit is marked failed and the run returns an error, so the
	//     operator sees the collision and decides (re-run with another
	//     policy or a rename). Nothing is clobbered.
	//   skip: --resolve-conflicts skip. The conflicting unit is skipped; the
	//     run continues.
	//   proceed: --overwrite / --truncate explicitly opt in to rewriting the
	//     destination (overwrite via staging+swap, truncate in-place); both
	//     always rewrite — there is no pre-copy identical-data shortcut. The
	//     only content check is the post-copy verify (--verify on).
	if !reReading && u.Table != "" {
		conflicted := u.Conflict
		if !conflicted {
			exists, err := m.dstExists(ctx, u.DstSchema, u.DstName)
			if err != nil {
				return fmt.Errorf("conflict check: exists: %w", err)
			}
			conflicted = exists
		}
		// F-11 object classes: an existing destination table is EXPECTED for
		// both single-class modes, so it is not a conflict for them. The
		// planner's u.Conflict and the live m.dst.Exists signal are the SAME
		// check (does the destination table exist), so both are suppressed.
		// --metadata-only is idempotent by design (spec F-11 acceptance:
		// "a repeated run is idempotent"); --data-only loads into the existing
		// structure, which it does not create. Both classes (the default)
		// keep the strict FR-10 policy.
		if md, data := m.cfg.IncludeClasses(); md != data {
			conflicted = false
		}
		switch {
		case conflicted && m.cfg.ConflictAction == config.Skip:
			m.addSkipped()
			// Terminal state: the unit was deliberately not copied, so
			// `status` counts it as done and `resume` does not re-queue it.
			m.journal.CreateUnit(u, StateSkipped)
			// F-04: conflict event (skipped) — the operator audit trail.
			m.events.Log("conflict", "schema", u.DstSchema, "table", u.DstName,
				"action", "skip")
			return nil
		case conflicted && m.cfg.ConflictAction == config.Stop:
			// stop is the default policy (FR-10 §8): surface the conflict
			// and stop; the operator decides (re-run with another policy or
			// a rename). Nothing is clobbered.
			_ = m.setUnitState(u, StateFailed)
			m.addFailure()
			// F-04: conflict event (stop) — the run halts on this collision.
			m.events.Log("conflict", "schema", u.DstSchema, "table", u.DstName,
				"action", "stop")
			return &ConflictError{Schema: u.DstSchema, Table: u.DstName}
		}
		// Overwrite/Truncate: the conflict is expected — the destination is
		// rewritten below (overwrite via staging+swap, truncate in-place).
	}
	if err := m.runUnit(ctx, u); err != nil {
		m.addFailure()
		m.journal.SetState(u)
		// F-04: retry/failure event — a resume (or an explicit re-run)
		// retries the unit. (The FR-02 snapshot is held by the source agent,
		// which heals it after a failed unit.)
		// retries the unit; the operator sees the failure and its cause.
		// on_unit_failure records the mode the run is applying (spec F-06:
		// continue = default, keep going; abort = stop at first failure).
		m.events.Log("unit_failed", "schema", u.Schema, "table", u.Table,
			"attempts", u.Attempts, "on_unit_failure", m.cfg.OnUnitFailure,
			"error", err.Error())
		return err
	}
	return nil
}

// addSkipped increments the skipped-units counter (FR-10/FR-11) under statMu.
// processUnit may run on multiple workers (FR-03), so the counter is guarded.
func (m *Manager) addSkipped() {
	m.statMu.Lock()
	m.Skipped++
	m.statMu.Unlock()
}

// addFailure increments the failed-units counter under statMu (see addSkipped).
func (m *Manager) addFailure() {
	m.statMu.Lock()
	m.Failures++
	m.statMu.Unlock()
}

// ConflictError is returned (FR-10 §8, strict default) when a unit collides
// with an object that already exists on the destination and no conflict
// policy was given. The run STOPS with this error so the operator sees the
// collision and decides (re-run with --resolve-conflicts skip|overwrite,
// --truncate, --append, or a rename). Nothing is clobbered.
type ConflictError struct {
	Schema string
	Table  string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict: %s.%s already exists on the destination (re-run with --resolve-conflicts skip|overwrite|truncate to proceed)",
		e.Schema, e.Table)
}

// unitTableKey is the per-table metrics key (FR-04 "per table"): the
// destination-qualified name the operator sees. A DB prefix is included only
// when the unit carries one (single-DB runs key by schema.name).
func unitTableKey(u *Unit) string {
	if u.DB != "" {
		return u.DB + "." + u.DstSchema + "." + u.DstName
	}
	return u.DstSchema + "." + u.DstName
}

// hostPairLabel renders the FR-04 host-pair metric label ("src -> dst") from
// the two connection DSNs (host:port only, no credentials). Empty when either
// DSN cannot be parsed.
func hostPairLabel(srcDSN, dstDSN string) string {
	s, d := pg.DSNHostPort(srcDSN), pg.DSNHostPort(dstDSN)
	if s == "" || d == "" {
		return ""
	}
	return s + " -> " + d
}

// dsnHostPort extracts "host:port" from a Postgres DSN (URL or key/value form),
// without exposing credentials.

// setUnitState transitions a unit to a new state: it updates the in-memory
// unit, journals it (the durable record), and moves the FR-04 per-status /
// active-flow gauges. It returns the journal error (callers that already
// ignored it keep doing so). The metrics move is always applied so the gauge
// tracks the unit even when the journal write is a best-effort no-op.
func (m *Manager) setUnitState(u *Unit, s State) error {
	u.State = s
	err := m.journal.SetState(u)
	m.monitor.SetUnitState(unitTableKey(u), string(s))
	return err
}

// tableFor maps a run unit back to the plan.Table the source COPY reads
// from (FR-03). A single Manager run processes exactly one plan, and the unit
// carries its source schema/name (u.Schema/u.Table) and destination
// schema/name (u.DstSchema/u.DstName). The source COPY reads the SOURCE
// names, so Schema/Name are the source names; DstSchema/DstName carry the
// destination names (after rename) so the copy is fully-qualified on both
// sides and a rename is honoured end-to-end (FR-11/§4.10).
func (m *Manager) tableFor(u *Unit) plan.Table {
	return plan.Table{
		DB:        u.DB,
		Schema:    u.Schema,
		Name:      u.Table,
		DstDB:     u.DB,
		DstSchema: u.DstSchema,
		DstName:   u.DstName,
		Bytes:     u.Bytes,
	}
}

// resolve returns how a conflicting unit must be handled (FR-11). It reads the
// operator's --resolve-conflicts value and decides the copy target and
// verification:
//
//	stop         -> the caller (processUnit) stops the run (never reaches here)
//	skip         -> the caller (processUnit) skips the unit (never reaches here)
//	overwrite    -> copy to a staging table and swap over the final (default)
//	truncate     -> copy in-place into the final table after TRUNCATE, verify
//
// inPlace  : copy into the final table directly (only truncate)
// truncate : clear the final table before copying (only --truncate)
// verify   : compare the content checksum after copying (always)
func (m *Manager) resolve(u *Unit) (inPlace, truncate, verify bool) {
	// Content verification runs after the copy, only when the operator asks
	// for it (--verify on). The default (off) makes a run one source read per
	// unit: the data COPY itself.
	verify = m.cfg.Verify == config.VerifyOn
	switch m.cfg.ConflictAction {
	case config.Truncate:
		return true, true, verify
	default:
		// Stop/Skip are handled in processUnit; Overwrite and any unlisted
		// action fall back to the staging+swap path.
		return false, false, verify
	}
}

// runUnit copies, verifies, rebuilds indexes and swaps a single unit
// (FR-01/FR-02/FR-11/FR-17), recording each transition in the journal. It takes
// the pre-built unit (see Run) so the pool can share unit construction. The
// conflict policy (FR-11) selects the copy target: the default and --overwrite
// copy into a staging table and atomically swap it over the final name;
// --truncate/--append copy in-place into the final table (FR-11).
func (m *Manager) runUnit(ctx context.Context, u *Unit) error {
	// Source and destination names may differ (FR-11/§4.10 rename): the copy
	// READS from the source (u.Schema/u.Table) and WRITES to the destination
	// (u.DstSchema/u.DstName). Without a rename they are equal.
	ds, dn := u.DstSchema, u.DstName

	// 1. Record the unit as pending (FR-02 journal).
	if _, err := m.journal.CreateUnit(u, StatePending); err != nil {
		return fmt.Errorf("journal pending: %w", err)
	}
	// FR-04: move the per-status gauge (a fresh unit was "" at seed time; a
	// re-read unit was already pending, which is a no-op move).
	m.monitor.SetUnitState(unitTableKey(u), string(StatePending))

	// 2. Read the schema off the SOURCE, then create the DESTINATION schema
	//    (and the copy target) from it (FR-02). The target is built from the
	//    source's own column definitions (format_type) so it is faithful
	//    across clusters; the distribution policy is reproduced on a
	//    Greenplum source (F-11 metadata: "types, distribution").
	t := m.tableFor(u)
	schema, name := u.Schema, u.Table
	// The column list is stable catalog data (not snapshot state), so it is
	// read on the SOURCE AGENT's pool (2B-5: the controller holds no direct
	// source connection on the two-agent topology; single-process: its own
	// srcQ), NOT the pinned FR-02 snapshot connection: a query fault there
	// (e.g. the table was dropped, or the source is unreachable) must not
	// abort the REPEATABLE READ snapshot that every data COPY shares. On a
	// pool source the pool hands out a connection per query, so a bad
	// statement is contained. (M2 20+ table scale test, 28.09: reading
	// Columns on the pinned conn aborted the whole snapshot transaction on
	// one failed unit, cascading 25P02 to every sibling unit. Same failure
	// class the Distribution read below already avoids.)
	cols, err := m.srcColumns(ctx, schema, name)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	if len(cols) == 0 {
		// A missing source table yields ZERO columns (the pg_class subquery
		// returns NULL), not an error. Detect it here, BEFORE the data COPY:
		// a `COPY ... TO STDOUT` for a vanished table would fail on the
		// pinned FR-02 snapshot connection, and any SQL error aborts the
		// whole REPEATABLE READ transaction — cascading 25P02 to every
		// remaining unit (F-06: one failed unit must not take down the
		// operation). The existence check runs on a pool connection (the
		// source agent's, two-agent), so the error is contained and the
		// snapshot survives.
		if !m.srcTableExists(ctx, schema, name) {
			m.setUnitState(u, StateFailed) // mark failed so a resume retries it (FR-06/FR-07)
			return fmt.Errorf("source table %s.%s does not exist (dropped during the run?)", schema, name)
		}
	}
	// The distribution policy is stable catalog data (not snapshot state),
	// so it is read on the source agent's pool (2B-5), NOT the pinned FR-02
	// snapshot connection: a query fault there must not abort the
	// REPEATABLE READ snapshot that every data COPY shares. (GP6 live e2e,
	// 27.09: the first version read it on the pinned conn and the resulting
	// error aborted the whole snapshot transaction.)
	dist := m.srcDistribution(ctx, schema, name)

	// 2b. Fix-1 partition guard: a string-keyed partitioned parent WITHOUT a
	//     DEFAULT partition, on a collation-different destination, would have
	//     its whole COPY abort mid-stream ("no partition for partitioning
	//     key") once a row matches no leaf bound. Fail the unit EARLY — before
	//     any DDL or data is moved — with the planner's diagnostic, so the
	//     operator can add a DEFAULT partition (or align collations) and retry.
	//     A re-routing to a different leaf (with a default, or a matching
	//     bound) is the intended Fix-1 behavior and is NOT guarded.
	if u.PartitionGuard != "" {
		m.setUnitState(u, StateFailed)
		m.events.Log("partition_guard", "table", schema+"."+name, "reason", u.PartitionGuard)
		return fmt.Errorf("partition guard: %s", u.PartitionGuard)
	}

	// 2B-2 (architecture 08.10): in the two-agent topology the DESTINATION
	// AGENT owns the schema DDL — its ServeUnitData runs EnsureSchema before
	// the staging DDL, and the metadata-only create-table does the same on
	// the agent. The controller must not create the destination schema
	// itself (no direct destination connection, and a low-privilege dest
	// agent must be the one to fail its own DDL, T7).
	if m.twoAgent == nil {
		if err := m.dst.EnsureSchema(ctx, ds); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}

	// 3a. Recreate the source sequences BEFORE the table DDL (F-11 metadata
	//     phase). A serial column's default (nextval('<schema>.<seq>'::regclass))
	//     is copied verbatim into the destination DDL, so the sequence must
	//     exist when the table is created.
	m.createSequences(ctx, u)

	// F-11 --metadata-only: the unit is the STRUCTURE
	// phase — the table DDL (above) and the source sequences (3a) are all it
	// does; no data is copied, verified, or swapped. Completing at
	// StateCompleted (not StateSwapped) is deliberate: restoreConstraints /
	// requeueIndexes skip non-swapped units, and a metadata-only table has no
	// indexes to build. A repeated run is idempotent (spec F-11 acceptance):
	// the conflict gate is lifted for structure runs (above) and the DDL is
	// CREATE ... IF NOT EXISTS (dest.CreateTable), so re-creating an existing
	// structure is a no-op.
	if md, data := m.cfg.IncludeClasses(); md && !data {
		// Create the structure from the source layout (idempotent: the DDL is
		// CREATE ... IF NOT EXISTS, so a re-run over an existing structure is
		// a no-op — spec F-11 acceptance "a repeated run is idempotent").
		// The sequences were already created in step 3a, above, which is the
		// required order (a serial default references the sequence).
		exists, err := m.dstExists(ctx, ds, dn)
		if err != nil {
			return fmt.Errorf("metadata-only: exists: %w", err)
		}
		if !exists {
			if err := m.dstCreateTable(ctx, ds, cols, dn, dist, u.Partition); err != nil {
				return fmt.Errorf("create table (metadata-only): %w", err)
			}
		}
		m.events.Log("metadata_only", "schema", ds, "table", dn)
		if err := m.setUnitState(u, StateCompleted); err != nil {
			return fmt.Errorf("journal completed (metadata-only): %w", err)
		}
		m.statMu.Lock()
		m.Units++
		m.statMu.Unlock()
		return nil
	}

	// 3. Resolve the conflict policy (FR-11): the copy target is either a
	//    staging table (default / --overwrite) or the final table in-place
	//    (--truncate / --append). All destinations below use ds/dn.
	inPlace, truncate, verify := m.resolve(u)
	// F-11 --data-only: the destination structure ALREADY EXISTS and the data
	// is loaded INTO it, replacing its rows (the unit's job is to make the
	// destination's data equal the source's — an append would double rows on
	// a resume/re-run). So regardless of the --resolve-conflicts policy the
	// copy is in-place into the final table with a TRUNCATE first, then the
	// checksum is verified against the source. The structure is NOT created:
	// a missing destination table is a hard error (run --metadata-only or a
	// full copy first), and no staging+swap is used.
	if md, data := m.cfg.IncludeClasses(); !md && data {
		exists, err := m.dstExists(ctx, ds, dn)
		if err != nil {
			m.setUnitState(u, StateFailed)
			return fmt.Errorf("--data-only: exists check: %w", err)
		}
		if !exists {
			m.setUnitState(u, StateFailed)
			return fmt.Errorf("--data-only: destination table %s.%s does not exist (create the structure first with --metadata-only or a full copy)", ds, dn)
		}
		inPlace, truncate = true, true
	}
	// Two-agent (P3) data path is staging+swap only: the destination agent
	// creates a staging table and (after the controller's verify) swaps it.
	// The in-place targets (--truncate, --data-only) have no two-agent
	// equivalent yet — fail the unit with a clear config error instead of
	// silently degrading the topology.
	if m.twoAgent != nil && inPlace {
		m.setUnitState(u, StateFailed)
		return fmt.Errorf("two-agent topology does not support the in-place copy target (conflict action %q / --data-only); use the default staging+swap path", m.cfg.ConflictAction)
	}
	var target string
	switch {
	case m.twoAgent != nil && !inPlace:
		// Two-agent (ADR-002): the destination AGENT owns the staging DDL
		// (it creates it from the UnitSpec the source agent sends — cols /
		// dist / part captured on the source side). The controller must not
		// create it here: the staging name is the contract between the two
		// agents.
		target = dest.StagingName(u.JobID, u.UnitID)
	case inPlace:
		// Copy into the final table directly. Create it from the source layout
		// if it does not exist yet, then (optionally) truncate it.
		target = dn
		if !m.dst.Exists(ctx, ds, dn) {
			if err := m.dst.CreateTable(ctx, ds, cols, dn, dist, u.Partition); err != nil {
				return fmt.Errorf("create table: %w", err)
			}
		}
		if truncate {
			if err := m.dst.Truncate(ctx, ds, dn); err != nil {
				return fmt.Errorf("truncate: %w", err)
			}
		}
	default:
		target = dest.StagingName(u.JobID, u.UnitID)
		// The staging table is scratch for this unit's in-flight copy. A
		// surviving one (a crashed/failed run that copied but never swapped)
		// holds unswapped partial data; it MUST be empty before the copy,
		// otherwise a resume appends to the partial data and doubles the rows
		// (F-06/F-07: "resume completes to the end without duplicates"). A fresh run has
		// no staging table, so it is created from the source layout.
		if m.dst.Exists(ctx, ds, target) {
			if err := m.dst.Truncate(ctx, ds, target); err != nil {
				return fmt.Errorf("truncate staging: %w", err)
			}
		} else if err := m.dst.CreateStaging(ctx, ds, cols, target, dist, u.Partition); err != nil {
			return fmt.Errorf("create staging: %w", err)
		}
	}

	// 4. Mark copying (FR-06/FR-07 journal).
	if err := m.setUnitState(u, StateCopying); err != nil {
		return fmt.Errorf("journal copying: %w", err)
	}

	// 5. Stream the data: source --(framed, compressed)--> target (FR-01/§4.2).
	//    the on_segment sub-paths: rows land on the destination segments
	//    directly (no coordinator write); a table that cannot be routed
	//    degrades to the coordinator path below (correct, just not direct).
	//    fp is the framed pipe used by the coordinator path; it is nil for a
	//    pure-direct unit (direct builds its own per-segment pipes).
	//    P7: the data COPY runs only in the source/dest agents (two-agent);
	//    the FR-02 run-wide DB snapshot is held by the long-lived source
	//    agent, not the controller.
	var (
		rows          int64
		wire, logical uint64
		// P9-E (FR-02): the row-order-independent content digests computed ON
		// THE DATA PATH (the coordinator two-agent path): srcDigest from the
		// source agent, dstDigest from the destination agent. The verify step
		// compares them instead of re-reading the tables.
		srcDigest, dstDigest string
		// MESH (architecture 08.10): the destination agent owns the verify +
		// swap decision. streamTwoAgentMini returns its verdict —
		// meshSwapped = the agent performed the atomic swap itself;
		// meshVerified = the SOURCE vs DEST digests matched. When set, runUnit
		// skips its own verify/swap for the unit (the destination already did
		// it; re-doing it would double-swap / re-verify).
		// meshSwapped = the destination agent performed the atomic swap itself
		// (a true MESH mini unit that succeeded); meshVerified = the SOURCE vs
		// DEST digests matched. A coordinator-downgraded mesh unit returns
		// both false (the controller then verifies + swaps it as usual).
		meshSwapped, meshVerified bool
		meshEvents                []string
		// 2B-1: the COORDINATOR two-agent path — the destination agent owns the
		// verify + swap decision too (the source digest rides the unit's
		// Trailer, the destination compares it against its own on-the-data-path
		// digest and performs the atomic swap). The verdicts come back from
		// streamTwoAgent; when set, runUnit skips its own verify + swap,
		// exactly like the mesh path.
		coordSwapped, coordVerified bool
	)
	// F-12: a small table (below --small-table-threshold) rides the transport
	// with compression OFF; record the decision in events.jsonl (F-04) so the
	// operator can see which units skipped the compressor and why.
	if m.compressionOffFor(t) {
		m.events.Log("compression_off",
			"table", t.Schema+"."+t.Name,
			"bytes", t.Bytes,
			"threshold", m.cfg.SmallTableThreshold,
			"reason", "small-table auto-disable (F-12)")
	}
	switch {
	case m.twoAgent != nil && m.cfg.CopyMode == config.CopyMesh && t.SrcReplicated:
		// Design 04.10 (question 5): a DISTRIBUTED REPLICATED source table
		// holds a FULL copy on every source segment, so an ON SEGMENT read
		// would stream S identical copies (S× rows on the destination). The
		// unit takes the coordinator data path instead (one COPY through the
		// source coordinator, write through the destination coordinator) —
		// the only correct read for a replicated table.
		m.events.Log("replicated_fallback",
			"table", t.Schema+"."+t.Name,
			"reason", "DISTRIBUTED REPLICATED source: ON SEGMENT read would stream S identical full copies")
		rows, wire, logical, srcDigest, dstDigest, coordSwapped, coordVerified, err = m.streamTwoAgent(ctx, t, u, target, ds, dn, verify)
	case m.twoAgent != nil && m.coordinatorFallbackFor(t):
		// P1b: a SMALL table (below --small-table-threshold) takes the
		// coordinator data path even under --copy-mode on_segment: the
		// segment-direct pipeline (S segreceivers + S destination
		// connections + a per-slice compressor) does not pay back on a
		// payload that does not even justify compression (F-12, the same
		// metric and threshold). The coordinator path also carries the
		// inline content digest (P9-E), so the small table verifies
		// without a re-read for free.
		m.events.Log("onsegment_coordinator_fallback",
			"table", t.Schema+"."+t.Name,
			"rows", t.RowEst,
			"bytes", t.Bytes,
			"threshold", m.cfg.SmallTableThreshold,
			"reason", "small table: the segment-direct pipeline does not pay back (F-12 threshold)")
		rows, wire, logical, srcDigest, dstDigest, coordSwapped, coordVerified, err = m.streamTwoAgent(ctx, t, u, target, ds, dn, verify)
	case m.twoAgent != nil && m.cfg.CopyMode == config.CopyMesh:
		// P3 production-dispatch (mesh, P4 design): the segment-direct data
		// path moves into the agents — the source agent triggers the ON
		// SEGMENT dispatch with the D-entry address book (the
		// destination-segment mini-agent data endpoints); each source
		// segreceiver streams its slice to its destination segment's
		// mini-agent, which performs a LOCAL utility COPY on its segment.
		// The destination agent runs in SUPERVISOR mode: it builds the
		// distributed staging table, broadcasts the unit to the D minis, and
		// combines their reports (XOR_D). NEITHER cluster coordinator is in
		// the data path (design 04.10). DEGRADATION IS PER TABLE (the P4
		// design): a table that cannot be placed segment-to-segment (merge /
		// partitioned / a non-Greenplum side) takes the coordinator path
		// (one COPY through the destination coordinator, executed by the
		// supervisor's fallback path) — the rest of the job keeps the mini
		// path.
		rows, wire, logical, srcDigest, dstDigest, meshSwapped, meshVerified, meshEvents, err = m.streamTwoAgentMini(ctx, t, u, target, ds, dn, cols, dist, u.Partition, verify)
		// 1E: the destination agent's diagnostics for the unit (swap_done /
		// verify_mismatch / mini errors / journal errors) go to the
		// controller's events.jsonl (source=dest-agent) — the architecture
		// 08.10 rule: the controller's diagnostics come ONLY from the two
		// agents.
		for _, ev := range meshEvents {
			m.events.Log("dest_agent",
				"table", t.Schema+"."+t.Name,
				"event", ev)
		}
	case m.twoAgent != nil:
		// Two-agent (ADR-002, P3): the data COPY runs ONLY in the agents over
		// a real, compressed socket (source agent -> destination agent). The
		// controller holds no data connection for the unit: it passes the
		// instruction, reads back the destination's count + the wire volume,
		// and keeps the DDL/journal/verify/swap decision. The FR-02 snapshot
		// is held by the long-lived source agent, not the controller.
		rows, wire, logical, srcDigest, dstDigest, coordSwapped, coordVerified, err = m.streamTwoAgent(ctx, t, u, target, ds, dn, verify)
	}
	// A MESH unit the destination handled (meshSwapped = the agent performed
	// the swap; meshVerified = the SOURCE vs DEST digests matched) marks the
	// unit as mesh: the destination agent is the writer of record for its
	// gpstream_units row (architecture 08.10), so the controller's own DB
	// journal writes for the terminal states are skipped (the controller
	// records only the durable file mirror). A coordinator-downgraded mesh
	// unit (both false) is journaled by the controller as usual.
	if meshSwapped || meshVerified {
		u.Mesh = true
	}
	if err != nil {
		// Roll the copy target back on a staging failure (FR-07); an in-place
		// failure is recorded as failed and re-tried by a resume (TRUNCATE is
		// idempotent, so a retry re-copies from a clean table).
		if !inPlace {
			_ = m.dstDropStaging(ctx, ds, target)
		}
		u.State = StateFailed
		_ = m.journal.SetState(u)
		m.monitor.SetUnitState(unitTableKey(u), string(StateFailed))
		return fmt.Errorf("stream: %w", err)
	}
	u.RowCount = rows
	if (meshSwapped || coordSwapped) && dstDigest != "" {
		// A unit the destination agent swapped itself (MESH mini, 1C; or
		// COORDINATOR two-agent, 2B-1): its content checksum is the
		// destination's on-the-data-path digest (it matched the source's when
		// --verify is on; with --verify off it is the recorded destination
		// digest). The journal records it like the coordinator path does after
		// its verify. The controller-verified path stamps its own checksum in
		// the verify step, so only a destination-swapped unit needs it here.
		u.Checksum = dstDigest
	}
	m.statMu.Lock()
	m.Rows += rows
	m.statMu.Unlock()
	// FR-04: rows and REAL byte volume (wire/logical from the copy path —
	// not row counts, which the old code mis-recorded as bytes).
	tkey := unitTableKey(u)
	m.monitor.RecordRows(tkey, rows)
	m.monitor.RecordBytes(tkey, wire, logical)
	// 6. Verify the content checksum (FR-02, --verify on). Two paths:
	//    (a) P9-E inline: when the data path computed the row-order-
	//        independent content digest on BOTH sides (coordinator two-agent
	//        with --verify on), compare them directly — the digest was
	//        derived as the rows streamed (O(1) memory), so verification
	//        re-reads NEITHER table. On the M7 pilot (100 TB -> 1 PB) this
	//        removes a full re-scan of the source and of the destination.
	//    (b) fallback: re-derive the checksum on both sides (the segment /
	//        routed copy modes keep this path, as does a run where the
	//        agents did not report inline digests). The source checksum is
	//        read on the source agent so it sees the SAME FR-02 snapshot as
	//        the data COPY.
	//    Off by default — the COPY protocol's row count is then the
	//    landed-data check.
	if verify && !meshVerified && !coordVerified {
		// A unit the destination already verified (meshVerified / coordVerified)
		// skips the controller's verify: the destination compared the SOURCE vs
		// DEST digests itself and gated the swap on the match (architecture
		// 08.10; the coordinator path since 2B-1).
		_ = m.setUnitState(u, StateVerifying)
		// The two inline digests (the source's + the destination's, both
		// computed ON THE DATA PATH) are always present now: the content
		// digest is unconditional (no --verify gate). An empty pair is an
		// internal invariant violation (a data path that stopped carrying the
		// digest), NOT a case to re-read the tables for — the re-read verify
		// was removed (08.10): it meant a full re-scan of both tables, which
		// on the M7 pilot (100 TB -> 1 PB) is unacceptable.
		if srcDigest == "" || dstDigest == "" {
			if !inPlace {
				_ = m.dstDropStaging(ctx, ds, target)
			}
			_ = m.setUnitState(u, StateFailed)
			return fmt.Errorf("verify: inline digests missing (src=%q dst=%q) — the data path stopped carrying the content digest", srcDigest, dstDigest)
		}
		srcSum, dstSum := srcDigest, dstDigest
		if srcSum != dstSum {
			if !inPlace {
				_ = m.dstDropStaging(ctx, ds, target)
			}
			_ = m.setUnitState(u, StateFailed)
			// FR-04 "checksum errors": a content mismatch in the verify phase.
			m.monitor.RecordChecksumError()
			return fmt.Errorf("verify: checksum mismatch (src=%s dst=%s)", srcSum, dstSum)
		}
		u.Checksum = srcSum
		m.statMu.Lock()
		m.Comps++
		m.statMu.Unlock()
	}
	// 2B-1: a unit the DESTINATION verified (meshVerified / coordVerified)
	// also passed the --verify gate (the agent compared the digests) — count
	// it in Comps like the controller-verified path does, so the metric means
	// "units whose content was verified" regardless of which side did it.
	if verify && (meshVerified || coordVerified) {
		m.statMu.Lock()
		m.Comps++
		m.statMu.Unlock()
	}

	// 7. (FR-17) Indexes are NOT rebuilt here: the staging table is created
	//    without indexes, so a pre-swap REINDEX was a no-op. The index build is
	//    enqueued to the background pool after the swap (step 10), where it
	//    runs over the final table without blocking the data path.

	// 8. Record completed (data + checksum, ADR-001 §4.4). The unit is
	//    stamped with the run's snapshot epoch (T5, DESIGN §2.2): a later
	//    resume reads the epoch to decide which tables' verified data came
	//    from one snapshot (keep) and which span several (re-read).
	u.Epoch = m.epoch
	if err := m.setUnitState(u, StateCompleted); err != nil {
		return fmt.Errorf("journal completed: %w", err)
	}
	m.statMu.Lock()
	m.Units++
	m.statMu.Unlock()

	// 9. Atomically swap the staging table to its final name (FR-02). Only the
	//    staging path swaps; the in-place path already wrote the final table.
	if !inPlace {
		if !meshSwapped && !coordSwapped {
			// A re-read unit (u.Conflict, T5) replaces the previous result: the
			// final name already exists on the destination and the verified
			// staging must be swapped over it. (A unit the destination already
			// swapped — meshSwapped / coordSwapped — skips this: the
			// destination agent performed the atomic rename itself,
			// architecture 08.10; the coordinator path since 2B-1.)
			overwrite := m.cfg.ConflictAction == config.Overwrite || u.Conflict
			if err := m.dstSwap(ctx, ds, dn, target, overwrite); err != nil {
				// A swap failure after a verified copy is still safe (the staging
				// table holds the verified data); leave it and report so a resume
				// can retry the swap.
				return fmt.Errorf("swap: %w", err)
			}
		}
	}
	if err := m.setUnitState(u, StateSwapped); err != nil {
		return fmt.Errorf("journal swapped: %w", err)
	}

	// 9b. Resynchronise sequence VALUES (F-11: "values are synchronised
	//     post-load"). The final table now holds the source rows, so the
	//     destination sequences catch up to the source's last_value: the
	//     next nextval on the destination returns last_value+1, exactly
	//     like on the source. (The sequences themselves were created in
	//     step 3a, before the table DDL.)
	m.syncSequenceValues(ctx, u)

	// 9c. Segment balance (F-15): measure the destination's per-segment
	//     fill of the just-swapped table (metric max/avg over
	//     gp_segment_id) and surface a warning when the fill exceeds the
	//     fixed 1.5 gate. Non-fatal by design (like 9b/10):
	//     the data is already in place, the metric is diagnostic, and a
	//     non-Greenplum destination has no segments to measure.
	m.measureBalance(ctx, u)

	// 10. Enqueue the index build to the background pool (FR-17). Non-fatal by
	//     design: the data is already in place, and a failed/missing read is
	//     covered by the journal scan in Run on the next resume.
	m.enqueueIndexes(ctx, u)
	return nil
}

// streamTwoAgent runs the unit's data COPY through the two-agent data path
// (ADR-002 remediation, P3): the controller hands the instruction to the
// twoAgentRunner, which drives the long-lived source agent (owns the FR-02
// snapshot + `COPY ... TO STDOUT`) and a per-unit destination agent (owns
// `COPY ... FROM STDIN` + the staging DDL). The framed, compressed stream goes
// source agent -> destination agent over a real socket; the controller only
// reads back the destination's row count and the wire volume. The destination
// staging table is created + loaded by the destination agent. 2B-1: the verify
// + swap decision is the destination's (the source digest rides the unit's
// Trailer) — the returned swapped/verified are the destination's verdict.
func (m *Manager) streamTwoAgent(ctx context.Context, t plan.Table, u *Unit, target, ds, dn string, verifyDigest bool) (int64, uint64, uint64, string, string, bool, bool, error) {
	uc, _ := m.unitCompressors(t)
	jobID := jobIDFromHash(u.JobID)
	// 2B-1: the destination performs the atomic swap itself, so it needs the
	// controller's overwrite decision (a re-read / --overwrite unit replaces
	// the existing final table; the default preserves it). The same expression
	// runUnit used for its own swap (step 9).
	overwrite := m.cfg.ConflictAction == config.Overwrite || u.Conflict
	return m.twoAgent.Stream(ctx, unitStreamRequest{
		StreamReq: agent.StreamReq{
			Op:        "stream",
			Schema:    t.Schema,
			Table:     t.Name,
			JobID:     jobID,
			UnitID:    uint32(u.UnitID),
			Compress:  m.twoAgentCompressString(uc),
			DstSchema: ds,
			DstTable:  dn,
			Staging:   target,
			Overwrite: overwrite,
			// P9-E (FR-02): with --verify on, compute the row-order-independent
			// content digest ON THE DATA PATH on both sides (O(1) memory), so
			// the verify step compares the two inline digests instead of
			// re-reading the source and destination tables.
			VerifyDigest: verifyDigest,
		},
		DstConn: m.cfg.Dest,
		DstCert: m.cfg.SSLCert,
		DstKey:  m.cfg.SSLKey,
		DstCA:   m.cfg.SSLCA,
	})
}

// wantsRoutedWrite reports whether a router_full table should take the P9
// routed segment-to-segment data path (S≠D, or S==D with unequal keys): the
// plan proved the route is router_full, the table is not a partition parent,
// the ON SEGMENT dispatch is deployed (--seg-binary + --seg-listen), and the
// operator has not disabled the routed write (--no-routed-write). Anything
// else degrades to the merge path (SliceMux through the destination
// coordinator — correct, just not segment-to-segment).
func (m *Manager) wantsRoutedWrite(u *Unit, part pg.PartitionSpec) bool {
	return u.RouteMode == route.RouterFull &&
		part.Spec == "" &&
		m.cfg.SegBinary != "" &&
		m.cfg.SegListen != "" &&
		!m.cfg.NoRoutedWrite
}

// streamTwoAgentSegment runs the P4 on_segment data path through the agents:
// the controller (control plane only) reads the source's primary-segment count
// and the table shape (columns / distribution / partition) on its source pool,
// builds the UnitSpec the destination agent needs, and hands it to the
// twoAgentRunner. The source agent triggers the ON SEGMENT dispatch (the kernel
// launches the segreceiver on every primary source segment, which stream each
// slice compressed to the destination agent); the destination agent merges the
// N slices (SliceMux) and loads the staging table. The controller holds no data
// connection: it reads back the destination's authoritative row count + the
// on-network wire volume. The swap is the controller's decision (runUnit step 9).
func (m *Manager) streamTwoAgentSegment(ctx context.Context, t plan.Table, u *Unit, target, ds, dn string, cols []pg.Column, dist pg.Distribution, part pg.PartitionSpec) (int64, uint64, uint64, error) {
	n, err := m.srcPrimarySegmentCount(ctx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("on_segment two-agent: %w", err)
	}
	uc, _ := m.unitCompressors(t)
	jobID := jobIDFromHash(u.JobID)
	overwrite := m.cfg.ConflictAction == config.Overwrite || u.Conflict
	compStr := m.twoAgentCompressString(uc)

	// P8 direct_1to1 fast-path: the plan proved this table's route is
	// direct_1to1 (S==D, equal hash keys, same major, non-legacy). Then source
	// segment s's slice == destination segment s's slice, so the destination
	// can load the N content-tagged slices with N per-segment utility-mode
	// COPYs (ServeDirect) instead of merging them into one coordinator COPY
	// (ServeSegments): no SliceMux, no destination re-distribution. The source
	// side is the SAME ON SEGMENT dispatch (the kernel streams the same
	// content-tagged slices). A partitioned parent cannot be written
	// per-segment (utility-mode COPY FROM is rejected on a partition-hierarchy
	// root), so it stays on the merge path; a mismatch in the live destination
	// segment count (a primary down since planning) also degrades to the merge
	// path.
	// Design 04.10 (hash-mismatch guard): the plan degraded this table to
	// merge because the kernels cannot place rows identically (different
	// major / legacy hashops) — log it so the operator sees why the table
	// did not take a segment-to-segment sub-path.
	if u.RouteReason != "" {
		ev := "routed_fallback"
		if strings.HasPrefix(u.RouteReason, "hash-mismatch") {
			ev = "hash_mismatch_fallback"
		}
		m.events.Log(ev,
			"table", t.Schema+"."+t.Name,
			"reason", u.RouteReason)
	}
	var req unitSegmentRequest
	req = unitSegmentRequest{
		OnSegmentReq: agent.OnSegmentReq{
			Op:            "onsegment",
			Schema:        t.Schema,
			Table:         t.Name,
			JobID:         jobID,
			UnitID:        uint32(u.UnitID),
			SegBinary:     m.cfg.SegBinary,
			Compress:      compStr,
			DataTLSCert:   m.cfg.SegTLSCert,
			DataTLSKey:    m.cfg.SegTLSKey,
			DataTLSCA:     m.cfg.SegTLSCA,
			FrameAck:      m.cfg.FrameAckArg(),
			AckTimeout:    m.cfg.AckTimeout,
			MaxReconnects: m.cfg.MaxReconnects,
			RetransmitMax: m.cfg.RetransmitBufferMax,
		},
		Spec: agent.UnitSpec{
			JobID:     jobID,
			UnitID:    uint32(u.UnitID),
			Schema:    ds,
			Table:     dn,
			Staging:   target,
			Overwrite: overwrite,
			SrcSchema: t.Schema,
			SrcTable:  t.Name,
			Compress:  compStr,
			Cols:      cols,
			Dist:      dist,
			Part:      part,
		},
		Segments:  n,
		SegListen: m.cfg.SegListen,
		DstConn:   m.cfg.Dest,
		DstCert:   m.cfg.SSLCert,
		DstKey:    m.cfg.SSLKey,
		DstCA:     m.cfg.SSLCA,
	}
	if u.RouteMode == route.Direct1to1 && part.Spec == "" {
		segs, serr := m.destSegments(ctx)
		if serr == nil && len(segs) == n {
			dstDSNs := make([]string, 0, n)
			ok := true
			for _, seg := range segs {
				dsn, derr := segmentDSN(m.cfg.Dest, seg)
				if derr != nil {
					ok = false
					break
				}
				dstDSNs = append(dstDSNs, dsn)
			}
			if ok {
				req.DirectWrite = true
				req.DirectLaunch = agent.DirectLaunch{
					UnitID:    uint32(u.UnitID),
					Schema:    ds,
					Table:     dn,
					Staging:   target,
					Overwrite: overwrite,
					Compress:  compStr,
					Cols:      cols,
					Dist:      dist,
					Part:      part,
					D:         int32(n),
					DstDSNs:   dstDSNs,
				}
				m.events.Log("direct_1to1_fastpath",
					"table", t.Schema+"."+t.Name,
					"segments", n,
					"reason", "S==D + equal hash keys + same major: per-segment utility COPYs, no merge/re-distribution")
			}
		}
	}
	// P9 routed write (S≠D, or S==D with unequal keys): the plan proved
	// router_full, and the ON SEGMENT dispatch is available. The segreceivers
	// route every row to its destination segment (cdbhash + JCH over the
	// destination distribution key) and fan each (source-seg,dest-seg) pair
	// over --workers sub-streams: S×D×W segment-to-segment TCP connections,
	// NEITHER cluster coordinator in the data path (the source coordinator
	// only dispatched the read, the destination coordinator only built the
	// staging DDL / swaps it). A table that cannot be routed degrades to the
	// merge path (SliceMux through the destination coordinator — correct,
	// just not segment-to-segment).
	if m.wantsRoutedWrite(u, part) {
		rr, rwire, rlogical, rerr := m.streamTwoAgentRouted(ctx, t, u, target, ds, dn, cols, dist, part,
			compStr, jobID, overwrite, n)
		if isDirectDegraded(rerr) {
			m.events.Log("routed_degraded",
				"table", t.Schema+"."+t.Name,
				"reason", rerr.Error())
			// fall through to the merge path
		} else {
			return rr, rwire, rlogical, rerr
		}
	}
	return m.twoAgent.StreamSegment(ctx, req)
}

// streamTwoAgentMini runs the P3 MESH mini data path (P4 design, production
// dispatch): the source agent triggers the ON SEGMENT dispatch with the
// D-entry address book (the destination-segment mini-agent data endpoints,
// seg order); each source segreceiver streams its slice to its destination
// segment's mini-agent (direct_1to1: segreceiver s dials entry s; routed
// S≠D: it routes every row to its destination segment d by cdbhash/JCH over
// the distribution key and fans each (s,d) pair over W sub-streams to entry
// d); each mini-agent performs a LOCAL utility COPY on its segment. The
// destination agent (supervisor mode) builds the distributed staging table,
// broadcasts the unit to the D minis, and combines their reports (XOR_D).
// NEITHER cluster coordinator is in the data path (design 04.10).
//
// Per-table degradation (the P4 design): a table that cannot be placed
// segment-to-segment — a merge route (hash-mismatch / non-hash distribution),
// a partitioned parent, or a non-Greenplum side (no ON SEGMENT dispatch / no
// segment catalog) — takes the COORDINATOR path instead (one COPY through
// the destination coordinator, executed by the supervisor's fallback path),
// logged so the operator sees why. The rest of the job keeps the mini path
// (degradation is per table, not per job).
// streamTwoAgentMini runs the MESH (segment-to-segment) data path through the
// agents. The destination agent (supervisor mode) owns the verify + swap
// decision (architecture 08.10): it compares the SOURCE vs DEST digests and,
// when they match (or --verify off), performs the atomic swap itself. The
// manager returns the destination's verdict (swapped, verified) so runUnit
// skips its own verify/swap for the mesh unit.
func (m *Manager) streamTwoAgentMini(ctx context.Context, t plan.Table, u *Unit, target, ds, dn string, cols []pg.Column, dist pg.Distribution, part pg.PartitionSpec, verifyDigest bool) (int64, uint64, uint64, string, string, bool, bool, []string, error) {
	uc, _ := m.unitCompressors(t)
	jobID := jobIDFromHash(u.JobID)
	compStr := m.twoAgentCompressString(uc)

	// 1. Topology: S (source primaries) + D (destination primaries). Both
	//    require a Greenplum side; a plain-PostgreSQL side cannot run the
	//    mini path (no ON SEGMENT dispatch / no per-segment utility COPY),
	//    so it degrades to the coordinator path (per table).
	S, serr := m.srcPrimarySegmentCount(ctx)
	segs, derr := m.destSegments(ctx)
	D := len(segs)
	if serr != nil || derr != nil || D == 0 {
		reason := "non-Greenplum source/destination (no segment catalog)"
		if derr != nil {
			reason = derr.Error()
		} else if serr != nil {
			reason = serr.Error()
		}
		m.events.Log("mesh_coordinator_fallback",
			"table", t.Schema+"."+t.Name,
			"reason", reason)
		rows, wire, logical, srcD, dstD, coordSw, coordVer, err := m.streamTwoAgent(ctx, t, u, target, ds, dn, verifyDigest)
		return rows, wire, logical, srcD, dstD, coordSw, coordVer, nil, err
	}

	// 2. Route decision (the plan's per-table mode, re-checked against the
	//    live topology): direct_1to1 (S==D, equal hash keys, non-legacy) ->
	//    the mini direct path (segreceiver s -> mini s, one slice each);
	//    router_full (S!=D or unequal keys, hash-distributed) -> the mini
	//    routed path (segreceivers route by cdbhash/JCH, S*D*W
	//    segment-to-segment streams, no coordinator in the data path);
	//    anything else (merge / partitioned) -> the coordinator path.
	routed := m.wantsRoutedWrite(u, part)
	if u.RouteMode != route.Direct1to1 && !routed {
		reason := "route mode " + string(u.RouteMode)
		if u.RouteReason != "" {
			reason += " (" + u.RouteReason + ")"
		}
		m.events.Log("mesh_coordinator_fallback",
			"table", t.Schema+"."+t.Name,
			"reason", reason+" — cannot be placed segment-to-segment")
		rows, wire, logical, srcD, dstD, coordSw, coordVer, err := m.streamTwoAgent(ctx, t, u, target, ds, dn, verifyDigest)
		return rows, wire, logical, srcD, dstD, coordSw, coordVer, nil, err
	}
	// A live destination segment count that no longer matches the plan (a
	// primary down since planning) breaks slice coverage: degrade this
	// table to the coordinator path (correct, just not segment-direct).
	if u.RouteMode == route.Direct1to1 && D != S {
		m.events.Log("mesh_coordinator_fallback",
			"table", t.Schema+"."+t.Name,
			"reason", fmt.Sprintf("live destination segment count %d != planned %d (a primary down?) — slice coverage would break", D, S))
		rows, wire, logical, srcD, dstD, coordSw, coordVer, err := m.streamTwoAgent(ctx, t, u, target, ds, dn, verifyDigest)
		return rows, wire, logical, srcD, dstD, coordSw, coordVer, nil, err
	}

	// 3. The MiniLaunch: the staging layout (the supervisor builds the
	//    distributed staging table on the destination coordinator — the DDL
	//    replicates to the segments, so each mini finds its local staging
	//    slice) + the unit's shape (S source segments; W sub-streams per
	//    pair for the routed path). The inline digests stay OFF (the mesh
	//    re-read verify is the same as the segment/routed paths; the
	//    combined XOR_D digests are a future enhancement).
	W := m.cfg.WorkersPerPair
	// SourceSegments is the mini's fan-in: direct_1to1 -> 1 (each mini
	// accepts its OWN segreceiver's single slice); routed -> S (the S*W
	// sub-stream fan-in from all source segments).
	fanIn := int32(1)
	if routed {
		fanIn = int32(S)
	}
	// 1F: the version-exchange wiring (the source agent dials the dest's peer
	// listener once before the first mesh unit; it SSH-gathers the segreceiver
	// versions from the source segments as the "user@" prefix).
	peerAddr, segSSH := "", ""
	if m.twoAgent != nil {
		peerAddr, segSSH = m.twoAgent.PeerInfo()
	}
	mreq := unitMiniRequest{
		OnSegmentReq: agent.OnSegmentReq{
			Op:            "onsegment",
			Schema:        t.Schema,
			Table:         t.Name,
			JobID:         jobID,
			UnitID:        uint32(u.UnitID),
			SegBinary:     m.cfg.SegBinary,
			PeerAddr:      peerAddr,
			SegSSH:        segSSH,
			Compress:      compStr,
			DataTLSCert:   m.cfg.SegTLSCert,
			DataTLSKey:    m.cfg.SegTLSKey,
			DataTLSCA:     m.cfg.SegTLSCA,
			FrameAck:      m.cfg.FrameAckArg(),
			AckTimeout:    m.cfg.AckTimeout,
			MaxReconnects: m.cfg.MaxReconnects,
			RetransmitMax: m.cfg.RetransmitBufferMax,
		},
		Launch: agent.MiniLaunch{
			Schema:         ds,
			Staging:        target,
			Cols:           cols,
			Dist:           dist,
			Part:           part,
			Compress:       compStr,
			JobID:          jobID,
			SourceSegments: fanIn,
			Workers:        W,
			// The destination agent owns the verify + swap decision (runUnit
			// step 9 moved to the agent): it compares the SOURCE vs DEST
			// digests and swaps the staging to its final name itself.
			Final:     dn,
			Overwrite: m.cfg.ConflictAction == config.Overwrite || u.Conflict,
			Verify:    verifyDigest,
			// The destination agent writes the gpstream_units rows for this
			// MESH unit (architecture 08.10): it carries the journal key
			// (the controller's string job id + int64 unit id) + the source
			// names + the run's snapshot epoch.
			JournalJobID:  u.JobID,
			JournalUnitID: u.UnitID,
			JournalDB:     t.DB,
			JournalSchema: t.Schema,
			JournalTable:  t.Name,
			JournalKind:   "table",
			Epoch:         m.epoch,
		},
		D: D,
	}
	if routed {
		// The routed (S!=D) mini path: the segreceivers route every row to
		// its destination segment by cdbhash/JCH over the source's
		// distribution key (the staging/final table is created with the
		// source's distribution, so the destination's placement key is the
		// source's dist key) and fan each (s,d) pair over W sub-streams to
		// the D-entry address book (entry d = destination segment d's
		// mini-agent).
		if dist.Kind != "hash" || len(dist.Keys) == 0 {
			m.events.Log("mesh_coordinator_fallback",
				"table", t.Schema+"."+t.Name,
				"reason", fmt.Sprintf("distribution policy %q (no hash key) cannot be routed", dist.Kind))
			rows, wire, logical, srcD, dstD, coordSw, coordVer, err := m.streamTwoAgent(ctx, t, u, target, ds, dn, verifyDigest)
			return rows, wire, logical, srcD, dstD, coordSw, coordVer, nil, err
		}
		colBy := map[string]int{}
		for i, c := range cols {
			colBy[c.Name] = i
		}
		routeKey := ""
		for i, k := range dist.Keys {
			idx, ok := colBy[k]
			if !ok {
				m.events.Log("mesh_coordinator_fallback",
					"table", t.Schema+"."+t.Name,
					"reason", fmt.Sprintf("dist-key column %q is not in the source table", k))
				rows, wire, logical, srcD, dstD, coordSw, coordVer, err := m.streamTwoAgent(ctx, t, u, target, ds, dn, verifyDigest)
				return rows, wire, logical, srcD, dstD, coordSw, coordVer, nil, err
			}
			pair := fmt.Sprintf("%d:%d", cols[idx].Oid, idx)
			if i == 0 {
				routeKey = pair
			} else {
				routeKey += "," + pair
			}
		}
		mreq.OnSegmentReq.Routed = true
		mreq.OnSegmentReq.RouteKey = routeKey
		mreq.OnSegmentReq.RouteD = int32(D) // destination segment count (the address-book size)
		mreq.OnSegmentReq.RouteWorkers = W
		m.events.Log("mesh_routed_write",
			"table", t.Schema+"."+t.Name,
			"src_segments", S,
			"dst_segments", D,
			"workers", W,
			"connections", S*D*W,
			"reason", "S!=D (or unequal keys): segreceivers route to destination-segment mini-agents; S*D*W segment-to-segment streams, no coordinator in the data path")
	} else {
		// Direct_1to1: each segreceiver s dials the address-book entry s (its
		// own destination segment's mini-agent). RouteD carries the SOURCE
		// segment count (S) — ProgramCommandMesh's --dest-count.
		mreq.OnSegmentReq.RouteD = int32(S)
		m.events.Log("mesh_direct_write",
			"table", t.Schema+"."+t.Name,
			"segments", S,
			"reason", "S==D + equal hash keys + same major: per-segment utility COPYs on the destination mini-agents, no coordinator in the data path")
	}
	return m.twoAgent.StreamMini(ctx, mreq)
}

// streamTwoAgentRouted runs the P9 routed (S≠D) data path: the source agent
// triggers the ON SEGMENT dispatch with routing enabled (each segreceiver
// routes its slice to its destination segment and fans each (s,d) pair over
// W sub-streams); the destination agent (routed mode) merges the per-segment
// sub-streams and performs D utility-mode per-segment COPYs. The controller
// computes the routing key (the source distribution key, mapped to its column
// positions + type OIDs) and the per-segment destination DSNs (a catalog
// read on the dest — control plane). A table that cannot be routed (non-hash
// distribution, unsupported key, a key column absent from the source, or a
// non-Greenplum destination) returns a *directDegradedError; the caller falls
// back to the merge path. The swap is the controller's decision (runUnit).
func (m *Manager) streamTwoAgentRouted(ctx context.Context, t plan.Table, u *Unit, target, ds, dn string, cols []pg.Column, dist pg.Distribution, part pg.PartitionSpec, compStr string, jobID uint32, overwrite bool, S int) (int64, uint64, uint64, error) {
	// 1. Destination topology: the up primaries in content order.
	segs, err := m.destSegments(ctx)
	if err != nil {
		return 0, 0, 0, &directDegradedError{reason: err.Error()}
	}
	D := int32(len(segs))
	// 2. The routing key: the table's distribution policy. The staging/final
	// table is created with the source's distribution (a faithful copy), so
	// the destination's placement key is the source's dist key — route by it.
	if dist.Kind != "hash" || len(dist.Keys) == 0 {
		return 0, 0, 0, &directDegradedError{
			reason: fmt.Sprintf("distribution policy %q (no hash key) cannot be routed", dist.Kind),
		}
	}
	colBy := map[string]int{}
	for i, c := range cols {
		colBy[c.Name] = i
	}
	keyIdx := make([]int, 0, len(dist.Keys))
	keyOid := make([]uint32, 0, len(dist.Keys))
	routeKey := ""
	for i, k := range dist.Keys {
		idx, ok := colBy[k]
		if !ok {
			return 0, 0, 0, &directDegradedError{
				reason: fmt.Sprintf("dist-key column %q is not in the source table", k),
			}
		}
		keyIdx = append(keyIdx, idx)
		keyOid = append(keyOid, cols[idx].Oid)
		pair := fmt.Sprintf("%d:%d", cols[idx].Oid, idx)
		if i == 0 {
			routeKey = pair
		} else {
			routeKey += "," + pair
		}
	}
	// 3. Per-segment utility-mode DSNs (content 0..D-1).
	dstDSNs := make([]string, 0, D)
	for _, seg := range segs {
		dsn, derr := segmentDSN(m.cfg.Dest, seg)
		if derr != nil {
			return 0, 0, 0, &directDegradedError{reason: derr.Error()}
		}
		dstDSNs = append(dstDSNs, dsn)
	}
	W := m.cfg.WorkersPerPair
	m.events.Log("routed_write",
		"table", t.Schema+"."+t.Name,
		"src_segments", S,
		"dst_segments", int(D),
		"workers", W,
		"connections", S*int(D)*W,
		"reason", "S!=D (or unequal keys): segreceivers route to destination segments; S*D*W segment-to-segment streams, no coordinator in the data path")
	return m.twoAgent.StreamRouted(ctx, unitRoutedRequest{
		OnSegmentReq: agent.OnSegmentReq{
			Op:            "onsegment",
			Schema:        t.Schema,
			Table:         t.Name,
			JobID:         jobID,
			UnitID:        uint32(u.UnitID),
			SegBinary:     m.cfg.SegBinary,
			Compress:      compStr,
			DataTLSCert:   m.cfg.SegTLSCert,
			DataTLSKey:    m.cfg.SegTLSKey,
			DataTLSCA:     m.cfg.SegTLSCA,
			Routed:        true,
			RouteKey:      routeKey,
			RouteD:        D,
			RouteWorkers:  W,
			FrameAck:      m.cfg.FrameAckArg(),
			AckTimeout:    m.cfg.AckTimeout,
			MaxReconnects: m.cfg.MaxReconnects,
			RetransmitMax: m.cfg.RetransmitBufferMax,
		},
		Launch: agent.RoutedLaunch{
			DirectLaunch: agent.DirectLaunch{
				UnitID:    uint32(u.UnitID),
				Schema:    ds,
				Table:     dn,
				Staging:   target,
				Overwrite: overwrite,
				Compress:  compStr,
				Cols:      cols,
				Dist:      dist,
				Part:      part,
				D:         D,
				DstDSNs:   dstDSNs,
			},
			SourceSegments: int32(S),
			Workers:        W,
		},
		Segments:  S,
		SegListen: m.cfg.SegListen,
		DstConn:   m.cfg.Dest,
		DstCert:   m.cfg.SSLCert,
		DstKey:    m.cfg.SSLKey,
		DstCA:     m.cfg.SSLCA,
	})
}

// twoAgentCompressString renders the unit's compression mode the way the
// destination agent's compress.Parse expects (the agent side must pick the
// same mode as the source side of the wire). The unit's compressor is the
// none-pair either when the operator chose --compress none or when the F-12
// small-table auto-disable applied (unitCompressors).
func (m *Manager) twoAgentCompressString(c *compress.Compressor) string {
	if m.compMode == compress.ModeNone || c == m.compNone {
		return "none"
	}
	switch m.compMode {
	case compress.ModeZlib:
		return "zlib"
	default:
		return "zstd"
	}
}

// jobIDFromHash derives the uint32 frame job-id the agents bind their frames
// to, from the string plan hash (the single-controller path uses a fixed 1).
func jobIDFromHash(jobID string) uint32 {
	var h uint32
	for i := 0; i < len(jobID); i++ {
		h = h*31 + uint32(jobID[i])
	}
	return h
}

// measureBalance records the F-15 segment-fill balance of a just-swapped
// unit (spec F-15: "control of destination segment-fill imbalance
// (max/avg metric per segment), warning when the threshold is exceeded").
// It runs AFTER the swap, so the final table holds the copied rows.
// Non-fatal by design: the data is already in place and the metric is
// diagnostic — a non-Greenplum destination (no gp_segment_id) or a
// measurement fault simply yields no balance. Every measured table is
// recorded in events.jsonl (F-04) and appended to Stats.Balances for the
// final report; a threshold breach additionally carries warning=true.
func (m *Manager) measureBalance(ctx context.Context, u *Unit) {
	ds, dn := u.DstSchema, u.DstName
	if ds == "" {
		ds = u.Schema
	}
	if dn == "" {
		dn = u.Table
	}
	b, berr := m.dstBalance(ctx, ds, dn)
	if berr != nil {
		// Non-fatal (F-15 is diagnostic): a balance read fault is logged
		// and the unit is not affected (the data is in place).
		m.events.Log("segment_balance_failed",
			"table", ds+"."+dn, "error", berr.Error())
		return
	}
	if b.Segments == 0 {
		return // not Greenplum, or the table is empty / unmeasurable
	}
	m.statMu.Lock()
	m.Balances = append(m.Balances, b)
	m.statMu.Unlock()
	m.events.Log("segment_balance",
		"table", b.Table, "segments", b.Segments, "max", b.Max,
		"avg", b.Avg, "max_avg", b.MaxAvg,
		"threshold", b.Threshold, "warning", b.Warning)
}

// createSequences recreates the unit's source sequences on the destination
// (F-11 metadata phase: "sequences"). It runs BEFORE the table
// DDL, because a serial column's default (nextval('schema.seq'::regclass))
// is copied verbatim into the destination table and must resolve to an
// existing sequence. The sequence keeps its source schema-qualified name.
// Non-fatal by design (like the FR-17 index build): CREATE SEQUENCE IF NOT
// EXISTS is idempotent, and a failure is recorded in events.jsonl (F-04)
// with the root cause.
//
// The catalog reads (sequence discovery, definition) run on the base source
// querier m.srcQ, NOT the pinned FR-02 snapshot connection: a fault there
// must not abort the REPEATABLE READ transaction that every data COPY
// shares (the F-06 fix 866318b, Columns/Distribution reads).
func (m *Manager) createSequences(ctx context.Context, u *Unit) {
	schema, name := u.Schema, u.Table
	ds, dn := u.DstSchema, u.DstName
	if ds == "" {
		ds = schema
	}
	if dn == "" {
		dn = name
	}
	seqs, err := m.srcSequences(ctx, schema, name)
	if err != nil {
		m.events.Log("sequence_sync_failed",
			"table", ds+"."+dn,
			"error", err.Error(),
			"reason", "sequence catalog read (F-11)")
		return
	}
	for _, s := range seqs {
		if err := m.dstCreateSequence(ctx, s.Name); err != nil {
			m.events.Log("sequence_sync_failed",
				"table", ds+"."+dn,
				"sequence", s.Name,
				"error", err.Error(),
				"reason", "create sequence (F-11)")
		}
	}
}

// syncSequenceValues resynchronises the unit's destination sequences to the
// source's last_value (F-11: "values are synchronised post-load"). It
// runs AFTER the swap, so the final table holds the source rows and the
// next nextval on the destination returns last_value+1 — exactly like on
// the source. Non-fatal by design: the data is already in place, and the
// resync is idempotent (setval), so a resume re-runs it.
//
// The last_value read runs on the base source querier (not the pinned
// snapshot): a sequence only ever moves forward, so reading it outside the
// snapshot can only make the resynchronised destination value HIGHER, and
// the destination's nextval can never collide with an id the copied rows
// already hold.
func (m *Manager) syncSequenceValues(ctx context.Context, u *Unit) {
	schema, name := u.Schema, u.Table
	ds, dn := u.DstSchema, u.DstName
	if ds == "" {
		ds = schema
	}
	if dn == "" {
		dn = name
	}
	seqs, err := m.srcSequences(ctx, schema, name)
	if err != nil {
		// createSequences already logged the identical catalog-read fault
		// (step 3a); the values cannot be resynced, but the data is in
		// place, so this is not a new failure.
		return
	}
	for _, s := range seqs {
		srcSeq := s.Name
		base := s.Name
		if i := strings.LastIndex(base, "."); i >= 0 {
			srcSeq = base[:i]
			base = base[i+1:]
		}
		last, err := m.srcSequenceLastValue(ctx, s.Name)
		if err != nil {
			m.events.Log("sequence_sync_failed",
				"table", ds+"."+dn,
				"sequence", s.Name,
				"error", err.Error(),
				"reason", "read source last_value (F-11)")
			continue
		}
		if last > 0 {
			if err := m.dstSetSequenceValue(ctx, srcSeq, base, last); err != nil {
				m.events.Log("sequence_sync_failed",
					"table", ds+"."+dn,
					"sequence", s.Name,
					"error", err.Error(),
					"reason", "setval on destination (F-11)")
				continue
			}
		}
		m.events.Log("sequence_synced",
			"table", ds+"."+dn,
			"sequence", s.Name,
			"last_value", last)
	}
}

// enqueueIndexes reads the source table's index definitions and enqueues a
// background build over the destination's final name (FR-17). Failures are
// non-fatal by design: the unit's data is already in place, and the journal
// scan in Run (requeueIndexes) re-enqueues the build on the next resume.
func (m *Manager) enqueueIndexes(ctx context.Context, u *Unit) {
	defs, err := m.srcIndexes(ctx, u.Schema, u.Table)
	if err != nil {
		return
	}
	// The PRIMARY KEY's unique index is NOT built here: the post-swap
	// constraint pass applies "ADD CONSTRAINT ... PRIMARY KEY", which builds
	// the PK index atomically. PostgreSQL does not adopt a pre-existing
	// standalone unique index on that statement, so a pre-built PK index
	// would be a 42P07 collision. The pool builds only the secondary indexes
	// (which overlap the copy, F-17).
	if out := poolIndexDefs(defs); len(out) == 0 {
		return
	} else {
		defs = out
	}
	m.idxMu.Lock()
	m.idxSeen[uint32(u.UnitID)] = true
	m.idxMu.Unlock()
	m.idxPool.Enqueue(index.Job{
		UnitID:    uint32(u.UnitID),
		Dst:       m.indexDst(),
		Schema:    u.DstSchema,
		Table:     u.DstName,
		SrcSchema: u.Schema,
		SrcTable:  u.Table,
		Defs:      defs,
		Priority:  u.Bytes, // --index-priority=size: larger tables first
	})
}

// indexDst is the pool worker's destination-side build capability (2B-4):
// the destination AGENT in the two-agent topology (the controller holds no
// direct destination connection); the controller's own m.dst pool in the
// single-process fallback (dead in production, removed in 2B-7).
func (m *Manager) indexDst() index.DST {
	if m.twoAgent != nil {
		return m
	}
	return m.dst
}

// poolIndexDefs filters the source index definitions to the ones the FR-17
// pool must build: the secondary indexes. The PRIMARY KEY's unique index is
// excluded — the post-swap constraint pass applies
// "ADD CONSTRAINT ... PRIMARY KEY", which builds the PK index atomically, and
// PostgreSQL does not adopt a pre-existing standalone unique index on that
// statement, so a pre-built PK index would be a 42P07 collision.
func poolIndexDefs(defs []pg.IndexDef) []pg.IndexDef {
	out := make([]pg.IndexDef, 0, len(defs))
	for _, d := range defs {
		if d.PK {
			continue
		}
		out = append(out, d)
	}
	return out
}

// requeueIndexes re-enqueues the index build for every unit the journal holds
// in the final ("swapped") state but that this run has not yet enqueued — e.g.
// a previous run killed after the swap and before the pool drained. The
// idempotent createOneIndex (existing index -> REINDEX by name) makes the
// re-enqueue duplicate-safe (FR-17 acceptance: clean resume, no duplicates).
func (m *Manager) requeueIndexes(ctx context.Context) {
	for _, u := range m.journal.Units() {
		if u.State != StateSwapped {
			continue
		}
		// T5: a kept (swapped in an earlier run) unit whose indexes were
		// already built (IndexDone) is not re-enqueued — an idempotent
		// re-run must not REINDEX every table (FR-17 + DESIGN §2.2 keep).
		if u.IndexDone {
			continue
		}
		m.idxMu.Lock()
		_, seen := m.idxSeen[uint32(u.UnitID)]
		if !seen {
			m.idxSeen[uint32(u.UnitID)] = true
		}
		m.idxMu.Unlock()
		if seen {
			continue
		}
		defs, err := m.srcIndexes(ctx, u.Schema, u.Table)
		if err != nil {
			continue
		}
		if defs = poolIndexDefs(defs); len(defs) == 0 {
			continue
		}
		m.idxPool.Enqueue(index.Job{
			UnitID:    uint32(u.UnitID),
			Dst:       m.indexDst(),
			Schema:    u.DstSchema,
			Table:     u.DstName,
			SrcSchema: u.Schema,
			SrcTable:  u.Table,
			Defs:      defs,
			Priority:  u.Bytes,
		})
	}
}

// CreateIndexes (2B-4) is the destination-agent build capability the FR-17
// index pool invokes when the controller holds no direct destination
// connection: it forwards to dstCreateIndexes (the dest agent's
// create-indexes ddl op). It satisfies index.DST.
func (m *Manager) CreateIndexes(ctx context.Context, schema, table, srcSchema, srcTable string, defs []pg.IndexDef) error {
	return m.dstCreateIndexes(ctx, schema, table, srcSchema, srcTable, defs)
}

// primarySegmentCount returns the number of up primary CONTENTS on the
// source — i.e. the number of segreceivers the kernel launches with
// ON SEGMENT (GANGTYPE_PRIMARY_WRITER, one per content; the master's
// content=-1 row is not a segment). It requires a Greenplum source
// (gp_segment_configuration); on plain PostgreSQL it errors, which the
// caller surfaces as a clear "segment-direct requires Greenplum" failure.
//
// count(*) would include the master row (content=-1) and overcount by one
// (E2 e2e, 27.09: n=3 on a 2-segment cluster — the receiver waited for a
// third slice that never came).
func (m *Manager) primarySegmentCount(ctx context.Context) (int, error) {
	v := pg.RowStr(ctx, m.srcQ,
		`SELECT count(DISTINCT content) FROM gp_segment_configuration
		 WHERE role = 'p' AND status = 'u' AND content >= 0`)
	if v == "" {
		return 0, fmt.Errorf("cannot enumerate segments (segment-direct requires a Greenplum source)")
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("bad segment count %q: %w", v, err)
	}
	return n, nil
}

// destSegment is one up primary of the destination cluster (content 0..D-1).
type destSegment struct {
	Content  int32
	Hostname string
	Port     int
}

// destSegments lists the destination's up primary segments in content order
// (R5). Two-agent (2B-5): the DESTINATION agent enumerates + dense-validates
// them on a fresh LOCAL coordinator connection (the segments ddl op); the
// controller holds no direct destination connection. It requires a Greenplum
// destination (gp_segment_configuration); on a plain-PostgreSQL destination
// it errors, which the caller degrades to the coordinator path.
func (m *Manager) destSegments(ctx context.Context) ([]destSegment, error) {
	if m.twoAgent != nil {
		resp, err := m.twoAgent.DDL(ctx, agent.DDReq{Action: "segments"})
		if err != nil {
			return nil, err
		}
		out := make([]destSegment, 0, len(resp.Segments))
		for _, s := range resp.Segments {
			out = append(out, destSegment{Content: s.Content, Hostname: s.Hostname, Port: s.Port})
		}
		return out, nil
	}
	segRows, err := m.dstQ.Query(ctx,
		`SELECT content, hostname, port FROM gp_segment_configuration
		  WHERE role = 'p' AND status = 'u' AND content >= 0
		  ORDER BY content`)
	if err != nil {
		return nil, fmt.Errorf("cannot enumerate dest segments (direct mode requires a Greenplum destination): %w", err)
	}
	seg := make([]destSegment, 0, len(segRows))
	for _, r := range segRows {
		if len(r) < 3 {
			continue
		}
		seg = append(seg, destSegment{
			Content:  int32(toInt(r[0])),
			Hostname: fmt.Sprint(r[1]),
			Port:     int(toInt(r[2])),
		})
	}
	if len(seg) == 0 {
		return nil, fmt.Errorf("no up primary segments on the destination")
	}
	// content must be a dense 0..D-1 (a missing content breaks slice coverage).
	for i, s := range seg {
		if s.Content != int32(i) {
			return nil, fmt.Errorf("dest segment contents are not dense (got %d at position %d) — direct mode requires all primaries up", s.Content, i)
		}
	}
	return seg, nil
}

// toInt renders a numeric catalog value as an int (driver-agnostic).
func toInt(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	default:
		s := fmt.Sprint(n)
		var out int64
		_, _ = fmt.Sscanf(s, "%d", &out)
		return out
	}
}

// segmentDSN derives a utility-mode connection string for one destination
// segment from the operator's dest DSN (URL form): the host/port are swapped to
// the segment's, and gp_session_role=utility is appended (required to connect
// to a primary segment; verified live on open-gpdb 6.30.1, R5 spike).
func segmentDSN(destDSN string, seg destSegment) (string, error) {
	u, err := url.Parse(destDSN)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "", fmt.Errorf("segment-to-segment writes require a URL-form dest DSN (postgres://user@host:port/db), got %q — pass a URL-form --dest or the run degrades to the merge path through the destination coordinator", destDSN)
	}
	u.Host = seg.Hostname + ":" + strconv.Itoa(seg.Port)
	q := u.Query()
	q.Set("gp_session_role", "utility")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (m *Manager) closePipes(w []io.Closer) error {
	var first error
	for _, c := range w {
		if c != nil {
			if err := c.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

// directDegradedError marks a table that cannot use the direct path; the
// caller falls back to the coordinator path (correct, just not segment-direct).
type directDegradedError struct{ reason string }

func (e *directDegradedError) Error() string { return "direct mode: " + e.reason }

func isDirectDegraded(err error) bool {
	var d *directDegradedError
	return errors.As(err, &d)
}

// quoteIdentDSN quotes a SQL identifier (the dest agent's helper is not
// exported; keep a local copy for DSN/COPY building in this file).
func quoteIdentDSN(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '"') // closing quote
	return string(out)
}

// startRunStatsWriter periodically refreshes runstats.json (FR-04) so
// `gpstream status` can render a live table (throughput, ETA, per-table
// rows/bytes, unit states) for a running operation. It is a no-op (returns
// nil) when the journal has no file mirror. The final write happens on stop.
func (m *Manager) startRunStatsWriter(ctx context.Context) func() {
	path := m.journal.RunStatsPath()
	if path == "" {
		return nil
	}
	write := func() {
		m.mu_runstats.Lock()
		defer m.mu_runstats.Unlock()
		st := monitor.RunStats{
			UpdatedAt:   time.Now().UTC().Format(time.RFC3339),
			StartedAt:   m.monitor.StartedAt().UTC().Format(time.RFC3339),
			ElapsedSec:  int(m.monitor.Latency().Seconds()),
			Pair:        m.monitor.PairLabel(),
			Bytes:       m.monitor.Bytes,
			Decoded:     m.monitor.BytesDecoded,
			Rows:        m.monitor.RowsTotal(),
			Throughput:  m.monitor.Throughput(),
			ETASeconds:  m.monitor.ETASeconds(),
			ActiveFlows: m.monitor.ActiveFlowsCount(),
			ByStatus:    m.monitor.TablesByStatus(),
			PerTable:    m.monitor.PerTable(),
		}
		b, err := json.MarshalIndent(st, "", "  ")
		if err != nil {
			return
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, b, 0o644); err == nil {
			_ = os.Rename(tmp, path)
		}
	}
	write()
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				write()
			}
		}
	}()
	return func() { write() }
}

// startLimiterSampler periodically samples host load so the limiter adapts the
// data-path rate (FR-14). It is a no-op when no rate-limit is set and returns
// nil so the caller can skip the deferred stop.
func (m *Manager) startLimiterSampler(ctx context.Context) func() {
	if m.lim == nil {
		return nil
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.lim.Update()
			}
		}
	}()
	return func() {}
}

// jobID derives a stable job identifier from the plan hash (FR-10). It is used
// as the namespace prefix for staging tables and journal keys.
func jobID(p *plan.Plan) string {
	if p == nil || p.Hash == "" {
		return "gpstream"
	}
	return p.Hash
}

// isEOF reports whether err is io.EOF (a clean end of the COPY stream).
func isEOF(err error) bool {
	return errors.Is(err, io.EOF)
}
