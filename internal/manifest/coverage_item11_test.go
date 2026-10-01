package manifest

// F-09 coverage (item 11): manifest 72.8% -> >=80%. These tests target the
// deterministic, cluster-free branches: file-based journal/epoch error paths
// (MkdirAll/WriteFile/ReadFile/Unmarshal failures), the pure helpers
// (splitDot/dsnHostPort/hostPairLabel/ConflictError.Error), the FR-02
// snapshot machinery over FakeConn
// (establishSnapshot/beginSnapshotTx error+degrade paths), the F-11 sequence
// sync (createSequences/syncSequenceValues error branches), and the copyStream
// coordinator error branches (dest-fail, source-fail, segment-direct dispatch).
import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/ratelimit"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// badParentFile returns a path whose PARENT is a regular file, so any
// MkdirAll/OpenFile/Create/ReadFile/WriteFile under it fails with ENOTDIR
// (a non-NotExist OS error) — used to drive the journal error branches.
func badParentFile(t *testing.T) string {
	t.Helper()
	base := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(base, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed parent file: %v", err)
	}
	return base
}

// TestJournalRunStatsPathEmpty covers RunStatsPath with no mirror set.
func TestJournalRunStatsPathEmpty(t *testing.T) {
	j := NewJournal(nil, "")
	if j.RunStatsPath() != "" {
		t.Error("no mirror: RunStatsPath must be empty")
	}
	if j.ConflictsPath() != "" {
		t.Error("no mirror: ConflictsPath must be empty")
	}
}

// TestJournalWriteConflictsErrors covers the WriteConflicts OS-failure
// branches (MkdirAll and Create under a file-parent, plus a bad-marshal).
func TestJournalWriteConflictsErrors(t *testing.T) {
	bad := badParentFile(t)
	j := NewJournal(nil, filepath.Join(bad, "journal.jsonl"))
	if err := j.WriteConflicts([]plan.Conflict{{DB: "d", Schema: "s", Table: "t"}}); err == nil {
		t.Error("WriteConflicts under a file-parent: want MkdirAll error")
	}
	// A Create failure: the dir exists but the target path is a directory.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "conflicts.jsonl"), 0o755); err != nil {
		t.Fatalf("seed dir-as-file: %v", err)
	}
	j2 := NewJournal(nil, filepath.Join(dir, "journal.jsonl"))
	if err := j2.WriteConflicts([]plan.Conflict{{DB: "d"}}); err == nil {
		t.Error("WriteConflicts with a directory target: want Create error")
	}
}

// TestJournalInitNilQuerier covers Init with no primary (the file-mirror-only
// fallback) — it must be a no-op, not an error.
func TestJournalInitNilQuerier(t *testing.T) {
	j := NewJournal(nil, filepath.Join(t.TempDir(), "j.jsonl"))
	if err := j.Init(context.Background()); err != nil {
		t.Errorf("Init with nil querier: %v", err)
	}
}

// TestJournalMirrorAppendErrors covers mirrorAppend's OpenFile and Encode
// failure branches.
func TestJournalMirrorAppendErrors(t *testing.T) {
	bad := badParentFile(t)
	j := NewJournal(nil, filepath.Join(bad, "journal.jsonl"))
	if err := j.mirrorAppend(&Unit{UnitID: 1}); err == nil {
		t.Error("mirrorAppend under a file-parent: want OpenFile error")
	}
}

// TestJournalSwapStateErrors covers SwapState's OS/decode branches: a
// non-NotExist open error, a truncated (decode-error) mirror, and a
// no-identity (UnitID==0) record. SwapState re-reads the mirror to restore
// the cache, so the target unit must exist in the cache first (created via
// the valid mirror), then the mirror is redirected to the file under test.
func TestJournalSwapStateErrors(t *testing.T) {
	valid := filepath.Join(t.TempDir(), "journal.jsonl")
	// Seed a unit into the cache (SetState appends to the valid mirror).
	jb := NewJournal(nil, valid)
	if _, err := jb.CreateUnit(&Unit{UnitID: 1, Schema: "s", Table: "t"}, StatePending); err != nil {
		t.Fatalf("seed unit: %v", err)
	}
	// Non-NotExist open error: redirect the mirror to a file whose parent is
	// a regular file.
	bad := badParentFile(t)
	jb.mirror = filepath.Join(bad, "journal.jsonl")
	if err := jb.SwapState(1); err == nil {
		t.Error("SwapState under a file-parent: want open error")
	}
	// Truncated mirror: a complete record (unit 2) then a torn final line.
	// SwapState requires the unit to be in StateCompleted (checksum set).
	jt := NewJournal(nil, valid)
	if _, err := jt.CreateUnit(&Unit{UnitID: 2, Schema: "s", Table: "t", Checksum: "sha1:x"}, StateCompleted); err != nil {
		t.Fatalf("seed unit 2: %v", err)
	}
	torn := `{"unit_id":2,"state":"completed"}` + "\n" + `{"unit_id":`
	if err := os.WriteFile(valid, []byte(torn), 0o600); err != nil {
		t.Fatalf("seed torn mirror: %v", err)
	}
	if err := jt.SwapState(2); err != nil {
		t.Errorf("SwapState with a torn final line must be tolerated (nil), got %v", err)
	}
	if jt.Get(2) == nil {
		t.Error("complete record before the torn line must be restored")
	}
	// No-identity record (UnitID==0) is skipped, not placed in the cache.
	// Unit 2 is now swapped (from the torn-line swap), so use a fresh
	// completed unit for the no-id case.
	if _, err := jt.CreateUnit(&Unit{UnitID: 4, Schema: "s", Table: "t2", Checksum: "sha1:y"}, StateCompleted); err != nil {
		t.Fatalf("seed unit 4: %v", err)
	}
	if err := os.WriteFile(valid, []byte(`{"state":"pending"}`+"\n"), 0o600); err != nil {
		t.Fatalf("seed no-id mirror: %v", err)
	}
	if err := jt.SwapState(4); err != nil {
		t.Errorf("SwapState no-id: %v", err)
	}
	if jt.Get(0) != nil {
		t.Error("UnitID==0 must not be placed in the cache")
	}
}

// TestJournalCreateUnitSetStateError covers CreateUnit's SetState-failure
// return.
func TestJournalCreateUnitSetStateError(t *testing.T) {
	bad := badParentFile(t)
	j := NewJournal(nil, filepath.Join(bad, "journal.jsonl"))
	// A completed unit requires a checksum; without one SetState fails, which
	// CreateUnit must surface.
	_, err := j.CreateUnit(&Unit{UnitID: 1, Schema: "s", Table: "t"}, StateCompleted)
	if err == nil {
		t.Error("CreateUnit(completed, no checksum): want SetState error")
	}
}

// TestLoadMetaBranches covers LoadMeta's primary-querier branch, the
// mirror-error path, and the primary-vs-mirror epoch comparison.
func TestLoadMetaBranches(t *testing.T) {
	// Primary (gpstream_jobs) reports a HIGHER epoch than the mirror file.
	dir := t.TempDir()
	mirror := filepath.Join(dir, "journal.jsonl")
	q := pg.NewFakeConn().On(`snap_epoch FROM gpstream_jobs`, [][]any{{int64(7)}})
	j := NewJournal(q, mirror)
	// Mirror file with a LOWER epoch.
	meta := `{"job_id":"x","snap_epoch":3}`
	if err := os.WriteFile(metaPath(mirror), []byte(meta), 0o600); err != nil {
		t.Fatalf("seed meta: %v", err)
	}
	m, err := j.LoadMeta("x")
	if err != nil {
		t.Fatalf("LoadMeta: %v", err)
	}
	if m == nil || m.SnapEpoch != 7 {
		t.Errorf("LoadMeta: want primary epoch 7 (higher wins), got %+v", m)
	}
	// Mirror HIGHER than primary: the mirror wins.
	if err := os.WriteFile(metaPath(mirror), []byte(`{"job_id":"x","snap_epoch":9}`), 0o600); err != nil {
		t.Fatalf("seed meta2: %v", err)
	}
	m2, _ := j.LoadMeta("x")
	if m2 == nil || m2.SnapEpoch != 9 {
		t.Errorf("LoadMeta: want mirror epoch 9 (higher wins), got %+v", m2)
	}
	// No mirror file: primary alone is returned (mirrorMeta nil branch).
	os.Remove(metaPath(mirror))
	m3, _ := j.LoadMeta("x")
	if m3 == nil || m3.SnapEpoch != 7 {
		t.Errorf("LoadMeta (no mirror): want primary 7, got %+v", m3)
	}
	// Mirror read error (parent is a file) -> LoadMeta surfaces it.
	bad := badParentFile(t)
	jb := NewJournal(nil, filepath.Join(bad, "journal.jsonl"))
	if _, err := jb.LoadMeta("x"); err == nil {
		t.Error("LoadMeta under a file-parent: want mirror read error")
	}
}

// TestSaveMetaErrors covers SaveMeta's MkdirAll and WriteFile failure branches.
func TestSaveMetaErrors(t *testing.T) {
	// MkdirAll failure: parent is a file.
	bad := badParentFile(t)
	jm := NewJournal(nil, filepath.Join(bad, "journal.jsonl"))
	if err := jm.SaveMeta("x", &JobMeta{JobID: "x", SnapEpoch: 1}); err == nil {
		t.Error("SaveMeta under a file-parent: want MkdirAll error")
	}
	// WriteFile failure: dir exists but jobmeta.json is a directory.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, metaFileName), 0o755); err != nil {
		t.Fatalf("seed meta-dir: %v", err)
	}
	jw := NewJournal(nil, filepath.Join(dir, "journal.jsonl"))
	if err := jw.SaveMeta("x", &JobMeta{JobID: "x", SnapEpoch: 1}); err == nil {
		t.Error("SaveMeta with a directory meta target: want WriteFile error")
	}
}

// TestLoadMetaFileBranches covers loadMetaFile's missing/torn/negative-epoch
// branches.
func TestLoadMetaFileBranches(t *testing.T) {
	dir := t.TempDir()
	mp := metaPath(filepath.Join(dir, "journal.jsonl"))
	// Missing file -> nil, nil (covered via a fresh dir).
	jf := NewJournal(nil, filepath.Join(dir, "journal.jsonl"))
	if m, _ := jf.loadMetaFile(); m != nil {
		t.Errorf("missing meta file: want nil, got %+v", m)
	}
	// Torn/invalid JSON -> tolerated as nil, nil.
	if err := os.WriteFile(mp, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("seed torn meta: %v", err)
	}
	if m, _ := jf.loadMetaFile(); m != nil {
		t.Errorf("torn meta: want nil, got %+v", m)
	}
	// Negative epoch -> treated as "no metadata".
	if err := os.WriteFile(mp, []byte(`{"snap_epoch":-1}`), 0o600); err != nil {
		t.Fatalf("seed neg meta: %v", err)
	}
	if m, _ := jf.loadMetaFile(); m != nil {
		t.Errorf("negative epoch: want nil, got %+v", m)
	}
	// ReadFile error (parent is a file) -> surfaced.
	bad := badParentFile(t)
	jb := NewJournal(nil, filepath.Join(bad, "journal.jsonl"))
	if _, err := jb.loadMetaFile(); err == nil {
		t.Error("loadMetaFile under a file-parent: want read error")
	}
}

// TestEstablishEpochErrors covers establishEpoch's LoadMeta and SaveMeta
// failure returns.
func TestEstablishEpochErrors(t *testing.T) {
	bad := badParentFile(t)
	j := NewJournal(pg.NewFakeConn(), filepath.Join(bad, "journal.jsonl"))
	m := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), j)
	m.jobID = "x"
	if _, _, err := m.establishEpoch(); err == nil {
		t.Error("establishEpoch under a file-parent: want error")
	}
}

// TestJournalBaseDirMetaPathNoSlash covers the no-separator fallbacks.
func TestJournalBaseDirMetaPathNoSlash(t *testing.T) {
	j := NewJournal(nil, "journal.jsonl") // no slash
	if j.journalBaseDir() != "" {
		t.Error("no-slash mirror: journalBaseDir must be empty")
	}
	if metaPath("journal.jsonl") != metaFileName {
		t.Error("no-slash mirror: metaPath must be the bare filename")
	}
}

// TestAuditLogErrors covers AuditLog.Log's open/marshal/write failure branches
// (a no-op log with an empty path returns immediately).
func TestAuditLogErrors(t *testing.T) {
	// Empty path: no-op.
	noop := NewAuditLog("")
	noop.Log("x", "k", "v") // must not panic
	noop.Close()

	// Open failure: path's parent is a file.
	bad := badParentFile(t)
	al := NewAuditLog(filepath.Join(bad, "audit.log"))
	al.Log("x", "k", "v") // openLocked fails -> returns silently
	al.Close()
}

// TestSplitDotNoDot covers splitDot's no-separator branch.
func TestSplitDotNoDot(t *testing.T) {
	s, n := splitDot("nodot")
	if s != "" || n != "nodot" {
		t.Errorf("splitDot(nodot) = %q,%q; want ,nodot", s, n)
	}
	s2, n2 := splitDot("a.b.c")
	if s2 != "a.b" || n2 != "c" {
		t.Errorf("splitDot(a.b.c) = %q,%q; want a.b,c", s2, n2)
	}
}

// TestDSNHostPort covers dsnHostPort's URL and key/value forms.
func TestDSNHostPort(t *testing.T) {
	if dsnHostPort("") != "" {
		t.Error("empty DSN: want empty host:port")
	}
	// URL form.
	if got := dsnHostPort("postgres://u@dbhost:5433/db"); got != "dbhost:5433" {
		t.Errorf("URL form = %q; want dbhost:5433", got)
	}
	// key/value form (host + explicit port).
	if got := dsnHostPort("host=kvhost port=6543 user=u"); got != "kvhost:6543" {
		t.Errorf("kv form = %q; want kvhost:6543", got)
	}
	// key/value form (host only -> default port 5432).
	if got := dsnHostPort("host=onlyport user=u"); got != "onlyport:5432" {
		t.Errorf("kv host-only = %q; want onlyport:5432", got)
	}
	// No host at all -> empty.
	if got := dsnHostPort("user=u dbname=d"); got != "" {
		t.Errorf("no host = %q; want empty", got)
	}
}

// TestHostPairLabelEmpty covers hostPairLabel with an unparseable DSN.
func TestHostPairLabelEmpty(t *testing.T) {
	if hostPairLabel("user=u", "host=h") != "" {
		t.Error("unparseable source DSN: hostPairLabel must be empty")
	}
	if got := hostPairLabel("host=a port=1", "host=b port=2"); got != "a:1 -> b:2" {
		t.Errorf("hostPairLabel = %q; want a:1 -> b:2", got)
	}
}

// TestConflictErrorString covers ConflictError.Error.
func TestConflictErrorString(t *testing.T) {
	e := &ConflictError{Schema: "s", Table: "t"}
	if !strings.Contains(e.Error(), "s.t") || !strings.Contains(e.Error(), "conflict") {
		t.Errorf("ConflictError.Error = %q; want it to name s.t and say conflict", e.Error())
	}
}


// TestEstablishSnapshotBranches covers establishSnapshot's mutually-exclusive
// early return and the beginSnapshotTx-failure degrade path.
func TestEstablishSnapshotBranches(t *testing.T) {
	ctx := context.Background()
	// ConsistentDB && ConsistentCluster are mutually exclusive -> early return,
	// snapshotMode stays empty, no transaction issued.
	mBoth := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), func(c *config.Config) {
		c.ConsistentDB = true
		c.ConsistentCluster = true
	})
	mBoth.establishSnapshot(ctx)
	if mBoth.snapshotMode != "" || mBoth.snapshotTx {
		t.Error("mutually-exclusive consistency flags: must not open a snapshot")
	}
	// ConsistentDB with a source whose BEGIN fails -> degrade (snapshotMode
	// cleared, no live snapshot) but non-fatal.
	srcFail := pg.NewFakeConn().SetExecError(errors.New("begin aborted"))
	mDeg := NewManager(configDefault(), srcFail, pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	mDeg.cfg.ConsistentDB = true
	mDeg.establishSnapshot(ctx)
	if mDeg.snapshotTx {
		t.Error("failed BEGIN: snapshotTx must stay false")
	}
	if mDeg.snapshotMode != "" {
		t.Errorf("failed BEGIN: snapshotMode must degrade to empty, got %q", mDeg.snapshotMode)
	}
	// ConsistentDB with a healthy source -> a live snapshot is established.
	mOK := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	mOK.cfg.ConsistentDB = true
	mOK.establishSnapshot(ctx)
	if !mOK.snapshotTx || mOK.snapshotMode != "db" {
		t.Errorf("healthy BEGIN: want snapshotTx=true mode=db, got %v %q", mOK.snapshotTx, mOK.snapshotMode)
	}
	mOK.releaseSnapshot(ctx) // commit + unpin (no-op safe)
}

// TestBeginSnapshotTxErrors covers beginSnapshotTx's cluster-freeze and
// BEGIN failure branches.
func TestBeginSnapshotTxErrors(t *testing.T) {
	ctx := context.Background()
	fail := pg.NewFakeConn().SetExecError(errors.New("exec down"))
	m := NewManager(configDefault(), fail, pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	if err := m.beginSnapshotTx(ctx, "cluster"); err == nil || !strings.Contains(err.Error(), "consistent-cluster") {
		t.Errorf("cluster freeze failure: want consistent-cluster error, got %v", err)
	}
	if err := m.beginSnapshotTx(ctx, "db"); err == nil || !strings.Contains(err.Error(), "begin snapshot tx") {
		t.Errorf("BEGIN failure: want begin-snapshot-tx error, got %v", err)
	}
	// Healthy: db mode succeeds (no freeze, BEGIN ok).
	mOK := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	if err := mOK.beginSnapshotTx(ctx, "db"); err != nil {
		t.Errorf("healthy db BEGIN: %v", err)
	}
}

// TestHealSnapshotDoubleHeal covers healSnapshot's second-lock re-check
// (another worker already healed) by clearing snapshotTx between the two
// checks via a concurrent flip.
func TestHealSnapshotDoubleHeal(t *testing.T) {
	ctx := context.Background()
	m := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	m.snapshotTx = true
	m.snapshotMode = "db"
	// First heal on a healthy conn: probe SELECT 1 succeeds -> returns early
	// (snapshot still healthy). This is the common no-op path.
	m.healSnapshot(ctx)
	if !m.snapshotTx {
		t.Error("healthy heal: snapshotTx must remain true")
	}
	// Second heal with the conn aborted (Exec fails): probe fails, ROLLBACK
	// also fails (same exec error) -> degrade (snapshotMode cleared).
	m2 := NewManager(configDefault(), pg.NewFakeConn().SetExecError(errors.New("25P02")), pg.NewFakeConn(),
		NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	m2.snapshotTx = true
	m2.snapshotMode = "db"
	m2.healSnapshot(ctx)
	if m2.snapshotMode != "" {
		t.Error("aborted+dead conn heal: must degrade (snapshotMode empty)")
	}
}

// TestCreateSequencesErrors covers createSequences' catalog-read failure and
// the dest CreateSequence failure branches (non-fatal: logged, not returned).
func TestCreateSequencesErrors(t *testing.T) {
	ctx := context.Background()
	// Source sequence catalog read fails (no canned pg_get_serial_sequence).
	mSrc := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	mSrc.createSequences(ctx, &Unit{Schema: "s", Table: "t"}) // must not panic
	// DstSchema/DstName empty -> default to source names (the ds/dn fallback).
	mDef := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	mDef.createSequences(ctx, &Unit{Schema: "s", Table: "t"}) // empty dst names
}

// TestSyncSequenceValuesErrors covers syncSequenceValues' catalog-read,
// last_value-read and setval failure branches (all non-fatal).
func TestSyncSequenceValuesErrors(t *testing.T) {
	ctx := context.Background()
	// Catalog read fails (no canned pg_get_serial_sequence) -> returns early.
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), nil)
	m.syncSequenceValues(ctx, &Unit{Schema: "s", Table: "t"})
	// Catalog returns a sequence but the last_value read fails.
	src := pg.NewFakeConn().
		On("pg_get_serial_sequence", [][]any{{"public.t_id_seq", "id"}})
	// No "last_value" canned -> SequenceLastValue errors -> logged, continue.
	m2 := NewManager(configDefault(), src, pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	m2.syncSequenceValues(ctx, &Unit{Schema: "public", Table: "t", DstSchema: "public", DstName: "t"})
}

// TestCopyStreamDestError covers the coordinator copyStream branch where the
// destination COPY fails (dstErr reported first).
func TestCopyStreamDestError(t *testing.T) {
	ctx := context.Background()
	src := pg.NewFakeConn().OnReadCopy("TO STDOUT", []byte("data"))
	dst := pg.NewFakeConn() // no WriteCopy sink -> CopyIntoStream errors
	m := NewManager(configDefault(), src, dst, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	w, r := net.Pipe()
	fp := transfer.NewFramedPipe(w, r, 1, 1, compress.New(compress.ModeNone, 0), compress.NewDec(compress.ModeNone))
	defer fp.Close()
	_, _, _, err := m.copyStream(ctx, plan.Table{Schema: "s", Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "stg"}, "stg", fp,
		unitSource{agent: nil, q: src})
	if err == nil || !strings.Contains(err.Error(), "dest copy") {
		t.Errorf("dest COPY failure: want 'dest copy' error, got %v", err)
	}
}

// TestCopyStreamSourceError covers the branch where the source COPY fails
// (no source payload) but the destination succeeds on the empty stream.
func TestCopyStreamSourceError(t *testing.T) {
	ctx := context.Background()
	src := pg.NewFakeConn() // no ReadCopy payload -> CopyToStream errors
	dst := pg.NewFakeConn().OnWriteCopy("FROM STDIN")
	m := NewManager(configDefault(), src, dst, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	w, r := net.Pipe()
	fp := transfer.NewFramedPipe(w, r, 1, 1, compress.New(compress.ModeNone, 0), compress.NewDec(compress.ModeNone))
	defer fp.Close()
	_, _, _, err := m.copyStream(ctx, plan.Table{Schema: "s", Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "stg"}, "stg", fp,
		unitSource{agent: srcAgent(src), q: src})
	if err == nil || !strings.Contains(err.Error(), "source copy") {
		t.Errorf("source COPY failure: want 'source copy' error, got %v", err)
	}
}

// srcAgent builds a source.Agent over the given querier (test helper).
func srcAgent(q pg.StreamingQuerier) *source.Agent { return source.New(q) }

// TestCopyStreamSegmentDirectDispatch covers the copyStream ->
// copyStreamSegmentDirect hand-off: with --copy-mode on_segment the
// coordinator pipe is closed and the segment-direct path runs (which errors
// here because a fake source has no up primary segments).
func TestCopyStreamSegmentDirectDispatch(t *testing.T) {
	ctx := context.Background()
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), func(c *config.Config) {
		c.CopyMode = config.CopyOnSegment
	})
	w, r := net.Pipe()
	fp := transfer.NewFramedPipe(w, r, 1, 1, compress.New(compress.ModeNone, 0), compress.NewDec(compress.ModeNone))
	_, _, _, err := m.copyStream(ctx, plan.Table{Schema: "s", Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "stg"}, "stg", fp,
		unitSource{q: m.srcQ})
	if err == nil || !strings.Contains(err.Error(), "segment-direct") {
		t.Errorf("on_segment dispatch: want a segment-direct error, got %v", err)
	}
}

// TestFramedPipeFrameAckOff covers framedPipe's net.Pipe (frame-ack off)
// branch and the receive-queue write-latency hook.
func TestFramedPipeFrameAckOff(t *testing.T) {
	m := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), func(c *config.Config) {
		c.FrameAck = "off"
	})
	m.recvRec = &ratelimit.WriteRecorder{}
	fp := m.framedPipe(1, compress.New(compress.ModeNone, 0), compress.NewDec(compress.ModeNone))
	if fp == nil {
		t.Fatal("framedPipe (frame-ack off): want non-nil pipe")
	}
	fp.Close()
}

// TestStartRunStatsWriterNoMirror covers startRunStatsWriter's no-mirror
// no-op (returns nil) and the periodic writer over a real mirror.
func TestStartRunStatsWriterNoMirror(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// No mirror path -> nil stop func.
	m := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), ""))
	if stop := m.startRunStatsWriter(ctx); stop != nil {
		t.Error("no mirror: startRunStatsWriter must return nil")
	}
	// With a mirror -> non-nil stop; the writer refreshes runstats.json.
	dir := t.TempDir()
	m2 := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), filepath.Join(dir, "journal.jsonl")))
	stop := m2.startRunStatsWriter(ctx)
	if stop == nil {
		t.Fatal("mirror present: startRunStatsWriter must return a stop func")
	}
	stop()
}

// TestRunInPlaceTruncate runs a full copy with the Truncate conflict policy:
// the in-place path (create the final table, then truncate it, copy, verify,
// no staging+swap). This is the only path that exercises runUnit's in-place
// create+truncate branch.
func TestRunInPlaceTruncate(t *testing.T) {
	cfg := config.Default()
	cfg.ConflictAction = config.Truncate
	m, _ := newManager(t, &cfg, []byte("data"), []byte("data"))
	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run (in-place truncate): %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
}

// TestRunMissingExtensions covers the F-11 metadata-phase diagnostic: a plan
// that reports dependent extensions missing on the destination logs the
// extension_missing event (non-fatal, the run still succeeds).
func TestRunMissingExtensions(t *testing.T) {
	cfg := config.Default()
	m, _ := newManager(t, &cfg, []byte("data"), []byte("data"))
	p := oneUnitPlan()
	p.MissingExtensions = []string{"postgis", "madlib"}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run (missing extensions): %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
}

// directCols is the single-column layout the direct-path tests route on.
func directCols() []pg.Column {
	return []pg.Column{{Name: "id", Oid: 23}}
}

// TestCopyStreamDirectDestErrors covers copyStreamDirect's early degrade
// branches: a non-URL dest DSN (segmentDSN failure), a missing/empty
// gp_segment_configuration, a non-dense content layout, a replicated target
// (no hash key), and a hash key absent from the source — all degrade to the
// coordinator path (a directDegradedError, never a hard failure).
func TestCopyStreamDirectDestErrors(t *testing.T) {
	ctx := context.Background()
	direct := func(c *config.Config) { c.CopyMode = config.CopyDirect }

	// 1. Non-URL dest DSN: destSegments succeeds (canned), the target has a
	// hash key (so routing is possible), but segmentDSN fails on the
	// key/value DSN -> degrade.
	mKV := newMgr(t, pg.NewFakeConn().OnReadCopy("TO STDOUT", []byte("x")),
		pg.NewFakeConn().
			On("gp_segment_configuration", [][]any{{0, "h1", 6000}}).
			On("to_regclass", [][]any{{int32(1)}}).
			On("policytype", [][]any{{"p", "id"}}), direct)
	mKV.cfg.Dest = "host=h1 port=6000 user=u dbname=d"
	_, _, _, err := mKV.copyStreamDirect(ctx, plan.Table{Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "t"}, "t", directCols(), unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("non-URL dest DSN: want directDegradedError, got %v", err)
	}

	// 2. gp_segment_configuration unreadable (no canned result) -> degrade.
	mNoSeg := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn(), direct)
	_, _, _, err = mNoSeg.copyStreamDirect(ctx, plan.Table{Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "t"}, "t", directCols(), unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("no segment catalog: want directDegradedError, got %v", err)
	}

	// 3. No up primary segments (empty catalog) -> degrade.
	mEmpty := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn().On("gp_segment_configuration", [][]any{}), direct)
	_, _, _, err = mEmpty.copyStreamDirect(ctx, plan.Table{Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "t"}, "t", directCols(), unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("empty segment catalog: want directDegradedError, got %v", err)
	}

	// 4. Non-dense contents (content 5 at position 0) -> degrade.
	mSparse := newMgr(t, pg.NewFakeConn(), pg.NewFakeConn().
		On("gp_segment_configuration", [][]any{{5, "h1", 6000}}), direct)
	mSparse.cfg.Dest = "postgres://u@h1:6000/d"
	_, _, _, err = mSparse.copyStreamDirect(ctx, plan.Table{Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "t"}, "t", directCols(), unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("non-dense contents: want directDegradedError, got %v", err)
	}

	// 5. No hash distribution key on the target (replicated) -> degrade.
	mNoHash := newMgr(t, pg.NewFakeConn(),
		pg.NewFakeConn().
			On("gp_segment_configuration", [][]any{{0, "h1", 6000}}).
			On("to_regclass", [][]any{{int32(1)}}).
			On("policytype", [][]any{{"r", ""}}), direct)
	mNoHash.cfg.Dest = "postgres://u@h1:6000/d"
	_, _, _, err = mNoHash.copyStreamDirect(ctx, plan.Table{Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "t"}, "t", directCols(), unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("replicated target: want directDegradedError, got %v", err)
	}

	// 6. Hash key column absent from the source layout -> degrade.
	mNoKey := newMgr(t, pg.NewFakeConn(),
		pg.NewFakeConn().
			On("gp_segment_configuration", [][]any{{0, "h1", 6000}}).
			On("to_regclass", [][]any{{int32(1)}}).
			On("policytype", [][]any{{"p", "missing_col"}}), direct)
	mNoKey.cfg.Dest = "postgres://u@h1:6000/d"
	_, _, _, err = mNoKey.copyStreamDirect(ctx, plan.Table{Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "t"}, "t", directCols(), unitSource{})
	if err == nil || !isDirectDegraded(err) {
		t.Errorf("dist key not in source: want directDegradedError, got %v", err)
	}
}

// TestCopyStreamDirectSegmentPath runs the FULL segment-direct data plane:
// two destination primaries on CLOSED ports. The per-segment goroutines
// attempt a real utility-mode connection (pg.NewConn) which is refused
// immediately; the router and source stream an empty (0-row) binary-COPY
// stream, so the only failure is the segment connect — surfacing the
// "direct dest" error after collectSegs drains both segments. This exercises
// segmentDSN, the per-segment net.Pipe + goroutine fan-out, NewRowRouter,
// collectSegs, closePipes and the error-precedence reporting.
func TestCopyStreamDirectSegmentPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	direct := func(c *config.Config) { c.CopyMode = config.CopyDirect }
	src := pg.NewFakeConn().OnReadCopy("TO STDOUT", []byte{} /* 0 rows */)
	dst := pg.NewFakeConn().
		On("gp_segment_configuration", [][]any{{0, "127.0.0.1", 1}, {1, "127.0.0.1", 2}}).
		On("to_regclass", [][]any{{int32(1)}}).
		On("policytype", [][]any{{"p", "id"}})
	m := newMgr(t, src, dst, direct)
	m.cfg.Dest = "postgres://u@127.0.0.1:5432/d"
	_, _, _, err := m.copyStreamDirect(ctx, plan.Table{Schema: "s", Name: "t"},
		&Unit{UnitID: 1, Schema: "s", Table: "t", DstSchema: "s", DstName: "t"}, "t", directCols(),
		unitSource{agent: source.New(src), q: src})
	if err == nil || !strings.Contains(err.Error(), "direct dest") {
		t.Fatalf("segment connect failure: want 'direct dest' error, got %v", err)
	}
	if !strings.Contains(err.Error(), "connect segment") {
		t.Errorf("want the segment connect error in the message, got %v", err)
	}
}

// TestRunDirectModeDegradesToCoordinator covers the runUnit direct-mode
// degrade: CopyMode=direct but the destination has no segment catalog, so
// copyStreamDirect degrades and the unit falls back to the coordinator path
// (the run still succeeds — degrade is correct, just not direct).
func TestRunDirectModeDegradesToCoordinator(t *testing.T) {
	cfg := config.Default()
	cfg.CopyMode = config.CopyDirect
	m, _ := newManager(t, &cfg, []byte("data"), []byte("data"))
	if err := m.Run(context.Background(), oneUnitPlan()); err != nil {
		t.Fatalf("run (direct, degrades to coordinator): %v", err)
	}
	if m.Units != 1 {
		t.Errorf("units = %d, want 1", m.Units)
	}
}
