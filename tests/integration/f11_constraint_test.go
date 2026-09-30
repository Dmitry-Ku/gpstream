//go:build integration

package integration

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
)

// TestConstraintRestoreFKCheckRename — F-11 (metadata phase, "constraints"):
// a source table with a foreign key and a user CHECK constraint is copied
// end-to-end. The staging+swap path creates a fresh destination table, so the
// post-swap constraint pass must re-apply BOTH constraints. The referenced
// table is ALSO migrated (and renamed), so the restored FK must reference the
// referenced table's DESTINATION name (the structural, search_path-independent
// rewrite), not its source name. The data path must be unaffected: counts
// match and the full checksum verify passes.
func TestConstraintRestoreFKCheckRename(t *testing.T) {
	src, dst, _ := startPGPair(t, 5711, 5712)
	defer func() { src.Close(); dst.Close() }()

	mustExec(t, src, `CREATE TABLE public.parents (id integer PRIMARY KEY, code text)`)
	mustExec(t, src, `CREATE TABLE public.children (
		id integer,
		parent_id integer,
		CONSTRAINT ch_pos CHECK (id > 0),
		CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES public.parents(id) ON DELETE CASCADE
	)`)
	mustExec(t, src, `INSERT INTO public.parents VALUES (1,'a'),(2,'b'),(3,'c')`)
	mustExec(t, src, `INSERT INTO public.children VALUES (10,1),(11,1),(12,2),(13,3),(14,3)`)
	if got := mustCount(t, src, `SELECT count(*) FROM public.children`); got != 5 {
		t.Fatalf("source children = %d rows, want 5", got)
	}
	// Confirm the source actually carries both constraints (premise).
	if got := mustCount(t, src, `SELECT count(*) FROM pg_constraint c
		JOIN pg_class t ON t.oid=c.conrelid WHERE t.relname='children' AND c.contype IN ('f','c')`); got != 2 {
		t.Fatalf("source children has %d f/c constraints, want 2 (premise broken)", got)
	}

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	// Rename both the FK owner and the referenced table: public.children ->
	// s_new.children_v2, public.parents -> s_new.parents_v2.
	cfg.RenameSchema = map[string]string{"public": "s_new"}
	cfg.RenameTable = map[string]string{
		"public.children": "children_v2",
		"public.parents":  "parents_v2",
	}

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 2 {
		t.Fatalf("plan has %d units, want 2 (children + parents)", len(p.Units))
	}
	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := manifest.NewManager(&cfg, src, dst, j)
	if err := runWithTimeout(t, mgr, p, 60*time.Second); err != nil {
		t.Fatalf("manager.Run: %v", err)
	}

	// Data correctness (the constraint pass must not touch the data path).
	if got := mustCount(t, dst, `SELECT count(*) FROM s_new.children_v2`); got != 5 {
		t.Fatalf("destination children_v2 = %d rows, want 5", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM s_new.parents_v2`); got != 3 {
		t.Fatalf("destination parents_v2 = %d rows, want 3", got)
	}

	// The CHECK constraint was restored on the renamed owner.
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_constraint c
		JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=c.connamespace
		WHERE n.nspname='s_new' AND t.relname='children_v2' AND c.contype='c' AND c.conname='ch_pos'`); got != 1 {
		t.Fatalf("destination CHECK ch_pos missing on s_new.children_v2")
	}
	// The FK was restored AND rewritten to the RENAMED referenced table
	// (s_new.parents_v2), not the source name (public.parents).
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_constraint c
		JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=c.connamespace
		JOIN pg_class rt ON rt.oid=c.confrelid JOIN pg_namespace rn ON rn.oid=rt.relnamespace
		WHERE n.nspname='s_new' AND t.relname='children_v2' AND c.contype='f'
		  AND c.conname='fk_child_parent' AND rn.nspname='s_new' AND rt.relname='parents_v2'`); got != 1 {
		t.Fatalf("destination FK fk_child_parent missing or not rewritten to s_new.parents_v2")
	}
	// The FK's ON DELETE action survived (CASCADE, confdeltype='c').
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_constraint c
		JOIN pg_class t ON t.oid=c.conrelid WHERE t.relname='children_v2' AND c.contype='f' AND c.confdeltype='c'`); got != 1 {
		t.Fatalf("destination FK ON DELETE CASCADE not preserved")
	}
}

// TestConstraintRestoreExternalRef — F-11: a foreign key that references a
// table OUTSIDE the migration set keeps its source name; when that table does
// not exist on the destination the ADD CONSTRAINT fails, but the pass is
// NON-FATAL (the data is already in place and verified): the run succeeds, the
// sibling CHECK constraint is still applied, and a constraint_restore_failed
// event is logged (F-04) so the operator can see the dangling reference.
func TestConstraintRestoreExternalRef(t *testing.T) {
	src, dst, _ := startPGPair(t, 5721, 5722)
	defer func() { src.Close(); dst.Close() }()

	// An anchor table that will NOT be migrated (it stays on the source only).
	mustExec(t, src, `CREATE TABLE public.anchors (id integer PRIMARY KEY)`)
	mustExec(t, src, `CREATE TABLE public.kids (
		id integer,
		anchor_id integer,
		CONSTRAINT ch_kid CHECK (id > 0),
		CONSTRAINT fk_kid_anchor FOREIGN KEY (anchor_id) REFERENCES public.anchors(id)
	)`)
	mustExec(t, src, `INSERT INTO public.anchors VALUES (1),(2)`)
	mustExec(t, src, `INSERT INTO public.kids VALUES (10,1),(11,2)`)

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	// Migrate ONLY public.kids (public.anchors is left behind).
	cfg.Tables = []string{"srcdb.public.kids"}

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 1 {
		t.Fatalf("plan has %d units, want 1 (kids only)", len(p.Units))
	}
	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := manifest.NewManager(&cfg, src, dst, j)
	// The run must SUCCEED even though the FK's referenced table is absent on
	// the destination (non-fatal constraint restore).
	if err := runWithTimeout(t, mgr, p, 60*time.Second); err != nil {
		t.Fatalf("manager.Run (external-ref FK must be non-fatal): %v", err)
	}

	// Data path unaffected.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.kids`); got != 2 {
		t.Fatalf("destination kids = %d rows, want 2", got)
	}
	// The sibling CHECK was still applied (the FK failure must not abort the
	// whole pass).
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_constraint c
		JOIN pg_class t ON t.oid=c.conrelid WHERE t.relname='kids' AND c.contype='c' AND c.conname='ch_kid'`); got != 1 {
		t.Fatalf("destination CHECK ch_kid missing (FK failure must not abort the pass)")
	}
	// The FK was NOT applied (its referenced table is absent on the dest).
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_constraint c
		JOIN pg_class t ON t.oid=c.conrelid WHERE t.relname='kids' AND c.contype='f'`); got != 0 {
		t.Fatalf("destination should have 0 FKs (referenced table absent), got %d", got)
	}
	// An external-ref diagnostic is recorded (F-04).
	evs := readEventLines(t, jdir)
	if !evsContain(evs, `"kind":"constraint_external_ref"`) {
		t.Fatalf("events.jsonl missing constraint_external_ref: %v", evs)
	}
	if !evsContain(evs, `"kind":"constraint_restore_failed"`) {
		t.Fatalf("events.jsonl missing constraint_restore_failed (dangling FK): %v", evs)
	}
}

// TestDestinationPKIndexRestored — F-17 regression: a source table with a
// PRIMARY KEY is copied end-to-end; the destination table must carry the
// rebuilt unique index on the key (the F-17 background index build). This is
// the guard for the source.Indexes catalog-query bug (the outer
// `n.nspname = $1` read the TABLE name instead of the schema, so the query
// always returned zero indexes and the PK was never rebuilt — which in turn
// left a restored foreign key with no unique index to reference).
func TestDestinationPKIndexRestored(t *testing.T) {
	src, dst, _ := startPGPair(t, 5731, 5732)
	defer func() { src.Close(); dst.Close() }()

	mustExec(t, src, `CREATE TABLE public.parents (id integer PRIMARY KEY, code text)`)
	mustExec(t, src, `INSERT INTO public.parents VALUES (1,'a'),(2,'b')`)

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	p := buildPlan(t, cfg, src, dst)
	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := manifest.NewManager(&cfg, src, dst, j)
	if err := runWithTimeout(t, mgr, p, 60*time.Second); err != nil {
		t.Fatalf("manager.Run: %v", err)
	}

	// The destination table exists and the unique index on id was rebuilt.
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname='public' AND i.indrelid='public.parents'::regclass
		  AND i.indisunique`); got != 1 {
		t.Fatalf("destination parents has %d unique index on id, want 1 (F-17 PK rebuild)", got)
	}
	// Data intact.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.parents`); got != 2 {
		t.Fatalf("destination parents = %d rows, want 2", got)
	}
}
