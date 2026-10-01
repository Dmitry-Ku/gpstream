// Package dest implements the destination agent (FR-01/§4.1): it receives the
// logical data of a table (streamed from the source agent) and writes it into
// a *staging* table via `COPY ... FROM STDIN` (FR-02), verifies it (FR-02),
// and only then atomically renames the staging table to its final name
// (FR-02). Existing data is never clobbered without an explicit policy (FR-11).
//
// The agent talks to the destination through pg.StreamingQuerier, so the same
// code works against a live Greenplum/PostgreSQL cluster and can be exercised
// in integration tests against a real PostgreSQL server.
package dest

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Agent is the destination-side agent for one cluster.
type Agent struct {
	q pg.StreamingQuerier
}

// New creates a destination agent over a streaming connection to the
// destination cluster (FR-01).
func New(q pg.StreamingQuerier) *Agent {
	return &Agent{q: q}
}

// StagingName returns the staging table name for a unit (FR-02): data is
// written into `public._gs_<jobid>_<unit>` (created in the final table's
// schema) and only renamed to the final name after verification, so a crash
// leaves no half-written final table.
func StagingName(jobID string, unitID int64) string {
	return fmt.Sprintf("_gs_%s_%d", jobID, unitID)
}

// CreateStaging creates an empty staging table in schema with the same column
// layout and — on a Greenplum source — the same distribution policy as src
// (FR-02; F-11 metadata: 'table DDL (types, distribution, AO parameters)').
// It uses the column definitions read from the source cluster (via
// source.Agent.Columns -> format_type) so the staging table is created
// faithfully across clusters (a CREATE TABLE ... LIKE would not work across
// clusters). dist carries the source's distribution policy; a zero value (or
// a non-Greenplum source) omits the clause and the destination applies its
// own default. part carries the source table's partition spec (Fix 1): a
// non-empty spec makes the staging table a partitioned parent with the same
// leaf layout, so the destination re-routes the copied rows by ITS OWN
// collation. The distribution clause precedes the partition spec (GP6
// requires "DISTRIBUTED BY" before "PARTITION BY"; live-verified 29.09).
// It is a no-op if the staging table already exists (crash-recovery re-uses
// it), which is why it swallows an "already exists" error.
func (a *Agent) CreateStaging(ctx context.Context, schema string, cols []pg.Column, stagingName string, dist pg.Distribution, part pg.PartitionSpec) error {
	sql := `CREATE TABLE IF NOT EXISTS ` + quoteIdent(schema) + "." + quoteIdent(stagingName) + " ("
	for i, c := range cols {
		if i > 0 {
			sql += ", "
		}
		sql += quoteIdent(c.Name) + " " + c.Type
		// F-16: reproduce the source column's explicit COLLATE clause so the
		// destination column keeps the source's collation semantics (sorting
		// / indexes over string columns). An empty collation (database
		// default) emits nothing — the destination DB default applies, and a
		// DB-level difference is reported separately (FR-10/FR-16).
		if c.Collation != "" {
			sql += " COLLATE " + quoteIdent(c.Collation)
		}
		if c.Default != "" {
			sql += " DEFAULT " + c.Default
		}
		if !c.Nullable {
			sql += " NOT NULL"
		}
	}
	// F-11 metadata 'AO parameters': reproduce the source table's storage
	// options (appendonly, orientation, blocksize, compress, fillfactor, ...)
	// so an AO table stays AO on the destination (a plain heap otherwise).
	//
	// ORDER MATTERS (live GP6, 29.09 M5 e2e): WITH (...) must PRECEDE
	// DISTRIBUTED BY — the kernel rejects "DISTRIBUTED BY (id) WITH (...)"
	// with a syntax error at "WITH". Same rule as PARTITION BY (Fix 1):
	// CREATE TABLE (cols) WITH (...) DISTRIBUTED BY (...) PARTITION BY (...).
	// A fake-conn test cannot catch this (it records, does not parse), so a
	// DDL-order assertion guards it below.
	sql += ")"
	if clause := pg.StorageOptionsSQL(dist.Reopts); clause != "" {
		sql += " " + clause
	}
	sql += dist.SQLClause()
	if part.Spec != "" {
		sql += " " + part.Spec
	}
	_, err := a.q.Exec(ctx, sql)
	if err != nil {
		// "relation already exists" is fine on resume; anything else is fatal.
		if isAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create staging %s.%s: %w", schema, stagingName, err)
	}
	return nil
}

// DropStaging removes a staging table (cleanup / rollback on a failed
// verify, FR-02/FR-07).
func (a *Agent) DropStaging(ctx context.Context, schema, stagingName string) error {
	_, err := a.q.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`,
		quoteIdent(schema), quoteIdent(stagingName)))
	return err
}

// DropTable removes a final (non-staging) table from the destination
// (FR-02 cleanup: a renamed-in or newly created table that gpstream owns
// must be removable to restore the pre-operation state). Idempotent.
func (a *Agent) DropTable(ctx context.Context, schema, table string) error {
	_, err := a.q.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`,
		quoteIdent(schema), quoteIdent(table)))
	return err
}

// DropSchemaIfEmpty removes a schema that gpstream created, but ONLY when it
// holds no user objects left (FR-02 cleanup: 'created schemas' are
// removable, pre-existing schemas with other data are never touched).
// Idempotent: a non-empty or absent schema is a no-op, not an error.
func (a *Agent) DropSchemaIfEmpty(ctx context.Context, schema string) error {
	if schema == "" || schema == "public" {
		return nil // never drop public: it pre-exists on every cluster
	}
	rows, err := a.q.Query(ctx,
		`SELECT count(*) FROM pg_class c
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1
		   AND c.relkind IN ('r','p','v','m','S','i','f')`, schema)
	if err != nil {
		return err
	}
	if len(rows) > 0 && len(rows[0]) > 0 {
		if n := asInt(rows[0][0]); n > 0 {
			return nil // objects remain: leave the schema in place
		}
	}
	_, err = a.q.Exec(ctx, `DROP SCHEMA IF EXISTS `+quoteIdent(schema))
	return err
}

// asInt converts a pgx-decoded count cell (int64/uint64/...) to int.
func asInt(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case int32:
		return int(n)
	case int:
		return n
	case uint64:
		return int(n)
	case float64:
		return int(n)
	case []byte:
		var m int
		fmt.Sscanf(string(n), "%d", &m)
		return m
	default:
		return 0
	}
}

// Truncate empties the final table in place (FR-11 --truncate). Unlike Swap
// (which renames a staging table over the final name), Truncate keeps the
// table's identity and just clears its rows so the source data can be copied
// into it; the table then holds exactly the source data, so a content
// checksum verifies it the same way as an overwrite.
func (a *Agent) Truncate(ctx context.Context, schema, table string) error {
	_, err := a.q.Exec(ctx, fmt.Sprintf(`TRUNCATE %s.%s`, quoteIdent(schema), quoteIdent(table)))
	return err
}

// CreateTable creates the final table from the source's column layout (FR-11).
// Used by the truncate/append paths when the destination table does not exist
// yet (create-then-truncate/append). part is the partition spec (Fix 1), ""
// for a plain table. It is a no-op if the table already exists.
func (a *Agent) CreateTable(ctx context.Context, schema string, cols []pg.Column, table string, dist pg.Distribution, part pg.PartitionSpec) error {
	return a.CreateStaging(ctx, schema, cols, table, dist, part)
}

// CopyIntoStream writes the streamed binary-COPY payload r into the staging
// table (FR-01). It returns the number of rows written. The staging table is
// fully qualified as <schema>.<stagingName>.
func (a *Agent) CopyIntoStream(ctx context.Context, schema, stagingName string, r io.Reader) (int64, error) {
	sql := fmt.Sprintf(`COPY %s.%s FROM STDIN (FORMAT binary)`,
		quoteIdent(schema), quoteIdent(stagingName))
	return a.q.WriteCopy(ctx, r, sql)
}

// Verify re-derives the content checksum of the staging table and compares it
// to the expected source checksum (FR-02). A mismatch fails the unit so it is
// re-copied rather than swapped (FR-07). The digest is row-order-independent
// (transfer.ContentDigest): the destination staging (a plain heap) lays the
// same rows down in a different order than the distributed source, so a
// raw-payload md5 would mismatch on identical data (E1 e2e, 27.09). It returns
// the row count too (FR-02).
func (a *Agent) Verify(ctx context.Context, schema, stagingName, wantChecksum string) (bool, int64, error) {
	d := transfer.NewContentDigest()
	rows, err := a.q.ReadCopy(ctx, d, copySQL(schema, stagingName))
	if err != nil {
		return false, 0, err
	}
	got := "0"
	if rows > 0 {
		got, err = d.Sum()
		if err != nil {
			return false, rows, fmt.Errorf("verify: digest: %w", err)
		}
	}
	if got != wantChecksum {
		return false, rows, fmt.Errorf("verify: checksum mismatch (want %s, got %s)", wantChecksum, got)
	}
	return true, rows, nil
}

// Checksum re-derives the content checksum of a final destination table
// (FR-10 §8: --overwrite checksum-skip). It mirrors source.Checksum — the same
// binary COPY payload digest (transfer.ContentDigest), which is
// row-order-independent, so a table whose data is already identical on the
// destination produces the same digest as the source. An empty table is
// "0" (matching source.Checksum) so two empty tables compare equal.
func (a *Agent) Checksum(ctx context.Context, schema, table string) (string, error) {
	d := transfer.NewContentDigest()
	rows, err := a.q.ReadCopy(ctx, d, copySQL(schema, table))
	if err != nil {
		return "", err
	}
	if rows == 0 {
		return "0", nil
	}
	return d.Sum()
}

// toStr converts a catalog value (any) to a string, tolerating the []byte
// form some drivers return.
func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(v)
}

// dropDependentFKs removes the foreign-key constraints on OTHER destination
// tables that reference schema.table. It is called by Swap before the
// overwrite DROP so the drop cannot be blocked by a pg_depend entry (2BP01).
//
// A foreign key is a dependency on the *referenced* table: con.confrelid is
// the referenced table's oid, con.conrelid is the referencing (child) table
// that carries the constraint. We enumerate every 'f' constraint whose
// confrelid is schema.table and drop it from its child table. Self-
// referencing FKs (the child is the table itself) are included and harmless
// — the table is being dropped anyway, and removing them first keeps the
// drop deterministic regardless of worker order.
//
// The drop is idempotent and lossless for the migration set: the manager's
// restoreConstraints re-applies the source FK definitions after the data
// path (manager.go). An FK owned by a table OUTSIDE the migration set is not
// re-applied (its referenced table's oid changed) — matching DROP ... CASCADE
// semantics for the constraint, which is the operator's explicit choice when
// opting into --overwrite.
func (a *Agent) dropDependentFKs(ctx context.Context, schema, table string) error {
	rows, err := a.q.Query(ctx,
		`SELECT n.nspname, c.relname, con.conname
		   FROM pg_constraint con
		   JOIN pg_class ref ON ref.oid = con.confrelid
		   JOIN pg_namespace rns ON rns.oid = ref.relnamespace
		   JOIN pg_class c ON c.oid = con.conrelid
		   JOIN pg_namespace n ON n.oid = con.connamespace
		  WHERE rns.nspname = $1 AND ref.relname = $2 AND con.contype = 'f'
		  ORDER BY n.nspname, c.relname, con.conname`,
		schema, table)
	if err != nil {
		// A catalog fault: do not cascade silently. Treat as "no dependent FKs"
		// and let the DROP itself surface a clear error if one actually blocks
		// (consistent with constraintExists / sequenceExists).
		return nil
	}
	for _, r := range rows {
		if len(r) < 3 {
			continue
		}
		childSchema, childTable, conName := toStr(r[0]), toStr(r[1]), toStr(r[2])
		if _, err := a.q.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s.%s DROP CONSTRAINT %s`,
			quoteIdent(childSchema), quoteIdent(childTable), quoteIdent(conName))); err != nil {
			return fmt.Errorf("drop constraint %s on %s.%s: %w", conName, childSchema, childTable, err)
		}
	}
	return nil
}

// Swap atomically renames the staging table to its final name in the same
// schema (FR-02). Because the staging and final tables share a schema, the
// rename is a single statement and therefore atomic. If the final table
// already exists it is dropped first only when overwrite is set (FR-11);
// otherwise the swap fails (stop default).
func (a *Agent) Swap(ctx context.Context, schema, table, stagingName string, overwrite bool) error {
	if overwrite {
		// The destination table may be referenced by foreign keys that live
		// on OTHER tables (a prior run's restoreConstraints, or an operator-
		// created table). GP6 does not enforce FKs in DML, but DROP honours
		// pg_depend: dropping a referenced table while a referrer still exists
		// fails with 2BP01. With parallel workers the swap order is
		// non-deterministic, so this is handled per-drop (not by ordering).
		// We drop the dependent FK constraints first — surgically, since a
		// blanket `DROP ... CASCADE` would also destroy operator views and
		// other dependent objects. restoreConstraints re-applies the source
		// FK set after the data path, so this is lossless for the migrated
		// tables (F-11).
		if err := a.dropDependentFKs(ctx, schema, table); err != nil {
			return fmt.Errorf("swap: drop dependent FKs of %s.%s: %w", schema, table, err)
		}
		if _, err := a.q.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`,
			quoteIdent(schema), quoteIdent(table))); err != nil {
			return fmt.Errorf("swap: drop existing %s.%s: %w", schema, table, err)
		}
	}
	// Guard: if the final name exists and we are not overwriting, fail
	// (stop default, FR-11) rather than silently clobbering.
	if !overwrite {
		if a.exists(ctx, schema, table) {
			return fmt.Errorf("swap: target %s.%s exists and overwrite is not set", schema, table)
		}
	}
	_, err := a.q.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s.%s RENAME TO %s`,
		quoteIdent(schema), quoteIdent(stagingName), quoteIdent(table)))
	if err != nil {
		return fmt.Errorf("swap: rename %s to %s.%s: %w", stagingName, schema, table, err)
	}
	return nil
}

// Exists reports whether schema.table exists in the destination.
func (a *Agent) Exists(ctx context.Context, schema, table string) bool {
	return a.exists(ctx, schema, table)
}

func (a *Agent) exists(ctx context.Context, schema, table string) bool {
	rows, err := a.q.Query(ctx,
		`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE c.relname=$1 AND n.nspname=$2 AND c.relkind IN ('r','p')`,
		table, schema)
	if err != nil || len(rows) == 0 {
		return false
	}
	return true
}

// EnsureSchema creates the destination schema if it does not exist (FR-11).
func (a *Agent) EnsureSchema(ctx context.Context, schema string) error {
	if schema == "" {
		return nil
	}
	_, err := a.q.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(schema)))
	if err == nil {
		return nil
	}
	// Concurrency: CREATE SCHEMA IF NOT EXISTS is NOT race-free on the target
	// kernel (PG 9.4 / GP6): two parallel workers creating the same schema
	// both pass the existence check, and the loser hits the pg_namespace
	// unique constraint (SQLSTATE 23505) instead of a no-op. A duplicate-key
	// on the namespace name means "someone else created it first" — that is
	// an idempotent success, not a fault (FR-02/FR-03: parallel workers,
	// lab e2e 29.09).
	s := err.Error()
	if strings.Contains(s, "23505") && strings.Contains(s, "pg_namespace_nspname_index") {
		return nil
	}
	return err
}

// CreateSequence recreates a source sequence on the destination (F-11
// metadata phase: 'sequences'). name is the sequence's
// schema-qualified source name; the destination table's serial column
// default was copied verbatim (nextval('<name>'::regclass)), so the
// sequence must be created under the SAME name for the default to resolve.
//
// The DDL is the standard serial-column sequence: `serial` is sugar for a
// sequence with START 1 / INCREMENT 1 / NO MIN/MAX / CACHE 1, so
// recreating it with those parameters reproduces the source sequence's
// parameters (its VALUE is resynchronised separately by SetSequenceValue
// post-load). CREATE SEQUENCE IF NOT EXISTS does not exist on the target
// kernel (PostgreSQL 9.4 / Greenplum 6), so idempotency is achieved by
// checking existence first and swallowing a 42P07 race (FR-07 resume): a
// sequence that survived a crashed run is left as-is.
func (a *Agent) CreateSequence(ctx context.Context, name string) error {
	schema, base := splitQualifiedName(name)
	if a.sequenceExists(ctx, schema, base) {
		return nil // resume: the sequence survived a crashed run
	}
	sql := fmt.Sprintf(`CREATE SEQUENCE %s.%s START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1`,
		quoteIdent(schema), quoteIdent(base))
	if _, err := a.q.Exec(ctx, sql); err != nil {
		if isAlreadyExists(err) {
			return nil // a concurrent resume created it between check and create
		}
		return fmt.Errorf("create sequence %s: %w", name, err)
	}
	return nil
}

// SetSequenceValue resynchronises a destination sequence to the value the
// source sequence held at copy time (F-11: 'values are synchronised
// post-load'). setval(seq, last_value, true) makes the NEXT nextval
// return last_value+1 — matching the source's state after a load. It runs
// AFTER the table swap, so it never touches a staging table, and it is
// idempotent (a resume re-applies the same value).
//
// The sequence is passed as a PARAMETER cast to regclass, not as a bare
// schema-qualified identifier: a bare `schema.seq` is not a valid regclass
// function argument (Postgres parses it as a field selection -> "missing
// FROM-clause entry for table schema"), so the qualified name must be a
// string literal / parameter.
func (a *Agent) SetSequenceValue(ctx context.Context, schema, seq string, last int64) error {
	_, err := a.q.Exec(ctx, `SELECT setval($1::regclass, $2, true)`, schema+"."+seq, last)
	return err
}

// sequenceExists reports whether a sequence named seq exists in schema.
func (a *Agent) sequenceExists(ctx context.Context, schema, seq string) bool {
	rows, err := a.q.Query(ctx,
		`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE c.relname=$1 AND n.nspname=$2 AND c.relkind='S'`,
		seq, schema)
	if err != nil || len(rows) == 0 {
		return false
	}
	return true
}

// splitQualifiedName splits "schema.seq" into (schema, seq); a name without
// a dot is treated as (public, name).
func splitQualifiedName(name string) (schema, base string) {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "public", name
}

// CreateIndexes applies the source index definitions to schema.table on the
// destination (F-17). It is called by the background index pool AFTER the
// staging table has been swapped to its final name, so the table already holds
// the copied data and the indexes are built over it.
//
// Each definition from the source (pg_get_indexdef) references the SOURCE
// schema/table names; for a rename (FR-11) they are rewritten to the
// destination names before the statement is issued. The index name itself is
// kept verbatim, so applying the same definition twice is idempotent: the
// second pass detects the existing index and falls back to REINDEX (which
// also makes a resume after a crash in the index phase safe — no duplicates,
// F-17). Definitions are applied in the order the caller provides them; the
// pool passes secondary indexes first and PK/unique last (F-17).
func (a *Agent) CreateIndexes(ctx context.Context, schema, table, srcSchema, srcTable string, defs []pg.IndexDef) error {
	for _, d := range defs {
		def := d.Def
		// Rewrite the source names to the destination names (no-op when the
		// unit is not renamed, i.e. src == dst).
		if srcSchema != schema || srcTable != table {
			// pg_get_indexdef emits the names as written in the source catalog
			// (usually unquoted); rewrite both the plain and the quoted form so
			// a rename (FR-11) is honoured either way.
			def = strings.ReplaceAll(def, srcSchema+"."+srcTable, schema+"."+table)
			def = strings.ReplaceAll(def, quoteIdent(srcSchema)+"."+quoteIdent(srcTable),
				quoteIdent(schema)+"."+quoteIdent(table))
		}
		if err := a.createOneIndex(ctx, schema, table, d.Name, def); err != nil {
			return fmt.Errorf("create index %s on %s.%s: %w", d.Name, schema, table, err)
		}
	}
	return nil
}

// createOneIndex applies a single index definition, idempotently (F-17). It
// first checks whether the index already exists; if so it REINDEXes it by name
// (the index exists but its data may be stale after a crash mid-build).
// Otherwise it issues the (rewritten) CREATE INDEX definition.
func (a *Agent) createOneIndex(ctx context.Context, schema, table, name, def string) error {
	if a.indexExists(ctx, schema, name) {
		_, err := a.q.Exec(ctx, fmt.Sprintf(`REINDEX INDEX %s`, quoteIdent(schema)+"."+quoteIdent(name)))
		if err != nil {
			return fmt.Errorf("reindex existing %s.%s: %w", schema, name, err)
		}
		return nil
	}
	_, err := a.q.Exec(ctx, def)
	return err
}

// indexExists reports whether an index named name exists in schema.
func (a *Agent) indexExists(ctx context.Context, schema, name string) bool {
	rows, err := a.q.Query(ctx,
		`SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE c.relname=$1 AND n.nspname=$2 AND c.relkind='i'`,
		name, schema)
	if err != nil || len(rows) == 0 {
		return false
	}
	return true
}

// Constraints restores table-level constraints (foreign keys and user CHECK
// constraints, F-11) on the destination table. The caller (the manager's
// post-swap restore pass) invokes it AFTER the data swap and after the FR-17
// index build drains, with definitions whose referenced table names are
// already rewritten to the destination — a foreign key must reference a
// destination table whose unique index is in place, and a restored constraint
// creates a pg_depend entry that would otherwise block a later re-swap (DROP)
// of the referenced table.
//
// Each definition is applied idempotently: a constraint with the same name
// already on the table (a resume re-run) is skipped, so the pass is safe to
// repeat. A fault is returned to the caller, which logs it as a non-fatal
// event (F-04): the data is already in place and verified, and a resume
// retries the pass.
func (a *Agent) Constraints(ctx context.Context, schema, table string, defs []pg.ConstraintDef) error {
	for _, d := range defs {
		if a.constraintExists(ctx, schema, table, d.Name) {
			continue // already restored (resume)
		}
		body := d.BuildDef()
		_, err := a.q.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s.%s ADD CONSTRAINT %s %s`,
			quoteIdent(schema), quoteIdent(table), quoteIdent(d.Name), body))
		if err != nil {
			return fmt.Errorf("add constraint %s on %s.%s: %w", d.Name, schema, table, err)
		}
	}
	return nil
}

// constraintExists reports whether schema.table already carries a constraint
// named name (the idempotency guard for a resume re-run).
func (a *Agent) constraintExists(ctx context.Context, schema, table, name string) bool {
	rows, err := a.q.Query(ctx,
		`SELECT 1 FROM pg_constraint con
		  JOIN pg_class c ON c.oid = con.conrelid
		  JOIN pg_namespace n ON n.oid = con.connamespace
		 WHERE n.nspname=$1 AND c.relname=$2 AND con.conname=$3`,
		schema, table, name)
	if err != nil || len(rows) == 0 {
		return false
	}
	return true
}

// copySQL returns a binary-format COPY that streams the table to STDOUT
// (FR-01), used by Verify to re-derive the content checksum.
func copySQL(schema, table string) string {
	return fmt.Sprintf(`COPY %s.%s TO STDOUT (FORMAT binary)`, quoteIdent(schema), quoteIdent(table))
}

// isAlreadyExists reports whether err is a Postgres "already exists" error
// (SQLSTATE 42P07). It is a lenient string check so a resume can proceed
// without importing the pg error type. (42P01 is undefined-table — a
// DIFFERENT error; matching it would swallow genuine DDL failures such as a
// serial default referencing a sequence that was not created yet.)
func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for i := 0; i+len("42P07") <= len(s); i++ {
		if s[i:i+5] == "42P07" {
			return true
		}
	}
	return false
}

// quoteIdent double-quotes a SQL identifier, escaping embedded quotes.
func quoteIdent(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '"')
	return string(out)
}
