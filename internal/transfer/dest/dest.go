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
// layout as src (FR-02). It uses the column definitions read from the source
// cluster (via source.Agent.Columns -> format_type) so the staging table is
// created faithfully across clusters (a CREATE TABLE ... LIKE would not work
// across clusters). It is a no-op if the staging table already exists
// (crash-recovery re-uses it), which is why it swallows an "already exists"
// error.
func (a *Agent) CreateStaging(ctx context.Context, schema string, cols []pg.Column, stagingName string) error {
	sql := `CREATE TABLE IF NOT EXISTS ` + quoteIdent(schema) + "." + quoteIdent(stagingName) + " ("
	for i, c := range cols {
		if i > 0 {
			sql += ", "
		}
		sql += quoteIdent(c.Name) + " " + c.Type
		if c.Default != "" {
			sql += " DEFAULT " + c.Default
		}
		if !c.Nullable {
			sql += " NOT NULL"
		}
	}
	sql += ")"
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
// yet (create-then-truncate/append). It is a no-op if the table already exists.
func (a *Agent) CreateTable(ctx context.Context, schema string, cols []pg.Column, table string) error {
	return a.CreateStaging(ctx, schema, cols, table)
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
	var buf writeCloserBuf
	rows, err := a.q.ReadCopy(ctx, &buf, copySQL(schema, stagingName))
	if err != nil {
		return false, 0, err
	}
	got := "0"
	if rows > 0 {
		got, err = transfer.ContentDigest(buf.data)
		if err != nil {
			return false, rows, fmt.Errorf("verify: digest: %w", err)
		}
	}
	if got != wantChecksum {
		return false, rows, fmt.Errorf("verify: checksum mismatch (want %s, got %s)", wantChecksum, got)
	}
	return true, rows, nil
}

// Swap atomically renames the staging table to its final name in the same
// schema (FR-02). Because the staging and final tables share a schema, the
// rename is a single statement and therefore atomic. If the final table
// already exists it is dropped first only when overwrite is set (FR-11);
// otherwise the swap fails (skip-existing default).
func (a *Agent) Swap(ctx context.Context, schema, table, stagingName string, overwrite bool) error {
	if overwrite {
		if _, err := a.q.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`,
			quoteIdent(schema), quoteIdent(table))); err != nil {
			return fmt.Errorf("swap: drop existing %s.%s: %w", schema, table, err)
		}
	}
	// Guard: if the final name exists and we are not overwriting, fail
	// (skip-existing default, FR-11) rather than silently clobbering.
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
	return err
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

// copySQL returns a binary-format COPY that streams the table to STDOUT
// (FR-01), used by Verify to re-derive the content checksum.
func copySQL(schema, table string) string {
	return fmt.Sprintf(`COPY %s.%s TO STDOUT (FORMAT binary)`, quoteIdent(schema), quoteIdent(table))
}

// isAlreadyExists reports whether err is a Postgres "already exists" error
// (SQLSTATE 42P01). It is a lenient string check so a resume can proceed
// without importing the pg error type.
func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for i := 0; i+len("42P01") <= len(s); i++ {
		if s[i:i+5] == "42P01" {
			return true
		}
	}
	return false
}

// writeCloserBuf buffers the COPY payload so a content checksum can be
// computed over it (FR-02).
type writeCloserBuf struct {
	data []byte
}

func (b *writeCloserBuf) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
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
