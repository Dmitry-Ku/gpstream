// Package source implements the source agent (FR-01/§4.1): it streams the
// logical data of a table off the source cluster via `COPY ... TO STDOUT` and
// hands the bytes to the stream, without touching local storage in the middle
// (FR-01). It also computes a content checksum (FR-02) used to verify a unit
// after the copy.
//
// The agent talks to the source through pg.StreamingQuerier, so the same code
// works against a live Greenplum/PostgreSQL cluster and can be exercised in
// integration tests against a real PostgreSQL server.
package source

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Agent is the source-side agent for one cluster.
type Agent struct {
	q pg.StreamingQuerier
}

// New creates a source agent over a streaming connection to the source
// cluster (FR-01).
func New(q pg.StreamingQuerier) *Agent {
	return &Agent{q: q}
}

// Columns reads the faithful column definitions of schema.table from the
// source cluster (FR-03): column name, the type expression from
// format_type(atttypid, atttypmod), nullability and default. The destination
// uses these to (re)create the table, so the schema is copied faithfully
// across clusters rather than guessed.
func (a *Agent) Columns(ctx context.Context, schema, table string) ([]pg.Column, error) {
	rows, err := a.q.Query(ctx,
		`SELECT a.attname,
		        format_type(a.atttypid, a.atttypmod),
		        a.atttypid,
		        NOT a.attnotnull,
		        pg_get_expr(d.adbin, d.adrelid)
		 FROM pg_attribute a
		 LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		 WHERE a.attrelid = (SELECT c.oid FROM pg_class c
		                     JOIN pg_namespace n ON n.oid = c.relnamespace
		                     WHERE c.relname = $1 AND n.nspname = $2)
		   AND a.attnum > 0 AND NOT a.attisdropped
		 ORDER BY a.attnum`,
		table, schema)
	if err != nil {
		return nil, err
	}
	out := make([]pg.Column, 0, len(rows))
	for _, r := range rows {
		if len(r) < 5 {
			continue
		}
		out = append(out, pg.Column{
			Name:     toStr(r[0]),
			Type:     toStr(r[1]),
			Oid:      toUint32(r[2]),
			Nullable: toBool(r[3]),
			Default:  toStr(r[4]),
		})
	}
	return out, nil
}

// Distribution reads the table's Greenplum distribution policy (F-11: the
// metadata phase must reproduce «типы, распределение, AO-параметры»). It
// degrades to pg.Distribution{} (Kind "") on a plain-PostgreSQL source or a
// read fault — the destination then uses its own default, which is the
// conservative, non-fatal choice (the data copy itself is unaffected).
//
// policytype: 'p' = hash/randomly (empty distkey => DISTRIBUTED RANDOMLY),
// 'r' = replicated (lab e2e, 27.09). The column is char(1) and pgx
// decodes it as a byte (112 = 'p'), so it is cast ::text in SQL to keep the
// comparison driver-type-agnostic. The catalog lives in pg_catalog on this
// GP6 build (unqualified name resolves); COALESCE with the gp_toolkit
// qualifier covers builds that expose it there. distkey is int2vector —
// unnest(... WITH ORDINALITY) preserves column order (array_position does
// not exist for int2vector on this fork). The keys are aggregated as text
// in SQL so the result stays driver-type-agnostic.
func (a *Agent) Distribution(ctx context.Context, schema, table string) pg.Distribution {
	oid, err := pg.Row(ctx, a.q,
		`SELECT COALESCE(to_regclass('gp_toolkit.gp_distribution_policy'), to_regclass('gp_distribution_policy'))`)
	if err != nil || oid == nil {
		return pg.Distribution{} // not Greenplum / catalog absent
	}
	rows, err := a.q.Query(ctx,
		`SELECT dp.policytype::text,
		        (SELECT string_agg(a.attname, ',' ORDER BY k.pos)
		         FROM unnest(dp.distkey::int2[]) WITH ORDINALITY AS k(attnum, pos)
		         JOIN pg_attribute a ON a.attrelid = dp.localoid AND a.attnum = k.attnum)
		 FROM gp_distribution_policy dp
		 JOIN pg_class c ON c.oid = dp.localoid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relname = $1 AND n.nspname = $2`,
		table, schema)
	if err != nil || len(rows) == 0 || len(rows[0]) < 2 {
		return pg.Distribution{}
	}
	var d pg.Distribution
	switch toStr(rows[0][0]) {
	case "r":
		d.Kind = "replicated"
	case "p":
		if keys := toStr(rows[0][1]); keys != "" {
			d.Kind = "hash"
			for _, k := range strings.Split(keys, ",") {
				if k != "" {
					d.Keys = append(d.Keys, k)
				}
			}
		} else {
			d.Kind = "randomly"
		}
	}
	return d
}

// toStr renders an any as a string ("" for nil).
func toStr(v any) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	default:
		return fmt.Sprint(s)
	}
}

// toBool renders an any as a bool.
func toBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	default:
		return false
	}
}

// toUint32 renders a value as a uint32 (0 otherwise). pgx decodes an OID as a
// uint32; a defensive numeric fallback keeps it driver-agnostic.
func toUint32(v any) uint32 {
	switch n := v.(type) {
	case uint32:
		return n
	case int32:
		return uint32(n)
	case int64:
		return uint32(n)
	case uint64:
		return uint32(n)
	default:
		return 0
	}
}

// copySQL returns a binary-format COPY that streams the table to STDOUT
// (FR-01/§4.1). The FORMAT binary keeps the transfer faithful to the logical
// row format and is transport-agnostic, so it is insensitive to differences in
// OS/kernel/glibc between the clusters (FR-16).
func copySQL(schema, table string) string {
	return fmt.Sprintf(`COPY %s.%s TO STDOUT (FORMAT binary)`, quoteIdent(schema), quoteIdent(table))
}

// CopyToStream streams the rows of schema.table into w (FR-01). It returns the
// number of rows copied. The bytes are the binary COPY payload; the caller
// (the job manager) frames/streams them to the destination agent.
func (a *Agent) CopyToStream(ctx context.Context, schema, table string, w io.Writer) (int64, error) {
	return a.q.ReadCopy(ctx, w, copySQL(schema, table))
}

// IndexDef is one index of a source table (F-17). It is the shared pg.IndexDef
// type (also used by the destination agent and the background index pool);
// Def is the verbatim CREATE INDEX definition from pg_get_indexdef and
// Unique/PK mark the index kind so the pool orders the rebuild: secondary
// indexes first, PK/unique last (TASK-SPEC F-17).
type IndexDef = pg.IndexDef

// Indexes returns the index definitions of the table on the source (FR-17).
// The background pool applies them to the destination after the swap, in
// priority order (secondary first, PK/unique last). A source table without
// indexes yields an empty (non-nil) slice.
func (a *Agent) Indexes(ctx context.Context, schema, table string) ([]IndexDef, error) {
	rows, err := a.q.Query(ctx,
		`SELECT c.relname, i.indisunique, i.indisprimary, pg_get_indexdef(c.oid)
		   FROM pg_index i
		   JOIN pg_class c ON c.oid = i.indexrelid
		   JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE n.nspname = $1 AND i.indrelid = (
				SELECT c2.oid FROM pg_class c2
				JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
				WHERE n2.nspname = $2 AND c2.relname = $1)
		  ORDER BY c.relname`,
		table, schema)
	if err != nil {
		return nil, fmt.Errorf("query indexes: %w", err)
	}
	out := make([]IndexDef, 0, len(rows))
	for _, r := range rows {
		if len(r) < 4 {
			continue
		}
		out = append(out, IndexDef{
			Name:   toStr(r[0]),
			Unique: toBool(r[1]),
			PK:     toBool(r[2]),
			Def:    toStr(r[3]),
		})
	}
	return out, nil
}

// Checksum returns a row-order-independent content digest of the binary COPY
// payload of schema.table (FR-02). Comparing the source and destination
// checksums verifies that the data arrived intact without re-copying the table
// (FR-01/§4.2). The digest is order-independent because the source (a
// distributed table, gathered in segment order) and the destination staging
// (a plain heap, insertion order) lay the same rows down in different orders —
// a raw-payload md5 would mismatch on identical data (E1 e2e, 27.09).
func (a *Agent) Checksum(ctx context.Context, schema, table string) (string, error) {
	var buf writeCloserBuf
	rows, err := a.q.ReadCopy(ctx, &buf, copySQL(schema, table))
	if err != nil {
		return "", err
	}
	if rows == 0 {
		return "0", nil
	}
	return transfer.ContentDigest(buf.data)
}

// writeCloserBuf is an io.Writer that buffers into a []byte. pgx.CopyTo writes
// the COPY payload to it; the checksum is then computed over the buffered bytes
// so the source and destination can compare a content digest (FR-02).
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
