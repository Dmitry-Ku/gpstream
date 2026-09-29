package pg

import "strings"

// Column is a faithful column definition fetched from the source cluster and
// used to (re)create a table on the destination (FR-02/FR-03). The Type is the
// result of format_type(atttypid, atttypmod), so it is faithful across
// clusters (it is the source's own type rendering, not a guess).
type Column struct {
	// Name is the column name.
	Name string
	// Type is a SQL type expression (from format_type), e.g. "integer",
	// "text", "numeric(10,2)", "timestamp with time zone".
	Type string
	// Oid is the column's type OID (pg_attribute.atttypid), used by the R5
	// direct path to dispatch the distribution-key hash function by type.
	Oid uint32
	// Nullable reports whether the column allows NULL.
	Nullable bool
	// Default is the column default expression (without the "DEFAULT"
	// keyword), or "" if none.
	Default string
	// Collation is the column's explicit COLLATE name (F-16: mandatory
	// column-level collation comparison). It is the collation NAME
	// (pg_collation.collcollate), or "" for the database-default
	// collation — only explicitly collated columns carry it. The
	// destination DDL must reproduce it, and the planner compares
	// source vs destination per table (doctor/dry-run/final-report
	// warnings when they differ).
	Collation string
}

// Distribution is a source table's Greenplum distribution policy, read from
// gp_distribution_policy and reproduced in the destination DDL (F-11:
// metadata phase — «ДДЛ таблиц (типы, распределение, AO-параметры)»). Without
// it the destination would default to the first column (DISTRIBUTED BY on the
// leading column), silently changing the segment layout of multi-key tables
// (lab e2e, 27.09: source (a, b) -> dest (a)).
type Distribution struct {
	// Kind: "hash" | "randomly" | "replicated"; "" = not a Greenplum source
	// or the policy could not be read (destination uses its own default).
	Kind string
	// Keys: distribution column names for Kind == "hash", in distkey order.
	Keys []string
}

// SQLClause renders the Greenplum distribution clause for a CREATE TABLE
// statement; "" for a non-Greenplum/undefined policy.
func (d Distribution) SQLClause() string {
	switch d.Kind {
	case "hash":
		if len(d.Keys) == 0 {
			return ""
		}
		parts := make([]string, len(d.Keys))
		for i, k := range d.Keys {
			parts[i] = quoteIdent(k)
		}
		return " DISTRIBUTED BY (" + strings.Join(parts, ", ") + ")"
	case "randomly":
		return " DISTRIBUTED RANDOMLY"
	case "replicated":
		return " DISTRIBUTED REPLICATED"
	default:
		return ""
	}
}

// IndexDef is one index of a source table, shared by the source agent (which
// reads it via pg_get_indexdef) and the destination agent / background index
// pool (which applies it after the swap) (F-17).
type IndexDef struct {
	// Name is the index name.
	Name string
	// Unique marks a unique index.
	Unique bool
	// PK marks the primary-key index; the background pool rebuilds
	// secondary indexes first and PK/unique last (F-17).
	PK bool
	// Def is the verbatim CREATE INDEX definition from pg_get_indexdef,
	// referencing the SOURCE schema/table names.
	Def string
}

// ConstraintDef is one table-level constraint captured from the source and
// ConstraintDef is one table-level constraint captured from the source and
// restored on the destination (F-11 metadata phase: «по опции —
// ограничения»). Only foreign keys (Kind "f") and user CHECK constraints
// (Kind "c") are captured. Primary-key / unique constraints are NOT modeled
// here: F-17 already restores them as unique indexes (this fork does not
// support ALTER TABLE ... ADD CONSTRAINT ... USING INDEX). Partition
// bound-CHECKs and constraints inherited into a partition leaf are excluded
// at capture time (coninhcount = 0 and the table is not a partition leaf), so
// a partitioned parent's user CHECK is captured once and the destination's
// PARTITION BY clause recreates the bound constraints.
//
// A foreign key is captured STRUCTURALLY (LocalColumns/RefColumns/RefSchema/
// RefTable/DelAction/UpdAction) rather than as the verbatim
// pg_get_constraintdef string, because that string qualifies the referenced
// table by the CAPTURE session's search_path (verified on GP6: "REFERENCES
// parents(id)" when the referenced table is in search_path, "REFERENCES
// schema.parents(id)" otherwise) — a string rewrite would be search_path-
// dependent and could miss the rename. The structural fields are rewritten to
// the destination by the manager and re-rendered with schema-qualified names,
// which is deterministic. A CHECK carries its verbatim expression in Def (it
// references no other table, so no rewrite is needed).
type ConstraintDef struct {
	// Name is the constraint name (pg_constraint.conname).
	Name string
	// Kind is the constraint type: "f" (foreign key) or "c" (check).
	Kind string
	// Def is the verbatim check expression for Kind "c" (pg_get_constraintdef,
	// e.g. "CHECK ((id > 0))"). Empty for an FK (reconstructed from the
	// structural fields).
	Def string
	// LocalColumns are the FK's columns on the owning table, in conkey order
	// (comma-joined, quoted identifiers). Empty for a CHECK.
	LocalColumns string
	// RefSchema/RefTable are the SOURCE schema and table the FK references
	// (pg_constraint.confrelid). The manager rewrites them to the
	// destination via the plan's rename map. Empty for a CHECK.
	RefSchema string
	RefTable  string
	// RefColumns are the referenced columns, in confkey order (comma-joined,
	// quoted identifiers). Empty for a CHECK.
	RefColumns string
	// DelAction/UpdAction are the ON DELETE / ON UPDATE key actions as the
	// SQL keyword (CASCADE / SET NULL / SET DEFAULT / RESTRICT / NO ACTION);
	// "NO ACTION" when the source had the default (confdeltype 'n' /
	// confupdtype 'n'). Empty for a CHECK.
	DelAction string
	UpdAction string
}

// BuildDef renders the constraint's DDL body for "ALTER TABLE t ADD CONSTRAINT
// <name> <body>". For a CHECK it returns Def verbatim. For an FK it returns
// "FOREIGN KEY (locals) REFERENCES <refschema>.<reftable>(refcols) [ON DELETE
// ...] [ON UPDATE ...]" with the referenced table schema-qualified (deterministic,
// independent of any session search_path). The caller has already rewritten
// RefSchema/RefTable to the destination.
func (d ConstraintDef) BuildDef() string {
	if d.Kind != "f" {
		return d.Def
	}
	ref := ""
	if d.RefSchema != "" {
		ref = quoteIdent(d.RefSchema) + "." + quoteIdent(d.RefTable)
	} else {
		ref = quoteIdent(d.RefTable)
	}
	var b strings.Builder
	b.WriteString("FOREIGN KEY (")
	b.WriteString(d.LocalColumns)
	b.WriteString(") REFERENCES ")
	b.WriteString(ref)
	b.WriteString("(")
	b.WriteString(d.RefColumns)
	b.WriteString(")")
	if d.DelAction != "" && d.DelAction != "NO ACTION" {
		b.WriteString(" ON DELETE ")
		b.WriteString(d.DelAction)
	}
	if d.UpdAction != "" && d.UpdAction != "NO ACTION" {
		b.WriteString(" ON UPDATE ")
		b.WriteString(d.UpdAction)
	}
	return b.String()
}

// QuoteIdent is the exported form of quoteIdent, so other packages (the
// source agent) can render SQL identifiers for DDL reconstruction.
func QuoteIdent(s string) string {
	return quoteIdent(s)
}

// quoteIdent double-quotes a SQL identifier, escaping embedded quotes
// (shared by DDL builders in this package).
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
