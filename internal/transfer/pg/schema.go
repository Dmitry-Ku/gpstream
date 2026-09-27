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
	// Nullable reports whether the column allows NULL.
	Nullable bool
	// Default is the column default expression (without the "DEFAULT"
	// keyword), or "" if none.
	Default string
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
