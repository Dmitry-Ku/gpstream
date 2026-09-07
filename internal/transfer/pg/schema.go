package pg

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
