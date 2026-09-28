package manifest

import "testing"

// TestQuoteIdentDSN guards the identifier quoting used by the R5 direct-path
// COPY statement: both ends must be quoted and embedded quotes doubled,
// otherwise the COPY statement is a syntax error (regression: the closing
// quote was once omitted, producing `COPY "public." _gs_… FROM STDIN`).
func TestQuoteIdentDSN(t *testing.T) {
	cases := []struct{ in, want string }{
		{"public", `"public"`},
		{`_gs_1e78dd6e3453cf1ba01307adf70e5d85_1`, `"_gs_1e78dd6e3453cf1ba01307adf70e5d85_1"`},
		{`we"ird`, `"we""ird"`},
		{"", `""`},
	}
	for _, c := range cases {
		if got := quoteIdentDSN(c.in); got != c.want {
			t.Errorf("quoteIdentDSN(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	// The assembled statement must be well-formed: schema.table both quoted.
	stmt := "COPY " + quoteIdentDSN("public") + "." + quoteIdentDSN("t1") + " FROM STDIN (FORMAT binary)"
	want := `COPY "public"."t1" FROM STDIN (FORMAT binary)`
	if stmt != want {
		t.Errorf("assembled statement = %q, want %q", stmt, want)
	}
}
