package source

import "testing"

// TestReoptionsList guards reoptionsList against the driver's decode shapes
// for pg_class.reloptions (text[]). The live M5 e2e (29.09) exposed that
// pgx v5 hands a text[] back as []interface{} of strings — a shape the
// original switch did not handle, so an AO table's options silently
// disappeared and the table arrived on the destination as a plain heap.
func TestReoptionsList(t *testing.T) {
	want := []string{"appendonly=true", "orientation=row", "blocksize=32768"}
	cases := []struct {
		name string
		v    any
	}{
		{"pgx-v5 []any", []any{"appendonly=true", "orientation=row", "blocksize=32768"}},
		{"[]string", []string{"appendonly=true", "orientation=row", "blocksize=32768"}},
		{"{}-braces string", "{appendonly=true, orientation=row, blocksize=32768}"},
		{"plain string", "appendonly=true, orientation=row, blocksize=32768"},
		{"[]byte braces", []byte("{appendonly=true, orientation=row, blocksize=32768}")},
		{"[]-brackets string", "[appendonly=true orientation=row blocksize=32768]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := reoptionsList(c.v)
			if len(got) != len(want) {
				t.Fatalf("reoptionsList(%v) = %v, want %v", c.v, got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("reoptionsList(%v)[%d] = %q, want %q", c.v, i, got[i], want[i])
				}
			}
		})
	}
}

// TestReoptionsListNil verifies the empty/nil cases yield nil (no options).
func TestReoptionsListNil(t *testing.T) {
	for _, v := range []any{nil, "", "  ", []string{}} {
		if got := reoptionsList(v); got != nil {
			t.Fatalf("reoptionsList(%#v) = %v, want nil", v, got)
		}
	}
}
