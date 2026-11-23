package segstream

import (
	"strings"
	"testing"
)

// FuzzParseDestAddrs covers the destination address book string the
// controller embeds into the ON SEGMENT program command. It is built from
// configuration values, but any parsing bug here (index math, empty-entry
// handling, port extraction) must surface as an error, never a panic.
func FuzzParseDestAddrs(f *testing.F) {
	f.Add("127.0.0.1:9001,127.0.0.1:9002", 2)
	f.Add("  10.0.0.1:5432  , 10.0.0.2:5432 ", 2)
	f.Add("", 1)
	f.Add("a", 3)
	f.Add(strings.Repeat("x,", 40)+"y", 83)
	f.Fuzz(func(t *testing.T, s string, D int) {
		// D comes from the kernel segment count in production; bound it so
		// the fuzzer explores entry-mismatch paths without spending its
		// budget on huge D values.
		if D < 0 {
			D = 0
		}
		if D > 1000 {
			D = 1000
		}
		addrs, err := ParseDestAddrs(s, D)
		if err == nil {
			if len(addrs) != D {
				t.Fatalf("ParseDestAddrs returned %d addresses for D=%d", len(addrs), D)
			}
			for i, a := range addrs {
				if a == "" {
					t.Fatalf("empty address at %d in %q", i, s)
				}
			}
		}
	})
}
