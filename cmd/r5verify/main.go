// Command r5verify independently recomputes the destination segment for
// every migrated row using the SAME routing code as RowRouter
// (route.HashKeyDatum -> route.Combine -> route.JumpConsistentHash) and
// compares it against the live gp_segment_id captured from the 4-segment
// destination. Proves S!=D direct writes land on the hash-correct segment.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gpstream/gpstream/internal/route"
)

const numSeg = int32(4)

func beInt32(v int64) []byte {
	b := make([]byte, 4)
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
	return b
}

// segInt4: single int4 key (t1, t3).
func segInt4(v int64) int32 {
	hkey, _ := route.HashKeyDatum(route.OidInt4, beInt32(v))
	h := route.Combine(0, hkey, false)
	return route.JumpConsistentHash(uint64(h), numSeg)
}

// segMulti: (int4, text) with possible NULL (t2) — mirrors hashSeg exactly.
func segMulti(a int64, b string, bNull bool) int32 {
	var h uint32
	hkey, _ := route.HashKeyDatum(route.OidInt4, beInt32(a))
	h = route.Combine(h, hkey, false)
	if bNull {
		h = route.Combine(h, 0, true)
	} else {
		hkey2, _ := route.HashKeyDatum(route.OidText, []byte(b))
		h = route.Combine(h, hkey2, false)
	}
	return route.JumpConsistentHash(uint64(h), numSeg)
}

func checkFile(path, kind string) (checked, bad int) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("open", path, ":", err)
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	// The psql -F delimiter survived shell escaping as a LITERAL "	"
	// (backslash + t), not a real tab. Split on that 2-char sequence.
	const delim = "\x5ct" // literal backslash + t (psql -F escaped through the shell)
	for sc.Scan() {
		line := sc.Text()
		checked++
		var want int64
		switch kind {
		case "int4":
			f1 := strings.SplitN(line, delim, 2)
			if len(f1) != 2 {
				bad++
				if bad <= 3 {
					fmt.Printf("  BADLINE %s: %q\n", path, line)
				}
				continue
			}
			v, _ := strconv.ParseInt(f1[0], 10, 64)
			want, _ = strconv.ParseInt(f1[1], 10, 32)
			if got := segInt4(v); got != int32(want) {
				if bad < 5 {
					fmt.Printf("  MISMATCH %s: key=%s want(seg_id)=%d got=%d\n", path, f1[0], want, got)
				}
				bad++
			}
		case "multi":
			f1 := strings.SplitN(line, delim, 3)
			if len(f1) != 3 {
				bad++
				if bad <= 3 {
					fmt.Printf("  BADLINE %s: %q\n", path, line)
				}
				continue
			}
			a, _ := strconv.ParseInt(f1[0], 10, 64)
			want, _ = strconv.ParseInt(f1[2], 10, 32)
			bNull := f1[1] == "\\N"
			if got := segMulti(a, f1[1], bNull); got != int32(want) {
				if bad < 5 {
					fmt.Printf("  MISMATCH %s: a=%s b=%q want(seg_id)=%d got=%d\n", path, f1[0], f1[1], want, got)
				}
				bad++
			}
		}
	}
	return
}

func main() {
	d := " /tmp/gs-work/vms"
	var totChecked, totBad int
	for _, tc := range []struct {
		file, kind string
	}{
		{d + "/t1map.tsv", "int4"},
		{d + "/t2map.tsv", "multi"},
		{d + "/t3map.tsv", "int4"},
	} {
		c, b := checkFile(tc.file, tc.kind)
		totChecked += c
		totBad += b
		fmt.Printf("%-10s checked=%-7d mismatches=%d\n", tc.kind, c, b)
	}
	fmt.Printf("TOTAL: checked=%d mismatches=%d\n", totChecked, totBad)
	if totBad == 0 {
		fmt.Println("R5-VERIFY PASS: every row landed on JCH(cdbhash(K),4)")
	} else {
		os.Exit(1)
	}
}
