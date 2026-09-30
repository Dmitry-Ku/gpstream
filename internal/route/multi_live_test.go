package route

// M1 (T4b prep): validation of GP-hash MULTI-ATTR + NULL on a live GPDB 6.30.1.
//
// Data: testdata/t02_multi.txt — 10000 rows of 'a|b|seg' from the live cluster A
// (vm1:5432): t02_multi (a int, b text) DISTRIBUTED BY (a, b), 2 primary.
// Generation is deterministic: i=0..9999, a=NULL when i%10==5 (else i),
// b=NULL when i%10==0 (else 'v'||i) — ~10% NULL in each column.
// Format: NULL -> empty field (no empty rows in the data, b is always 'v'+i).
//
// T0-2 covered only single-attr (int4, text); this test covers
// combining multiple attributes and NULL attributes (cdbhash.c:
// rotl1 + XOR only for non-NULL).
import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestRouteMultiAttrLiveCluster(t *testing.T) {
	f, err := os.Open("testdata/t02_multi.txt")
	if err != nil {
		t.Fatalf("open testdata: %v", err)
	}
	defer f.Close()

	var (
		nullA, nullB, bothNull int
		segCount               = [2]int32{}
		mismatches             int
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	n := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			t.Fatalf("line %d: bad format %q", n, line)
		}
		aS, bS, segS := parts[0], parts[1], parts[2]
		seg, err := strconv.ParseInt(segS, 10, 32)
		if err != nil {
			t.Fatalf("line %d: bad seg %q: %v", n, line, err)
		}
		var aBytes []byte
		var aNull bool
		if aS == "" {
			aNull = true
			nullA++
		} else {
			v, err := strconv.ParseInt(aS, 10, 32)
			if err != nil {
				t.Fatalf("line %d: bad a %q: %v", n, line, err)
			}
			aBytes = []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
		}
		var bBytes []byte
		var bNull bool
		if bS == "" {
			bNull = true
			nullB++
		} else {
			bBytes = []byte(bS)
		}
		if aNull && bNull {
			bothNull++
		}
		segCount[seg]++
		got := RouteMulti([][]byte{aBytes, bBytes}, []bool{aNull, bNull}, 2)
		if got != int32(seg) {
			if mismatches < 10 {
				t.Errorf("line %d a=%s b=%s: computed=%d actual=%d",
					n, aS, bS, got, seg)
			}
			mismatches++
		}
		n++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 10000 {
		t.Fatalf("expected 10000 rows, got %d", n)
	}
	if nullA < 900 || nullA > 1100 {
		t.Errorf("NULL-a count off: %d (want ~1000)", nullA)
	}
	if nullB < 900 || nullB > 1100 {
		t.Errorf("NULL-b count off: %d (want ~1000)", nullB)
	}
	if mismatches > 0 {
		t.Fatalf("%d/10000 mismatches", mismatches)
	}
	t.Logf("M1 PASS: 10000/10000 multi-attr (a int, b text) DISTRIBUTED BY (a,b), "+
		"NULL a=%d b=%d both=%d, seg0=%d seg1=%d",
		nullA, nullB, bothNull, segCount[0], segCount[1])
}
