package selftest

import (
	"context"
	"testing"
)

func TestRunAllPass(t *testing.T) {
	r := Run(context.Background())
	if !r.OK {
		for _, c := range r.Cases {
			if !c.OK {
				t.Errorf("case %s failed: %s", c.Name, c.Detail)
			}
		}
	}
}

func TestChecksumStable(t *testing.T) {
	if Checksum([]byte("a")) != Checksum([]byte("a")) {
		t.Fatal("checksum not stable")
	}
	if Checksum([]byte("a")) == Checksum([]byte("b")) {
		t.Fatal("checksum collision on different input")
	}
}

func TestLimiter(t *testing.T) {
	l := NewLimiter(100)
	if l.Reduced() {
		t.Fatal("should not be reduced initially")
	}
	l.Overload(35)
	if !l.Reduced() {
		t.Fatal("should reduce on iowait>30")
	}
	if l.cur != 100*0.75 {
		t.Errorf("expected 75, got %v", l.cur)
	}
	l.Clear()
	if !l.Restored() {
		t.Fatal("should restore after clear")
	}
}

func TestLimiterFloor(t *testing.T) {
	l := NewLimiter(2) // below floor 1? no, 2>1
	l.Overload(35)     // 2*0.75=1.5
	l.Overload(35)     // 1.5*0.75=1.125
	l.Overload(35)     // 1.125*0.75=0.84 -> floor 1
	if l.cur < l.floor {
		t.Errorf("floor violated: %v < %v", l.cur, l.floor)
	}
}

func TestStagingName(t *testing.T) {
	if StagingName("orders", "j1") != "_gs_j1_orders" {
		t.Fatalf("bad staging name: %s", StagingName("orders", "j1"))
	}
	if FinalName("_gs_j1_orders", "j1") != "orders" {
		t.Fatalf("bad final name: %s", FinalName("_gs_j1_orders", "j1"))
	}
	if FinalName("_gs_j1_orders", "") != "j1_orders" {
		t.Fatalf("final without jobid should strip only prefix: %s", FinalName("_gs_j1_orders", ""))
	}
}

func TestFrameCRC(t *testing.T) {
	f := Frame{JobID: 1, UnitID: 2, Seq: 3, Data: []byte("payload")}
	b, err := f.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got := Decode(b)
	if got == nil {
		t.Fatal("decode returned nil")
	}
	if got.Seq != 3 || string(got.Data) != "payload" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	// Corrupt -> crc mismatch -> nil.
	b[5] ^= 0xff
	if Decode(b) != nil {
		t.Fatal("corrupt frame decoded")
	}
	// Too short -> nil.
	if Decode([]byte{1, 2, 3}) != nil {
		t.Fatal("short frame decoded")
	}
}

func TestPlanHashDeterministic(t *testing.T) {
	a := planHash([]int{1, 2, 3}, "zstd", 10)
	b := planHash([]int{1, 2, 3}, "zstd", 10)
	c := planHash([]int{1, 2, 4}, "zstd", 10)
	if a != b {
		t.Fatal("plan hash not deterministic")
	}
	if a == c {
		t.Fatal("plan hash not sensitive to input")
	}
}
