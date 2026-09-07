package compress

import (
	"bytes"
	"testing"
)

func roundTrip(t *testing.T, c *Compressor, d *Decompressor, in []byte) {
	t.Helper()
	enc := c.Encode(in)
	dec := d.Decode(enc)
	if !bytes.Equal(dec, in) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d", len(dec), len(in))
	}
}

func TestRoundTripNone(t *testing.T) {
	c := New(ModeNone, 3)
	d := NewDec(ModeNone)
	in := []byte("the quick brown fox")
	roundTrip(t, c, d, in)
	if !bytes.Equal(c.Encode(in), in) {
		t.Errorf("none mode should return input unchanged")
	}
}

func TestRoundTripZlib(t *testing.T) {
	c := New(ModeZlib, 3)
	d := NewDec(ModeZlib)
	in := bytes.Repeat([]byte("greenplum streaming "), 1000)
	enc := c.Encode(in)
	if len(enc) >= len(in) {
		t.Errorf("zlib did not compress: %d >= %d", len(enc), len(in))
	}
	roundTrip(t, c, d, in)
}

func TestRoundTripZstd(t *testing.T) {
	c := New(ModeZstd, 3)
	d := NewDec(ModeZstd)
	in := bytes.Repeat([]byte("gpstream zstd compression "), 5000)
	enc := c.Encode(in)
	if len(enc) >= len(in) {
		t.Errorf("zstd did not compress: %d >= %d", len(enc), len(in))
	}
	dec := d.Decode(enc)
	if !bytes.Equal(dec, in) {
		t.Errorf("zstd round-trip mismatch: got %d, want %d", len(dec), len(in))
	}
}

func TestZstdLevelRange(t *testing.T) {
	// Levels 1 and 19 must both round-trip.
	for _, lvl := range []int{1, 3, 9, 19} {
		c := New(ModeZstd, lvl)
		d := NewDec(ModeZstd)
		in := bytes.Repeat([]byte("level "), 2000)
		roundTrip(t, c, d, in)
	}
}

func TestParse(t *testing.T) {
	cases := []struct {
		in      string
		mode    Mode
		level   int
		wantErr bool
	}{
		{"", ModeNone, 3, false},
		{"none", ModeNone, 3, false},
		{"zlib", ModeZlib, 3, false},
		{"zstd", ModeZstd, 3, false},
		{"zstd:7", ModeZstd, 7, false},
		{"zstd:1", ModeZstd, 1, false},
		{"zstd:19", ModeZstd, 19, false},
		{"bogus", ModeNone, 3, true},
	}
	for _, c := range cases {
		m, l, err := Parse(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("Parse(%q): expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q): unexpected error %v", c.in, err)
			continue
		}
		if m != c.mode {
			t.Errorf("Parse(%q) mode = %v, want %v", c.in, m, c.mode)
		}
		if l != c.level {
			t.Errorf("Parse(%q) level = %d, want %d", c.in, l, c.level)
		}
	}
}
