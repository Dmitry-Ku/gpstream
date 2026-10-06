package transfer

import (
	"bytes"
	"testing"
)

// TestUnitBeginFrameRoundTrip verifies the UnitBegin control frame (P1c)
// carries an opaque UnitSpec payload through the v2 frame encoding intact.
func TestUnitBeginFrameRoundTrip(t *testing.T) {
	spec := []byte(`{"job_id":123,"schema":"public","table":"orders"}`)
	f := NewUnitBeginFrame(0xAB, 7, 0, spec)
	if !f.IsUnitBegin() {
		t.Fatal("NewUnitBeginFrame: IsUnitBegin false")
	}
	if f.ContentID != ContentCoordinator {
		t.Fatalf("content id = %d, want ContentCoordinator", f.ContentID)
	}
	b := f.Encode()
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.IsUnitBegin() {
		t.Fatal("decoded frame lost FlagUnitBegin")
	}
	if !bytes.Equal(got.Data, spec) {
		t.Fatalf("unit spec payload = %q, want %q", got.Data, spec)
	}
	if got.JobID != 0xAB || got.UnitID != 7 {
		t.Fatalf("ids = (%d,%d), want (0xAB,7)", got.JobID, got.UnitID)
	}
}

// TestPingPongFrameRoundTrip verifies the idle-watchdog control frames (P1c)
// encode/decode with empty Data and the right flag.
func TestPingPongFrameRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    *Frame
		want func(*Frame) bool
	}{
		{"ping", NewPingFrame(1, 2, 3), (*Frame).IsPing},
		{"pong", NewPongFrame(1, 2, 3), (*Frame).IsPong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.want(tc.f) {
				t.Fatalf("%s: flag not set on construction", tc.name)
			}
			if len(tc.f.Data) != 0 {
				t.Fatalf("%s: Data = %d bytes, want 0", tc.name, len(tc.f.Data))
			}
			got, err := Decode(tc.f.Encode())
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !tc.want(got) {
				t.Fatalf("%s: flag lost after round-trip", tc.name)
			}
		})
	}
}

// TestControlFrameFlagIsolation verifies the new control flags do not collide
// with FlagTrailer (a frame is exactly one kind of control frame).
func TestControlFrameFlagIsolation(t *testing.T) {
	flags := []struct {
		name string
		f    uint8
	}{
		{"trailer", FlagTrailer},
		{"unitbegin", FlagUnitBegin},
		{"ping", FlagPing},
		{"pong", FlagPong},
	}
	for i := range flags {
		for j := range flags {
			if i == j {
				continue
			}
			if flags[i].f&flags[j].f != 0 {
				t.Fatalf("%s & %s overlap (bit collision)", flags[i].name, flags[j].name)
			}
		}
	}
	// A Trailer frame must not be mistaken for a UnitBegin (and vice versa).
	tr := NewTrailerFrame(1, 2, 3, 4, Trailer{Rows: 5})
	if tr.IsUnitBegin() || tr.IsPing() || tr.IsPong() {
		t.Fatal("trailer frame has a non-trailer control flag set")
	}
	ub := NewUnitBeginFrame(1, 2, 3, []byte("x"))
	if _, terr := ub.Trailer(); terr == nil {
		t.Fatal("unitbegin frame should not parse as a trailer")
	}
}
