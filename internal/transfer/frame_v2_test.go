package transfer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// TestFrameV2RoundTrip verifies a v2 frame round-trips with ver/flags/
// content_id intact (the new v2 fields).
func TestFrameV2RoundTrip(t *testing.T) {
	f := &Frame{JobID: 1, UnitID: 7, Seq: 42, ContentID: 3, Flags: FlagTrailer, Data: []byte("v2 payload")}
	b := f.Encode()
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Ver != frameVer2 {
		t.Errorf("ver = %d, want %d", got.Ver, frameVer2)
	}
	if got.ContentID != 3 {
		t.Errorf("content_id = %d, want 3", got.ContentID)
	}
	if got.Flags != FlagTrailer {
		t.Errorf("flags = %d, want %d", got.Flags, FlagTrailer)
	}
	if got.JobID != 1 || got.UnitID != 7 || got.Seq != 42 {
		t.Errorf("header mismatch: %+v", got)
	}
	if string(got.Data) != "v2 payload" {
		t.Errorf("data = %q", got.Data)
	}
}

// TestFrameV1BackCompat verifies a legacy v1 frame still decodes (backward
// compat reader) and that v1/v2 do not cross-talk (a v1 frame is not mistaken
// for v2 and vice versa).
func TestFrameV1BackCompat(t *testing.T) {
	f := &Frame{JobID: 10, UnitID: 5, Seq: 9, Data: []byte("legacy")}
	b := f.EncodeV1()
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if got.Ver != frameVer1 {
		t.Errorf("ver = %d, want %d", got.Ver, frameVer1)
	}
	if got.ContentID != 0 {
		t.Errorf("v1 content_id should default to 0, got %d", got.ContentID)
	}
	if string(got.Data) != "legacy" {
		t.Errorf("data = %q", got.Data)
	}
}

// TestFrameV2Corruption verifies a corrupted v2 payload is detected (ErrCorrupt).
func TestFrameV2Corruption(t *testing.T) {
	f := &Frame{JobID: 1, UnitID: 1, Seq: 1, ContentID: 0, Data: []byte("abcdef")}
	b := f.Encode()
	b[fixedHdrV2] ^= 0xFF // corrupt first payload byte
	if _, err := Decode(b); err != ErrCorrupt {
		t.Errorf("corrupted v2 frame: got %v, want ErrCorrupt", err)
	}
}

// TestTrailerRoundTrip verifies a Trailer control frame encodes/decodes with
// the correct digest and row count.
func TestTrailerRoundTrip(t *testing.T) {
	var sha [32]byte
	for i := range sha {
		sha[i] = byte(i)
	}
	tr := Trailer{SHA256: sha, Rows: 18234567}
	f := NewTrailerFrame(1, 5, 7, 2, tr)
	b := f.Encode()
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Flags&FlagTrailer == 0 {
		t.Fatal("FlagTrailer not set")
	}
	if got.ContentID != 2 {
		t.Errorf("content_id = %d, want 2", got.ContentID)
	}
	gotTr, err := got.Trailer()
	if err != nil {
		t.Fatalf("trailer: %v", err)
	}
	if gotTr != tr {
		t.Errorf("trailer mismatch: got %+v, want %+v", gotTr, tr)
	}
}

// TestTrailerWrongFrame verifies calling Trailer() on a data frame errors.
func TestTrailerWrongFrame(t *testing.T) {
	f := &Frame{JobID: 1, UnitID: 1, Seq: 1, Data: []byte("not a trailer")}
	if _, err := f.Trailer(); err == nil {
		t.Error("expected error for non-trailer frame")
	}
}

// TestFrameV2OverPipe verifies a v2 frame (with content_id + trailer) flows
// over a real pipe and is received correctly (the in-memory transport path).
func TestFrameV2OverPipe(t *testing.T) {
	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()
	src := NewSession(w, 0)
	dst := NewSession(r, 0)

	var sha [32]byte
	sha[0] = 0xEE
	tr := Trailer{SHA256: sha, Rows: 100}
	trailer := NewTrailerFrame(1, 5, 0, 1, tr)

	recv := make(chan *Frame, 1)
	errc := make(chan error, 1)
	go func() {
		got, err := dst.Receive(context.Background())
		if err != nil {
			errc <- err
			return
		}
		recv <- got
	}()
	if err := src.Send(trailer); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case err := <-errc:
		t.Fatalf("receive: %v", err)
	case got := <-recv:
		if got.ContentID != 1 {
			t.Errorf("content_id = %d, want 1", got.ContentID)
		}
		gotTr, err := got.Trailer()
		if err != nil || gotTr != tr {
			t.Errorf("trailer over pipe: got %+v err=%v", gotTr, err)
		}
	}
}

// TestFrameV2MixedStreamOverPipe streams a v2 data frame then a v2 trailer
// frame over a pipe and verifies the dest sees both in order with correct
// types (the §4.3 source->dest flow: data... then TRAILER).
func TestFrameV2MixedStreamOverPipe(t *testing.T) {
	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()
	src := NewSession(w, 0)
	dst := NewSession(r, 0)

	var sha [32]byte
	tr := Trailer{SHA256: sha, Rows: 5}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type got struct {
		f   *Frame
		err error
	}
	out := make(chan got, 2)
	go func() {
		for i := 0; i < 2; i++ {
			f, err := dst.Receive(ctx)
			out <- got{f, err}
			if err != nil {
				return
			}
		}
	}()
	_ = src.Send(&Frame{JobID: 1, UnitID: 1, Seq: 0, ContentID: 0, Data: []byte("data-bytes")})
	_ = src.Send(NewTrailerFrame(1, 1, 1, 0, tr))

	first := <-out
	second := <-out
	if first.err != nil {
		t.Fatalf("first: %v", first.err)
	}
	if first.f.Flags&FlagTrailer != 0 {
		t.Error("first frame should be data, not trailer")
	}
	if string(first.f.Data) != "data-bytes" {
		t.Errorf("first data = %q", first.f.Data)
	}
	if second.err != nil {
		t.Fatalf("second: %v", second.err)
	}
	if second.f.Flags&FlagTrailer == 0 {
		t.Error("second frame should be a trailer")
	}
	gotTr, err := second.f.Trailer()
	if err != nil || gotTr != tr {
		t.Errorf("second trailer: got %+v err=%v", gotTr, err)
	}
}

// TestReadFrameV1V2AutoDetect verifies ReadFrame auto-detects the version from
// the magic on a single pipe (a v1 frame followed by a v2 frame).
func TestReadFrameV1V2AutoDetect(t *testing.T) {
	var buf bytes.Buffer
	// v1 frame
	buf.Write((&Frame{JobID: 1, UnitID: 1, Seq: 1, Data: []byte("v1")}).EncodeV1())
	// v2 frame
	buf.Write((&Frame{JobID: 2, UnitID: 2, Seq: 2, ContentID: 4, Data: []byte("v2")}).Encode())

	p := &slicePipe{b: buf.Bytes()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f1, err := ReadFrame(ctx, p)
	if err != nil {
		t.Fatalf("read v1: %v", err)
	}
	if f1.Ver != frameVer1 || string(f1.Data) != "v1" {
		t.Errorf("v1 frame: %+v", f1)
	}
	f2, err := ReadFrame(ctx, p)
	if err != nil {
		t.Fatalf("read v2: %v", err)
	}
	if f2.Ver != frameVer2 || f2.ContentID != 4 || string(f2.Data) != "v2" {
		t.Errorf("v2 frame: %+v", f2)
	}
}

// slicePipe is a Pipe over a byte slice (reader end).
type slicePipe struct {
	b []byte
}

func (s *slicePipe) Read(p []byte) (int, error) {
	if len(s.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.b)
	s.b = s.b[n:]
	return n, nil
}
func (s *slicePipe) Write(p []byte) (int, error) { return len(p), nil }
func (s *slicePipe) Close() error                 { return nil }

var _ = errors.Is
