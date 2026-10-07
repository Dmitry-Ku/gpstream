package agent

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// TestPersistentChannelMultiUnit is the P1c unit acceptance: one connection
// carries MANY units (the persistent coordinator channel). Each unit is
// delimited by a UnitBegin frame (the spec) and a Trailer frame (the
// end-of-unit marker); the connection stays open between units. The payload
// is compressed (zstd, FR-12) and must round-trip byte-exact; the Trailer
// (raw digest acc + row count) must arrive with each unit.
func TestPersistentChannelMultiUnit(t *testing.T) {
	const units = 3
	a, b := net.Pipe() // a = sender (source), b = receiver (dest)
	defer a.Close()
	defer b.Close()

	comp := compress.New(compress.ModeZstd, 3)
	snd := NewPersistentSender(a, 0x1234)
	rcv := NewPersistentReceiver(b, 0x1234)

	// The source's raw digest accumulators (the P0 per-row XOR building
	// block) + row counts, per unit.
	wantAcc := make([][32]byte, units)
	wantRows := make([]int64, units)
	var wantData [][]byte
	for u := 0; u < units; u++ {
		payload := make([]byte, 100_000)
		for i := range payload {
			payload[i] = byte(u*7 + i%13) // compressible, unit-specific
		}
		wantData = append(wantData, payload)
		var acc [32]byte
		for i := range acc {
			acc[i] = byte(u + i)
		}
		wantAcc[u] = acc
		wantRows[u] = int64(1000 * (u + 1))
	}

	// The source sends all units over the ONE connection (sequentially, as
	// the strict worker pool does — streamMu serializes the data path).
	sndDone := make(chan error, 1)
	go func() {
		defer snd.Close()
		for u := 0; u < units; u++ {
			spec := UnitSpec{
				JobID: 0x1234, UnitID: uint32(u + 1),
				Schema: "public", Table: "t" + string(rune('0'+u)), Staging: "_gs_" + string(rune('0'+u)),
				Compress: "zstd:3", VerifyDigest: true,
			}
			if _, _, err := snd.SendUnit(context.Background(), spec, bytes.NewReader(wantData[u]), comp, wantAcc[u], wantRows[u]); err != nil {
				sndDone <- err
				return
			}
		}
		sndDone <- nil
	}()

	// The destination receives all units off the ONE connection.
	for u := 0; u < units; u++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		spec, data, err := rcv.ReceiveUnit(ctx)
		cancel()
		if err != nil {
			t.Fatalf("unit %d: receive: %v", u, err)
		}
		if spec.UnitID != uint32(u+1) || spec.Compress != "zstd:3" || spec.Schema != "public" {
			t.Fatalf("unit %d: spec = %+v (wrong spec on the persistent channel)", u, spec)
		}
		// Drain the unit's data to the Trailer frame (io.EOF).
		var got bytes.Buffer
		if _, err := copyFromReader(&got, data); err != nil {
			t.Fatalf("unit %d: read data: %v", u, err)
		}
		if !bytes.Equal(got.Bytes(), wantData[u]) {
			t.Fatalf("unit %d: payload mismatch (got %d bytes, want %d) — compression round-trip broken",
				u, got.Len(), len(wantData[u]))
		}
		// The Trailer (end-of-unit marker) must carry the source's digest acc + rows.
		if data.Trailer().Digest != wantAcc[u] || data.Trailer().Rows != wantRows[u] {
			t.Fatalf("unit %d: trailer = (acc=%x rows=%d), want (acc=%x rows=%d)",
				u, data.Trailer().Digest, data.Trailer().Rows, wantAcc[u], wantRows[u])
		}
	}
	if err := <-sndDone; err != nil {
		t.Fatalf("sender: %v", err)
	}
}

// TestPersistentChannelPingTolerance verifies the idle-watchdog: a Ping sent
// between units is answered with a Pong and does not disturb the next unit's
// UnitBegin (the receiver skips it).
func TestPersistentChannelPingTolerance(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	snd := NewPersistentSender(a, 1)
	rcv := NewPersistentReceiver(b, 1)

	done := make(chan error, 1)
	go func() {
		defer snd.Close()
		spec := UnitSpec{JobID: 1, UnitID: 9, Schema: "public", Table: "t", Staging: "_s", Compress: "none"}
		if _, _, err := snd.SendUnit(context.Background(), spec, bytes.NewReader([]byte("hello")), compress.New(compress.ModeNone, 0), [32]byte{}, 1); err != nil {
			done <- err
			return
		}
		// An idle ping AFTER the unit's trailer (the channel is now idle).
		snd.mu.Lock()
		_ = transfer.WriteFrame(a, transfer.NewPingFrame(1, 0, snd.seq))
		snd.seq++
		snd.mu.Unlock()
		done <- nil
	}()

	// Receive the unit (drains through its trailer).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	spec, data, err := rcv.ReceiveUnit(ctx)
	cancel()
	if err != nil || spec.UnitID != 9 {
		t.Fatalf("unit: spec=%+v err=%v", spec, err)
	}
	var got bytes.Buffer
	if _, err := copyFromReader(&got, data); err != nil || got.String() != "hello" {
		t.Fatalf("data = %q err=%v, want hello", got.String(), err)
	}
	// The next ReceiveUnit must skip the Ping (answering Pong) and block for
	// the next UnitBegin — bounded here by the sender closing the conn.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if _, _, err := rcv.ReceiveUnit(ctx2); err == nil {
		t.Fatal("expected an error after the ping (conn closed), got nil")
	}
	if err := <-done; err != nil {
		t.Fatalf("sender: %v", err)
	}
}

// copyFromReader drains r into w until io.EOF (a small helper that avoids
// importing io in the test's copy path — io.Copy would work too, but this
// keeps the EOF contract explicit).
func copyFromReader(w *bytes.Buffer, r interface{ Read([]byte) (int, error) }) (int64, error) {
	buf := make([]byte, 64*1024)
	var n int64
	for {
		k, err := r.Read(buf)
		if k > 0 {
			w.Write(buf[:k])
			n += int64(k)
		}
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
	}
}

// TestPersistentRedialBudget is the P1c re-dial budget acceptance (FR-06):
// once a destination has stayed dead for the whole budget (MaxReconnects
// consecutive units failing on a down channel), the next unit fails FAST
// (no 30s dial window) so the manager can record the failure and the run can
// resume. A live channel bypasses the budget (the reuse path short-circuits
// before the check).
func TestPersistentRedialBudget(t *testing.T) {
	// A local port that refuses instantly (nothing listens).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := l.Addr().String()
	_ = l.Close() // nothing dials it: every dial is refused

	s := &SourceServer{opts: ServerOptions{MaxReconnects: 2, ConnectTimeout: time.Second}}

	// Exhaust the budget (as if two consecutive units failed on the dead
	// destination): the next ensurePersistent must fail FAST, without a dial.
	s.persFails = s.reconnectBudget()
	start := time.Now()
	if err := s.ensurePersistent(context.Background(), deadAddr); err == nil {
		t.Fatal("ensurePersistent with an exhausted budget: want an error, got nil")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("exhausted-budget ensurePersistent took %v, want fast-fail (<5s)", d)
	}
	if s.persFails != s.reconnectBudget() {
		t.Fatalf("persFails = %d after an exhausted-budget check, want it capped at the budget (%d)", s.persFails, s.reconnectBudget())
	}

	// A live channel bypasses the budget (the reuse path short-circuits
	// before the check): a recovered destination re-serves the next unit.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	s.persistMu.Lock()
	s.persConn = a
	s.persSnd = NewPersistentSender(a, 0)
	s.persistMu.Unlock()
	if err := s.ensurePersistent(context.Background(), deadAddr); err != nil {
		t.Fatalf("ensurePersistent with a live channel: want nil, got %v", err)
	}
	// The live channel is still intact (not torn down by the budget check).
	if s.persSnd == nil {
		t.Fatal("live channel was torn down by the budget check")
	}
}
