package manifest

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// benchCopyStream builds a valid PostgreSQL binary COPY payload (signature +
// header + per-row int16 field-count/length fields + end-of-stream marker) —
// the exact bytes the GP kernel feeds the segreceiver on stdin inside
// COPY ... TO PROGRAM ... ON SEGMENT.
func benchCopyStream(t *testing.T, rows [][][]byte) []byte {
	t.Helper()
	var b []byte
	b = append(b, "PGCOPY\n\xff\r\n\x00"...)
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], 0) // no extension block
	binary.BigEndian.PutUint32(hdr[4:8], 0) // flags
	b = append(b, hdr[:]...)
	for _, row := range rows {
		var fc [2]byte
		binary.BigEndian.PutUint16(fc[:], uint16(len(row)))
		b = append(b, fc[:]...)
		for _, f := range row {
			var ln [4]byte
			if f == nil {
				binary.BigEndian.PutUint32(ln[:], 0xFFFFFFFF)
			} else {
				binary.BigEndian.PutUint32(ln[:], uint32(len(f)))
			}
			b = append(b, ln[:]...)
			b = append(b, f...)
		}
	}
	b = append(b, 0xFF, 0xFF) // end-of-stream marker
	return b
}

// TestBenchMeshDrainFrameAck runs the mesh drain against the REAL segreceiver
// send path (segstream.SendCompressed over a frame-ack ReliablePipe) — the
// exact transport the production segreceivers use. The drain must accept the
// slice, decompress it, and report the trailer row count + wire + logical
// volumes. This proves the mesh bench protocol end to end (frame-ack HELLO,
// framed + compressed payload, trailer) without a Greenplum kernel.
func TestBenchMeshDrainFrameAck(t *testing.T) {
	payload := benchCopyStream(t, [][][]byte{
		{[]byte("alpha"), []byte("1")},
		{nil, []byte("2")},
		{[]byte("gamma"), []byte("333333333333")},
	})
	wantRows := int64(3)
	wantLogical := uint64(len(payload))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	cfg := &config.Config{FrameAck: "on"}

	// Receiver: the bench drain (accepts ONE slice, reads it to completion).
	drainDone := make(chan benchMeshDrainResult, 1)
	go func() { drainDone <- benchMeshDrain(context.Background(), cfg, []net.Listener{l}, compress.ModeZstd) }()

	// Sender: the real segreceiver send path (what the GP kernel drives) —
	// dial + the identity-HELLO frame-ack sender (wrapFrameAck in
	// cmd/gpstream-segreceiver/main.go does exactly this).
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	rp, err := transfer.NewReliablePipeSenderOnConnID(
		cfg.ReliablePipeConfig(), conn,
		func() (transfer.DupConn, error) { return nil, context.Canceled },
		transfer.StreamID{JobID: 1, UnitID: 1, Content: 0})
	if err != nil {
		t.Fatalf("frame-ack sender: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := segstream.SendCompressed(sctx, rp, bytes.NewReader(payload), 1, 1, 0, compress.ModeZstd, 3, true)
	if err != nil {
		t.Fatalf("SendCompressed: %v", err)
	}
	if rows != wantRows {
		t.Fatalf("sender rows = %d, want %d", rows, wantRows)
	}

	res := <-drainDone
	if res.Err != nil {
		t.Fatalf("drain: %v", res.Err)
	}
	if res.Rows != wantRows {
		t.Errorf("drain rows = %d, want %d", res.Rows, wantRows)
	}
	if res.Logical != wantLogical {
		t.Errorf("drain logical = %d, want %d", res.Logical, wantLogical)
	}
	if res.Wire == 0 {
		t.Error("drain wire = 0, want > 0 (framed + compressed + frame-ack overhead)")
	}
}

// TestBenchMeshDrainPlain is the frame-ack-OFF variant: the drain reads the
// plain framed + compressed stream (no ReliablePipe) and still reports the
// trailer rows + wire + logical.
func TestBenchMeshDrainPlain(t *testing.T) {
	payload := benchCopyStream(t, [][][]byte{
		{[]byte("a"), []byte("b")},
		{[]byte("c"), []byte("d")},
	})
	wantRows := int64(2)
	wantLogical := uint64(len(payload))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cfg := &config.Config{FrameAck: "off"}

	drainDone := make(chan benchMeshDrainResult, 1)
	go func() { drainDone <- benchMeshDrain(context.Background(), cfg, []net.Listener{l}, compress.ModeZstd) }()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := segstream.SendCompressed(sctx, conn, bytes.NewReader(payload), 1, 1, 0, compress.ModeZstd, 3, false)
	if err != nil {
		t.Fatalf("SendCompressed: %v", err)
	}
	if rows != wantRows {
		t.Fatalf("sender rows = %d, want %d", rows, wantRows)
	}
	res := <-drainDone
	if res.Err != nil {
		t.Fatalf("drain: %v", res.Err)
	}
	if res.Rows != wantRows || res.Logical != wantLogical || res.Wire == 0 {
		t.Errorf("drain = rows %d (want %d) logical %d (want %d) wire %d (want >0)",
			res.Rows, wantRows, res.Logical, wantLogical, res.Wire)
	}
}

// TestBenchMeshDrainTwoContents runs the drain against TWO concurrent slices
// on two content listeners (the S=2 layout) — the parallel read path: each
// content's goroutine accepts and drains its own listener, so the slices are
// served concurrently (a serialized drain would read content 0 to completion
// before content 1's stream is read). The aggregate must sum both slices.
func TestBenchMeshDrainTwoContents(t *testing.T) {
	payloadA := benchCopyStream(t, [][][]byte{{[]byte("A1"), []byte("1")}, {[]byte("A2"), []byte("22")}})
	payloadB := benchCopyStream(t, [][][]byte{{[]byte("B1"), []byte("3")}})

	lA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lA.Close()
	lB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lB.Close()
	cfg := &config.Config{FrameAck: "off"}

	drainDone := make(chan benchMeshDrainResult, 1)
	go func() {
		drainDone <- benchMeshDrain(context.Background(), cfg, []net.Listener{lA, lB}, compress.ModeZstd)
	}()

	// Two concurrent senders (the GP kernel launches the segreceivers in
	// parallel).
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, pair := range []struct {
		l net.Listener
		p []byte
	}{{lA, payloadA}, {lB, payloadB}} {
		wg.Add(1)
		go func(i int, l net.Listener, p []byte) {
			defer wg.Done()
			<-start
			conn, err := net.Dial("tcp", l.Addr().String())
			if err != nil {
				t.Error(err)
				return
			}
			sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := segstream.SendCompressed(sctx, conn, bytes.NewReader(p), 1, uint32(i+1), 0, compress.ModeZstd, 3, false); err != nil {
				t.Errorf("sender %d: %v", i, err)
			}
		}(i, pair.l, pair.p)
	}
	close(start)
	wg.Wait()

	res := <-drainDone
	if res.Err != nil {
		t.Fatalf("drain: %v", res.Err)
	}
	if res.Rows != 3 {
		t.Errorf("drain rows = %d, want 3 (2 from A + 1 from B)", res.Rows)
	}
	if res.Logical != uint64(len(payloadA)+len(payloadB)) {
		t.Errorf("drain logical = %d, want %d", res.Logical, len(payloadA)+len(payloadB))
	}
	if res.Wire == 0 {
		t.Error("drain wire = 0, want > 0")
	}
}
