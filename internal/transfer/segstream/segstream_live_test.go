//go:build gp6lab

package segstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// buildSegreceiver compiles the real gpstream-segreceiver binary into a temp
// dir (the same binary the operator installs on the segment hosts).
func buildSegreceiver(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "gpstream-segreceiver")
	cmd := exec.Command("go", "build", "-o", bin, "../../../cmd/gpstream-segreceiver")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build segreceiver: %v\n%s", err, out)
	}
	return bin
}

// TestLiveSegreceiverCrossCheck (DESIGN §1.2 / T-B) runs the REAL
// gpstream-segreceiver binary against a REAL PostgreSQL 9.4.26 (the GP6
// kernel): a binary-COPY stream of a live table is fed to the binary on
// stdin (as the GPDB kernel would do inside COPY ... TO PROGRAM ... ON
// SEGMENT), the binary frames and streams it to the controller listener, and
// the controller's Receive must reproduce the exact bytes, the row count and
// the digest. This proves the whole segment side of the transport end to
// end (the ON SEGMENT dispatch itself is kernel-only and is exercised on the
// lab).
func TestLiveSegreceiverCrossCheck(t *testing.T) {
	const n = 20000
	bin := buildSegreceiver(t)

	cl, err := pgtest.Start(5605, "seg_test")
	if err != nil {
		t.Fatalf("start %s: %v", t.Name(), err)
	}
	t.Cleanup(func() { cl.Stop() })
	c, err := pg.NewConn(context.Background(), cl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	ctx := context.Background()

	// Seed: mixed NULLs / empty strings / short and long values so the
	// binary-COPY rows exercise every field path.
	if _, err := c.Exec(ctx, `CREATE TABLE seg_src (id int, name text, body text)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	stmt := `INSERT INTO seg_src
		SELECT i,
		       CASE WHEN i % 2 = 0 THEN NULL
		            WHEN i % 10 = 0 THEN ''
		            ELSE 'name_' || i END,
		       repeat('x' || (i % 7), i % 300)
		FROM generate_series(1, ` + strconv.Itoa(n) + `) i`
	if _, err := c.Exec(ctx, stmt); err != nil {
		t.Fatalf("populate: %v", err)
	}

	// The real binary-COPY stream from the live table (what the kernel
	// feeds to the segreceiver on stdin).
	var stream bytes.Buffer
	if _, err := c.ReadCopy(ctx, &stream, `COPY (SELECT * FROM seg_src ORDER BY id) TO STDOUT (FORMAT binary)`); err != nil {
		t.Fatalf("readcopy: %v", err)
	}
	if stream.Len() == 0 {
		t.Fatal("empty COPY stream")
	}

	// Controller side: a listener on a random localhost port (what the
	// gpstream controller runs on the host).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()

	// Segment side: the real binary, stdin = the COPY stream, frame-ack ON
	// (the production on_segment default). The controller side receives with
	// the production frame-ack stack (ResumeRouter + ReliablePipe receiver),
	// exactly as the dest server's serveSegmentsFrameAck does, so this
	// exercises the real segreceiver binary end to end in its default
	// transport.
	seg := exec.Command(bin, "--listen", addr, "--job", "1", "--unit", "1", "--content", "0")
	seg.Stdin = bytes.NewReader(stream.Bytes())
	var segErr bytes.Buffer
	seg.Stderr = &segErr
	if err := seg.Start(); err != nil {
		t.Fatalf("start segreceiver: %v", err)
	}

	// Controller side: the production frame-ack receive stack (one pool slot,
	// one slice). The router owns the listener and closes it on Close.
	router := transfer.NewResumeRouter(l)
	go router.Run()
	defer router.Close()
	slot := router.NewPoolSlot()
	rp, rerr := transfer.NewReliablePipeReceiverOnSlot(transfer.DefaultReliablePipeConfig(), slot, 5*time.Minute)
	if rerr != nil {
		t.Fatalf("frame-ack receiver: %v", rerr)
	}
	rctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	var got []byte
	res := &SliceResult{}
	rerr = ReadStreamCompressed(rctx, rp, res, compress.ModeNone, func(contentID int16, p []byte) error {
		if contentID != 0 {
			t.Errorf("content %d, want 0", contentID)
		}
		got = append(got, p...)
		return nil
	})
	if waitErr := seg.Wait(); waitErr != nil {
		t.Fatalf("segreceiver exit: %v\nstderr: %s", waitErr, segErr.String())
	}
	if rerr != nil {
		t.Fatalf("ReadStreamCompressed: %v", rerr)
	}
	if res.Err != nil {
		t.Fatalf("slice err: %v", res.Err)
	}
	if res.Rows != n {
		t.Fatalf("trailer rows = %d, want %d", res.Rows, n)
	}
	if !bytes.Equal(got, stream.Bytes()) {
		t.Fatalf("payload not byte-transparent: %d vs %d bytes", len(got), stream.Len())
	}
	wantSum := sha256.Sum256(stream.Bytes())
	if res.SHA256 != wantSum {
		t.Fatalf("trailer sha256 mismatch: %x vs %x", res.SHA256, wantSum)
	}
	fmt.Printf("live segreceiver: %d rows, %d bytes, trailer verified (frame-ack)\n", res.Rows, len(got))
}

// TestLiveSegreceiverBadContent: a segreceiver launched with a garbage
// --content must fail fast (non-zero exit) instead of streaming.
func TestLiveSegreceiverBadContent(t *testing.T) {
	bin := buildSegreceiver(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	seg := exec.Command(bin, "--listen", l.Addr().String(), "--job", "1", "--unit", "1", "--content", "not-a-number")
	seg.Stdin = bytes.NewReader([]byte("PGCOPY\n\xff\r\n\x00"))
	var out bytes.Buffer
	seg.Stderr = &out
	err = seg.Run()
	if err == nil {
		t.Fatal("expected non-zero exit on bad --content")
	}
	if out.Len() == 0 {
		t.Fatal("expected a diagnostic on stderr")
	}
}
