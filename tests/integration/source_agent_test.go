//go:build integration

package integration

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// countingConn counts the bytes actually READ off the socket by the
// destination (the on-wire volume), which is what P1 must prove is the
// COMPRESSED volume. (The source writes these bytes; the destination reads
// them, so the destination's read counter is the honest on-wire measurement.)
type countingConn struct {
	net.Conn
	n atomic.Uint64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.n.Add(uint64(n))
	return n, err
}

// TestSourceAgentPushOverTCP is the P1 acceptance: a real pgtest source
// cluster + a dummy destination (a socket server that reads the framed stream
// and reconstructs the logical payload). It proves the ADR-002 debt being
// closed at the source side:
//   - the bytes ON THE SOCKET are the compressed/framed volume (strictly less
//     than the logical table payload for compressible data),
//   - the reconstructed logical payload is byte-for-byte the source table,
//   - the source's reported row count matches the source table's rows.
//
// This is the two-agent coordinator data path (source agent -> dest socket);
// the destination agent's COPY FROM is P2.
func TestSourceAgentPushOverTCP(t *testing.T) {
	const (
		srcPort = 5613
		rows    = 20000
		jobID   = 0xABCD
		unitID  = 7
	)
	ctx := context.Background()

	cl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer cl.Stop()
	src, err := pg.NewConn(ctx, cl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer src.Close()

	// A compressible table: repetitive text + small ints. zstd must shrink it
	// well below the logical volume so the socket-bytes < logical-bytes
	// assertion is meaningful (not a flake).
	mustExec(t, src, `DROP TABLE IF EXISTS orders`)
	mustExec(t, src, `CREATE TABLE orders (id integer, customer text, amount numeric)`)
	mustExec(t, src,
		`INSERT INTO orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	if n := mustCount(t, src, `SELECT count(*) FROM orders`); n != rows {
		t.Fatalf("orders seeded %d, want %d", n, rows)
	}

	// Reference logical payload: read the table directly (binary COPY) so we
	// have the exact bytes the agent must stream.
	ref := &bytes.Buffer{}
	if _, err := source.New(src).CopyToStream(ctx, "public", "orders", ref); err != nil {
		t.Fatalf("reference copy: %v", err)
	}
	logicalLen := ref.Len()

	// Dummy destination: accept one connection, read the framed+compressed
	// stream to EOF, reconstruct the logical payload, and count on-wire bytes.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	type dstResult struct {
		payload []byte
		wire    uint64
		err     error
	}
	resCh := make(chan dstResult, 1)
	go func() {
		conn, aerr := l.Accept()
		if aerr != nil {
			resCh <- dstResult{err: aerr}
			return
		}
		defer conn.Close()
		cc := &countingConn{Conn: conn}
		dec := compress.NewDec(compress.ModeZstd)
		fr := transfer.NewFramedReader(cc, dec)
		got, rerr := readAllBuf(fr)
		if rerr != nil {
			resCh <- dstResult{err: rerr}
			return
		}
		resCh <- dstResult{payload: got, wire: cc.n.Load()}
	}()

	// Run the source agent in-process (the same code path as the binary).
	start := time.Now()
	res, perr := agent.Push(ctx, agent.SourceOptions{
		SrcConn:  cl.URL(),
		Schema:   "public",
		Table:    "orders",
		JobID:    jobID,
		UnitID:   unitID,
		DestAddr: l.Addr().String(),
		Compress: "zstd",
		Snapshot: agent.SnapDB,
	})
	if perr != nil {
		t.Fatalf("source agent push: %v", perr)
	}
	t.Logf("push: rows=%d wire=%d logical=%d (%v)", res.Rows, res.Wire, res.Logical, time.Since(start))

	dres := <-resCh
	if dres.err != nil {
		t.Fatalf("destination read: %v", dres.err)
	}

	// 1. The on-wire volume is the compressed volume: strictly less than the
	// logical payload (the whole point of FR-12 / the ADR-002 debt).
	if dres.wire >= uint64(logicalLen) {
		t.Fatalf("socket carried %d bytes >= logical %d: the stream is NOT compressed on the wire", dres.wire, logicalLen)
	}
	// The source's self-reported wire count must agree with the measured
	// socket bytes (its FR-04 report is honest).
	if res.Wire != dres.wire {
		t.Fatalf("source reported wire=%d but the socket carried %d", res.Wire, dres.wire)
	}

	// 2. The reconstructed logical payload is byte-for-byte the source table.
	if !bytes.Equal(dres.payload, ref.Bytes()) {
		t.Fatalf("reconstructed payload differs from source: got %d bytes, want %d", len(dres.payload), logicalLen)
	}

	// 3. The source's reported row count matches the source table.
	if res.Rows != rows {
		t.Fatalf("source reported rows=%d, want %d", res.Rows, rows)
	}
	// 4. The source's reported logical volume matches the reference payload.
	if int(res.Logical) != logicalLen {
		t.Fatalf("source reported logical=%d, want %d", res.Logical, logicalLen)
	}
	t.Logf("P1 ok: logical=%d wire=%d ratio=%.1fx rows=%d",
		logicalLen, dres.wire, float64(logicalLen)/float64(dres.wire), res.Rows)
}

// readAllBuf reads r fully into a fresh buffer, treating io.EOF as the end.
func readAllBuf(r io.Reader) ([]byte, error) {
	buf := &bytes.Buffer{}
	tmp := make([]byte, 256*1024)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			if _, werr := buf.Write(tmp[:n]); werr != nil {
				return buf.Bytes(), werr
			}
		}
		if err != nil {
			if err == io.EOF {
				return buf.Bytes(), nil
			}
			return buf.Bytes(), err
		}
	}
}
