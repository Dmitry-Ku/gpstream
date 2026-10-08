// Package agent is the two-agent data path (ADR-001/ADR-002 remediation).
//
// In the single-controller build the source COPY and the destination COPY run
// as two goroutines inside ONE controller process, joined by an in-memory
// net.Pipe; the compression happens on that in-memory pipe, so the network
// legs stay raw (the ADR-002 debt). This package moves the data plane OUT of
// the controller: the source agent (one process, on the source cluster) owns
// the source pg connection, the FR-02 snapshot and the `COPY ... TO STDOUT`,
// and pushes the bytes COMPRESSED over a real socket to the destination agent
// (a separate process, on the destination cluster), which decompresses and
// runs `COPY ... FROM STDIN`. The controller becomes a control plane (P3).
//
// The source and destination halves are deliberately thin: they reuse the
// exact framing/compression the single-controller path already uses
// (transfer.FramedWriter / FramedReader, ADR-002 P0), so the on-wire format is
// identical — only the transport selection (in-memory pipe -> real socket)
// changes.
package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

type SnapshotMode string

const (
	SnapNone SnapshotMode = ""
	SnapDB   SnapshotMode = "db"
)

// SourceOptions configures one source-agent push of a single table.
type SourceOptions struct {
	// SrcConn is the source postgres conn string.
	SrcConn string
	// PG-plane TLS (the agent is a client of the source DB).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Schema/Table is the source object to stream (schema-qualified).
	Schema, Table string
	// DstSchema/DstTable are the destination names (post-rename); the dest
	// agent creates its staging table in DstSchema. DstTable is the final
	// table name (the swap target); Staging is the staging name (dest.
	// StagingName). Overwrite controls the dest agent's swap semantics.
	DstSchema, DstTable, Staging string
	Overwrite                    bool
	// JobID/UnitID are the frame ids (the job hash + the unit ordinal).
	JobID, UnitID uint32
	// DestAddr is the destination agent's listen address (host:port). The
	// source dials it (the source owns the snapshot and the data; the dest
	// publishes a per-unit endpoint, ADR-001).
	DestAddr string
	// Compress is the FR-12 mode string ("none"/"zlib"/"zstd[:level]").
	Compress string
	// Snapshot is the FR-02 scope the agent holds while streaming.
	Snapshot SnapshotMode
	// Data-plane TLS for the agent<->agent socket (the source is the CLIENT;
	// the dest agent is the server). All empty = plain TCP.
	DataTLSCert, DataTLSKey, DataTLSCA string
	// ConnectTimeout bounds the socket dial (a wedged dest agent cannot hang
	// the source forever).
	ConnectTimeout time.Duration
	// StreamTimeout bounds the whole stream (a wedged dest cannot hang the
	// source's COPY past its own lifetime).
	StreamTimeout time.Duration
	// FrameAck enables the frame-level ACK + RST-resume transport (DESIGN
	// §4.6) on the data socket; ReliablePipe carries its parameters.
	FrameAck     bool
	ReliablePipe transfer.ReliablePipeConfig
	// VerifyDigest (FR-02, P9-E) asks the source to compute the
	// row-order-independent content digest of the payload ON THE DATA PATH
	// (as the rows stream, O(1) memory) and report it in SourceResult.Digest,
	// so a verifying controller can compare against the destination's inline
	// digest without re-reading either table. Set when --verify is on.
	VerifyDigest bool
}

// dialDestOnce performs a SINGLE dial + (optional) TLS handshake to the
// destination agent (o.DestAddr). The frame-ack re-dial on a break calls
// this: a refused connection (a dead destination) must fail the unit, not
// retry.
func (o *SourceOptions) dialDestOnce(ctx context.Context) (net.Conn, error) {
	raw, err := net.DialTimeout("tcp", o.DestAddr, o.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	if !transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
		return raw, nil
	}
	host, herr := hostOf(o.DestAddr)
	if herr != nil {
		raw.Close()
		return nil, fmt.Errorf("bad --dest %q: %w", o.DestAddr, herr)
	}
	ccfg, cerr := transfer.ClientTLSConfig(host, o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, "")
	if cerr != nil {
		raw.Close()
		return nil, fmt.Errorf("tls config: %w", cerr)
	}
	tc := tls.Client(raw, ccfg)
	if hsErr := tc.HandshakeContext(ctx); hsErr != nil {
		raw.Close()
		return nil, fmt.Errorf("tls handshake with dest: %w", hsErr)
	}
	return tc, nil
}

// SourceResult is what the source agent reports after a push (FR-04 volume +
// row count). Wire is the compressed/framed byte count actually on the socket
// between the agents; Logical is the uncompressed byte count the source read
// from PG (the FramedWriter does not count logical, so this is the source-side
// COPY payload size, measured via a counting wrap).
type SourceResult struct {
	Rows    int64
	Wire    uint64
	Logical uint64
	// Digest is the row-order-independent content digest of the binary-COPY
	// payload the source streamed (FR-02, P9-E inline verify), computed on
	// the data path as the rows flow (O(1) memory). Empty when the caller
	// did not request it (UnitSpec.VerifyDigest off / --verify off): the
	// verify path then falls back to re-deriving the checksum on the snapshot.
	Digest string
}

// Push streams o.Schema.o.Table from the source to o.DestAddr over a real
// socket, compressing + framing on the wire (FR-12). It opens its own source
// pg connection (the agent is the snapshot owner), establishes the FR-02
// snapshot when o.Snapshot is set, runs `COPY ... TO STDOUT (FORMAT binary)`,
// frames/compresses the payload into the socket and closes it (end-of-copy for
// the destination). It returns the source-side row count + wire/logical volume.
func Push(ctx context.Context, o SourceOptions) (SourceResult, error) {
	var out SourceResult
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 10 * time.Second
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 6 * time.Hour
	}

	// 1. Source pg connection (the agent is the snapshot owner, ADR-001).
	src, err := pg.NewConn(ctx, o.SrcConn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
	if err != nil {
		return out, fmt.Errorf("source agent: connect source: %w", err)
	}
	defer src.Close()

	// 2. Capture the source table's shape (columns, distribution, partition)
	// BEFORE the FR-02 snapshot. The manager reads catalog on the BASE pool,
	// not the pinned REPEATABLE READ connection: a catalog SQL fault on the
	// pinned conn aborts the whole snapshot transaction (25P02) and takes down
	// the data COPY (the skill's FR-02 snapshot pitfall). With the agent's
	// single connection, capturing the schema before BEGIN keeps the snapshot
	// clean for the data COPY. (Schema is stable across the run, so reading it
	// just before the snapshot is equivalent to the manager's base-pool read.)
	srcAgent := source.New(src)
	cols, cerr := srcAgent.Columns(ctx, o.Schema, o.Table)
	if cerr != nil {
		return out, fmt.Errorf("source agent: read columns %s.%s: %w", o.Schema, o.Table, cerr)
	}
	dist := srcAgent.Distribution(ctx, o.Schema, o.Table)
	// A partition-capture fault (plain PG without pg_get_partition_def, or a
	// transient catalog error) is NON-FATAL, matching the planner: the table
	// is treated as non-partitioned (the dest is created plain and still
	// receives all rows via the parent COPY).
	part, perr := pg.CapturePartitionSpec(src, ctx, o.Schema, o.Table)
	if perr != nil {
		part = pg.PartitionSpec{}
	}
	spec := UnitSpec{
		JobID: o.JobID, UnitID: o.UnitID,
		Schema: o.DstSchema, Table: o.DstTable, Staging: o.Staging, Overwrite: o.Overwrite,
		SrcSchema: o.Schema, SrcTable: o.Table,
		Compress: o.Compress,
		Cols:     cols, Dist: dist, Part: part,
		VerifyDigest: o.VerifyDigest,
	}

	// 2b. FR-02 snapshot (the pinned REPEATABLE READ tx lives on this conn).
	if o.Snapshot != SnapNone {
		if err := beginSnapshot(ctx, src, o.Snapshot); err != nil {
			return out, err
		}
		// The snapshot dies with the session; end it cleanly on success so a
		// reused conn (multi-unit agent, P3) does not leak an open tx.
		defer func() { _, _ = src.Exec(ctx, "COMMIT") }()
	}

	// 3. Dial the destination agent (optionally TLS), under a bounded ctx so a
	// wedged dest cannot hang the source past the COPY lifetime.
	dctx, dcancel := context.WithTimeout(ctx, o.StreamTimeout)
	defer dcancel()
	conn, err := o.dialDestOnce(dctx)
	if err != nil {
		return out, fmt.Errorf("source agent: dial dest %s: %w", o.DestAddr, err)
	}

	// 3b. Send the UnitSpec header first: the destination reads it to build
	// the staging table before it reads the framed data stream. (It is NOT
	// re-sent on a resume: the re-dial replays un-ACKed frames only.)
	if serr := WriteUnitSpec(conn, spec); serr != nil {
		conn.Close()
		return out, serr
	}

	// 4. Compression (FR-12) — the bytes between the agents travel compressed.
	mode, level, perr := compress.Parse(o.Compress)
	if perr != nil {
		conn.Close()
		return out, fmt.Errorf("source agent: compress %q: %w", o.Compress, perr)
	}
	comp := compress.New(mode, level)

	// 4b. Data channel: a plain conn, or (frame-ack on, DESIGN §4.6) a
	// ReliablePipe that re-dials the destination on a break and replays the
	// un-ACKed frames (the destination's receiver dedups by seq, so its open
	// COPY FROM survives the break).
	frameConn := transfer.Pipe(conn)
	var rp *transfer.ReliablePipe
	if o.FrameAck {
		var rerr error
		rp, rerr = transfer.NewReliablePipeSenderOnConn(o.ReliablePipe, conn, func() (transfer.DupConn, error) {
			return o.dialDestOnce(dctx)
		})
		if rerr != nil {
			conn.Close()
			return out, fmt.Errorf("source agent: frame-ack transport: %w", rerr)
		}
		frameConn = rp
	}
	fw := transfer.NewFramedWriter(frameConn, o.JobID, o.UnitID, comp)

	// 5. Stream: COPY ... TO STDOUT -> (count) -> [digest] -> frame+compress
	// -> socket. The counting wrap measures the logical (uncompressed) volume
	// the source read from PG, for the FR-04 report. When VerifyDigest is set
	// (--verify on, FR-02/P9-E) a digest tee computes the row-order-
	// independent content digest ON THE DATA PATH (O(1) memory), so the
	// controller can verify against the destination's inline digest without
	// re-reading either table.
	counted := &logicalCounter{}
	// The row-order-independent content digest is ALWAYS computed on the data
	// path (O(1) memory): the destination verifies on the data path without a
	// re-read, so it is unconditional (no --verify gate).
	dsum := transfer.NewContentDigest()
	dataOut := &digestTee{next: fw, d: dsum}
	tee := &teeWriter{logical: counted, out: dataOut}
	rows, cerr := srcAgent.CopyToStream(dctx, o.Schema, o.Table, tee)
	if cerr != nil {
		conn.Close()
		return out, fmt.Errorf("source agent: copy %s.%s: %w", o.Schema, o.Table, cerr)
	}
	// 6. End-of-copy: flush the framed tail and signal clean end-of-stream.
	// With frame-ack the FIN rides the pipe (the pipe owns the conn); without
	// it the socket close is the end-of-copy.
	if rp != nil {
		if flerr := fw.Flush(); flerr != nil && cerr == nil {
			cerr = flerr
		}
		_ = rp.Close()
	} else {
		if flerr := fw.CloseFlush(); flerr != nil && cerr == nil {
			cerr = flerr
		}
	}

	out.Rows = rows
	out.Wire = fw.Wire()
	out.Logical = uint64(counted.n)
	if cerr != nil {
		return out, cerr
	}
	// P9-E: finalize the inline content digest (the COPY completed cleanly,
	// so the stream has its end marker; a Sum error is a genuine anomaly).
	d, derr := dsum.Sum()
	if derr != nil {
		return out, fmt.Errorf("source agent: inline digest %s.%s: %w", o.Schema, o.Table, derr)
	}
	out.Digest = d
	return out, nil
}

// snapshotExec is the minimal pg-exec interface beginSnapshot needs (both a
// *pg.Conn and a pinned pool connection satisfy it).
type snapshotExec interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
}

// beginSnapshot opens the FR-02 REPEATABLE READ transaction on the agent's
// source connection (mirrors manager.beginSnapshotTx). The agent owns the
// snapshot for its whole lifetime; the controller (P3) streams units one by
// one over it.
//
// The SELECT 1 right after BEGIN pins the snapshot point IMMEDIATELY: in
// Postgres a REPEATABLE READ snapshot is established at the first query in
// the transaction, not at BEGIN. Without the probe, the "run-wide" snapshot
// would actually open at the first unit's data COPY, and concurrent source
// writes committed between the run start and that first COPY would leak into
// the run (breaking FR-02's run-consistency contract).
func beginSnapshot(ctx context.Context, src snapshotExec, mode SnapshotMode) error {
	if _, err := src.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ`); err != nil {
		return fmt.Errorf("source agent: begin snapshot tx: %w", err)
	}
	if _, err := src.Exec(ctx, `SELECT 1`); err != nil {
		return fmt.Errorf("source agent: pin snapshot point: %w", err)
	}
	return nil
}

// hostOf returns the host part of a host:port address (the TLS client's
// expected server name, if it is a name).
func hostOf(listen string) (string, error) {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	return host, nil
}

// dialDestRetry dials the destination agent's data socket, retrying while the
// destination is still binding its listen socket. The controller launches the
// per-unit destination agent and then instructs the source to dial it, so there
// is a short window where the destination has not bound yet and the dial is
// refused. A refused connection never reached the destination's (single)
// accept, so retrying is safe and cannot consume the destination's one
// connection. The retry is bounded (the destination binds within milliseconds
// of starting).
func dialDestRetry(ctx context.Context, addr string, perDial time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		conn, err := net.DialTimeout("tcp", addr, perDial)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("dial %s (retries exhausted): %w", addr, lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// logicalCounter is an io.Writer that only counts bytes (the logical volume).
type logicalCounter struct{ n int64 }

func (l *logicalCounter) Write(p []byte) (int, error) {
	l.n += int64(len(p))
	return len(p), nil
}

// teeWriter writes each chunk to both the logical counter and the framed
// (compressed) transport, so the source can report logical volume while
// pushing the compressed bytes.
type teeWriter struct {
	logical io.Writer
	out     io.Writer
}

func (t *teeWriter) Write(p []byte) (int, error) {
	if _, err := t.logical.Write(p); err != nil {
		return 0, err
	}
	return t.out.Write(p)
}

// digestTee tees each chunk into a ContentDigestWriter (FR-02, P9-E) and
// forwards it to the next writer, so the source computes the row-order-
// independent content digest ON THE DATA PATH (as the rows stream, O(1)
// memory) instead of re-reading the table after the copy. It is only in the
// chain when UnitSpec.VerifyDigest is set (--verify on), so the default
// (verify-off) path pays no digest cost.
type digestTee struct {
	next io.Writer
	d    *transfer.ContentDigestWriter
}

func (dr *digestTee) Write(p []byte) (int, error) {
	if _, err := dr.d.Write(p); err != nil {
		return 0, err
	}
	return dr.next.Write(p)
}
