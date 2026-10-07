// Command gpstream-source-agent is the two-agent data-path source (ADR-001 /
// ADR-002 remediation). It runs on a source-cluster host, owns the source pg
// connection and the FR-02 snapshot, streams tables with
// `COPY ... TO STDOUT (FORMAT binary)`, compresses + frames the payload
// (FR-12) and pushes it COMPRESSED over a real socket (optionally TLS) to the
// destination agent. It is the source half of the network data path that the
// single-controller build does in memory.
//
// Two modes:
//
//   - one-shot (default): stream exactly one table (given by --schema/--table
//     and --dest) and exit. Used by tests and manual runs.
//   - --serve LISTEN: the long-lived source agent (P3). It opens the source
//     pool, establishes the FR-02 snapshot (which must outlive every unit: a
//     REPEATABLE READ transaction lives on one server-side backend), and
//     listens on LISTEN for the controller's control protocol (stream /
//     checksum / release). The controller streams one unit per message; the
//     whole run shares the single snapshot. This is the mode the controller
//     uses (two-agent is the only data path).
//
// Exit codes: 0 clean (stream complete / released); 1 argument/connect/stream
// failure (the controller surfaces it as the unit error; a non-zero exit never
// leaves a silent partial stream).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/transfer"
)

func main() {
	// Source pg connection (the agent is a client of the source DB).
	src := flag.String("src", "", "source postgres conn string (required)")
	sslCert := flag.String("ssl-cert", "", "pg-plane client certificate PEM")
	sslKey := flag.String("ssl-key", "", "pg-plane client key PEM")
	sslCA := flag.String("ssl-ca", "", "pg-plane trusted CA bundle PEM (verify-full)")
	sslMinTLS := flag.String("ssl-min-tls", "1.2", "pg-plane minimum TLS version")
	// One-shot mode: the unit to stream.
	schema := flag.String("schema", "public", "source schema (one-shot mode)")
	table := flag.String("table", "", "source table (one-shot mode, required)")
	job := flag.String("job", "0", "job id (decimal, the plan hash)")
	unit := flag.String("unit", "0", "unit id (decimal, the unit ordinal)")
	dstSchema := flag.String("dst-schema", "", "destination schema (defaults to --schema)")
	dstTable := flag.String("dst-table", "", "destination final table name (defaults to --table)")
	staging := flag.String("staging", "", "staging table name (one-shot mode, required)")
	overwrite := flag.Bool("overwrite", false, "destination swaps by dropping the existing table")
	dest := flag.String("dest", "", "destination agent listen address host:port (one-shot mode, required)")
	compress := flag.String("compress", "zstd", "stream compression none|zlib|zstd[:level]")
	snapshot := flag.String("snapshot", "db", "FR-02 snapshot scope: none|db")
	// Data-plane TLS for the agent<->agent socket (source = client).
	dataCert := flag.String("data-tls-cert", "", "agent-stream client certificate PEM")
	dataKey := flag.String("data-tls-key", "", "agent-stream client key PEM")
	dataCA := flag.String("data-tls-ca", "", "agent-stream trusted CA bundle PEM (enables mTLS)")
	// Control-channel TLS (the controller's dial to this agent's control
	// socket; this agent = server). CA present = mTLS (require the
	// controller's client certificate).
	ctrlCert := flag.String("control-tls-cert", "", "control-socket serving certificate PEM")
	ctrlKey := flag.String("control-tls-key", "", "control-socket serving key PEM")
	ctrlCA := flag.String("control-tls-ca", "", "control-socket trusted CA bundle PEM (enables mTLS)")
	// Timeouts.
	connectTO := flag.Duration("connect-timeout", 10*time.Second, "socket dial timeout")
	streamTO := flag.Duration("stream-timeout", 6*time.Hour, "whole-stream timeout")
	// Frame-ACK (DESIGN §4.6): frame-level ACK + RST-resume on the data
	// socket. The controller passes the run's values (default on).
	frameAck := flag.String("frame-ack", "on", "frame-level ACK + RST-resume on the data socket: on|off (default on)")
	ackTimeout := flag.String("ack-timeout", "30s", "ACK timeout: 15s|30s|adaptive")
	maxReconnects := flag.Int("max-reconnects", 20, "reconnect budget per connection")
	retransmitMax := flag.Int64("retransmit-buffer-max", 1<<30, "in-flight retransmit window (bytes)")
	// Long-lived (P3) mode: listen for the controller's control protocol.
	serve := flag.String("serve", "", "control listen address host:port (long-lived source agent mode)")
	// P1d: --workers > 1 = snapshot-epoch (N pinned REPEATABLE READ
	// connections); --max-snapshot-delta bounds the pin spread (seconds).
	workers := flag.Int("workers", 1, "parallel data workers (1 = strict, N>1 = snapshot-epoch)")
	snapDelta := flag.Float64("max-snapshot-delta", 1, "snapshot-epoch pin-spread limit, seconds (0 disables)")
	// Doctor probe: print a marker and exit 0 (no stream), proving the binary
	// is installed on the source host.
	check := flag.Bool("check", false, "print a marker and exit 0 (doctor binary probe)")
	flag.Parse()

	fail := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "gpstream-source-agent: "+format+"\n", a...)
		os.Exit(1)
	}
	if *check {
		fmt.Fprintln(os.Stderr, "gpstream-source-agent: ok (check)")
		os.Exit(0)
	}
	if *src == "" {
		fail("--src is required")
	}

	// SIGINT/SIGTERM cancels the work so a controller-side stop unwinds the
	// COPY and the socket cleanly (and rolls back the snapshot in serve mode).
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Long-lived mode (P3): the controller drives this agent over the control
	// socket; the FR-02 snapshot spans the whole run.
	if *serve != "" {
		// P1e: the agent must run on the cluster master (coordinator) and
		// connect to pg LOCALLY. Verified before the control listener is
		// bound: a failing agent never accepts the controller's dial, so the
		// run aborts with this verdict (from the agent log).
		if cerr := agent.CheckOnCoordinator(ctx, *src, *sslCert, *sslKey, *sslCA, *sslMinTLS); cerr != nil {
			fail("coordinator check: %v", cerr)
		}
		srv := agent.NewSourceServer(agent.ServerOptions{
			SrcConn:          *src,
			SSLCert:          *sslCert,
			SSLKey:           *sslKey,
			SSLCA:            *sslCA,
			SSLMinTLS:        *sslMinTLS,
			Snapshot:         agent.SnapshotMode(*snapshot),
			Listen:           *serve,
			DataTLSCert:      *dataCert,
			DataTLSKey:       *dataKey,
			DataTLSCA:        *dataCA,
			ControlTLSCert:   *ctrlCert,
			ControlTLSKey:    *ctrlKey,
			ControlTLSCA:     *ctrlCA,
			ConnectTimeout:   *connectTO,
			StreamTimeout:    *streamTO,
			MaxReconnects:    *maxReconnects, // P1c: persistent channel re-dial budget
			FrameAck:         *frameAck != "off",
			ReliablePipe:     transfer.NewReliablePipeConfig(*ackTimeout, *maxReconnects, *retransmitMax),
			Workers:          *workers, // P1d: 1 = strict, N>1 = snapshot-epoch
			MaxSnapshotDelta: time.Duration(*snapDelta * float64(time.Second)),
		})
		addr, err := srv.Start(ctx)
		if err != nil {
			fail("serve: %v", err)
		}
		// The bound control address goes to stderr for the controller to parse;
		// the agent blocks until the run ends: a SIGINT/SIGTERM (controller stop)
		// or the controller closing its control connection (controller crash).
		// On control-connection loss the agent must EXIT, not linger: its
		// pinned FR-02 snapshot is a server-side REPEATABLE READ transaction on
		// one backend, and a lingering agent holds that backend (and its
		// snapshot) for the life of the process. serveControl closes s.stopped
		// when the controller's connection goes away, so Done() is the
		// controller-death signal; srv.Close() rolls the snapshot back.
		fmt.Fprintf(os.Stderr, "gpstream-source-agent: serving control on %s\n", addr)
		select {
		case <-ctx.Done():
		case <-srv.Done():
			fmt.Fprintln(os.Stderr, "gpstream-source-agent: control connection lost (controller gone); rolling back the run snapshot and exiting")
		}
		srv.Close()
		fmt.Fprintln(os.Stderr, "gpstream-source-agent: stopped")
		return
	}

	// One-shot mode: stream exactly one table to a given destination agent.
	if *table == "" || *dest == "" || *staging == "" {
		fail("--table, --dest and --staging are required (one-shot mode)")
	}
	ds, dt := *dstSchema, *dstTable
	if ds == "" {
		ds = *schema
	}
	if dt == "" {
		dt = *table
	}
	jobID, err := strconv.ParseUint(*job, 10, 32)
	if err != nil {
		fail("--job: %v", err)
	}
	unitID, err := strconv.ParseUint(*unit, 10, 32)
	if err != nil {
		fail("--unit: %v", err)
	}

	res, err := agent.Push(ctx, agent.SourceOptions{
		SrcConn:        *src,
		SSLCert:        *sslCert,
		SSLKey:         *sslKey,
		SSLCA:          *sslCA,
		SSLMinTLS:      *sslMinTLS,
		Schema:         *schema,
		Table:          *table,
		DstSchema:      ds,
		DstTable:       dt,
		Staging:        *staging,
		Overwrite:      *overwrite,
		JobID:          uint32(jobID),
		UnitID:         uint32(unitID),
		DestAddr:       *dest,
		Compress:       *compress,
		Snapshot:       agent.SnapshotMode(*snapshot),
		DataTLSCert:    *dataCert,
		DataTLSKey:     *dataKey,
		DataTLSCA:      *dataCA,
		ConnectTimeout: *connectTO,
		StreamTimeout:  *streamTO,
		FrameAck:       *frameAck != "off",
		ReliablePipe:   transfer.NewReliablePipeConfig(*ackTimeout, *maxReconnects, *retransmitMax),
	})
	if err != nil {
		fail("%v", err)
	}
	// The report goes to stderr (stdout is not a data path here). The
	// controller (P3) parses this to journal the unit.
	fmt.Fprintf(os.Stderr, "gpstream-source-agent: unit=%d rows=%d wire=%d logical=%d ok\n",
		unitID, res.Rows, res.Wire, res.Logical)
}
