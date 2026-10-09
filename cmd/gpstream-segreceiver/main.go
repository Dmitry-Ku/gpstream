// Command gpstream-segreceiver is the segment-side helper of the
// segment-direct transport (Fallback B). The GPDB kernel launches it on each
// primary segment via `COPY ... TO PROGRAM 'gpstream-segreceiver --listen
// H:P --job J --unit U --content <SEGID>' ON SEGMENT`; the segment's local
// slice of the table (binary COPY stream) arrives on stdin under the
// coordinator's snapshot.
//
// The process frames the bytes (Frame v2, content-tagged), streams them to
// the controller over one direct TCP connection and finishes with a Trailer
// frame (sha256 + row count). It is unprivileged: no root, no writes to
// segment data dirs, network egress to the controller only (F-15: install =
// copy one file to the segment hosts).
//
// Exit codes: 0 clean stream (trailer sent); 1 argument/transport failure.
// A non-zero exit fails the coordinator's COPY (DF_CANCEL_ON_ERROR), which
// the controller surfaces as the job error — there is no silent partial
// stream.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
	"github.com/gpstream/gpstream/internal/version"
)

// P9 routed mode: when --dest-addr is set, this segreceiver does not stream
// its slice to a destination agent as one content-tagged slice; it routes
// every row to its destination segment (cdbhash + jump-consistent-hash over
// the distribution key, the destination kernel's placement) and fans the
// (s,d) pair out over --route-workers sub-streams, so the cluster carries
// S×D×W segment-to-segment streams and NEITHER cluster coordinator is in the
// data path.

func main() {
	listen := flag.String("listen", "", "controller listen address host:port (required)")
	job := flag.String("job", "0", "job id (decimal)")
	unit := flag.String("unit", "0", "unit id (decimal)")
	content := flag.String("content", "", "segment content id (decimal; the kernel substitutes <SEGID>)")
	check := flag.Bool("check", false, "print a marker and exit 0 (dry-run binary probe; no stream)")
	// F-13 data-plane TLS (embedded by the controller in the COPY TO PROGRAM
	// string when the operator enabled it). All three empty = plain TCP.
	sslCert := flag.String("ssl-cert", "", "client certificate PEM (data-plane TLS)")
	sslKey := flag.String("ssl-key", "", "client key PEM (data-plane TLS)")
	sslCA := flag.String("ssl-ca", "", "trusted CA bundle PEM for the controller cert")
	// P4: per-slice compression (on_segment through agents). The controller
	// embeds the mode in the program string when the two-agent on_segment
	// path is active; the destination agent decompresses with the matching
	// decoder. Empty = plain (uncompressed) frames (the single-controller
	// path, pre-P4 behavior).
	compressMode := flag.String("compress", "", "per-slice compression mode: none|zlib|zstd[:level]")
	// P9 routed mode (S≠D): the destination agent's listen address. When set,
	// this segreceiver routes its slice to D destination segments (--route-d),
	// fanning each (s,d) pair over --route-workers sub-streams (S×D×W
	// connections cluster-wide; neither cluster coordinator is in the data
	// path). --route-key is the destination distribution key's column type
	// OIDs (comma-separated, in key order) — internal values embedded by the
	// controller, no user input.
	destAddr := flag.String("dest-addr", "", "P9 routed mode: destination agent listen address host:port (enables routing)")
	routeKey := flag.String("route-key", "", "P9 routed mode: distribution-key type OIDs, comma-separated")
	routeD := flag.Int("route-d", 0, "P9 routed mode: number of destination primary segments")
	routeWorkers := flag.Int("route-workers", 1, "P9 routed mode: sub-streams per (source-seg,dest-seg) pair")
	// P3 production-dispatch (mesh direct_1to1): --dest-count S (== D, the
	// source segment count) makes --dest-addr the D-entry ADDRESS BOOK
	// (comma-separated mini-agent data endpoints, seg order): this
	// segreceiver (on source segment s = <SEGID>) dials the entry s — its
	// own destination segment's mini-agent (S==D, equal hash keys) — and
	// streams its slice to that mini-agent's data listener. Neither cluster
	// coordinator is in the data path (design 04.10).
	destCount := flag.Int("dest-count", 0, "P3 mesh direct_1to1: the source segment count S (== D); --dest-addr is the D-entry address book (each segreceiver dials entry <SEGID>)")
	// Inc3: frame-level ACK + RST-resume on the segment<->agent data links
	// (DESIGN §4.6). When on, each slice rides a ReliablePipe: a broken
	// link re-dials the agent's data port and replays the un-ACKed frames;
	// the destination's router routes the re-dial to this slice's own
	// receiver-pipe (identity HELLO), so the unit self-heals within the
	// --max-reconnects budget. The destination agent is launched with the
	// matching --frame-ack by the controller.
	frameAck := flag.String("frame-ack", "on", "frame-level ACK + RST-resume on the data link: on|off (default on)")
	ackTimeout := flag.String("ack-timeout", "30s", "ACK timeout: 15s|30s|adaptive")
	maxReconnects := flag.Int("max-reconnects", 20, "reconnect budget per slice")
	retransmitMax := flag.Int64("retransmit-buffer-max", 1<<30, "in-flight retransmit window (bytes)")
	// The slice's end-of-stream Trailer ALWAYS carries the raw per-row
	// content-digest accumulator (the mesh srcDigest building block, P0). The
	// destination uses it to verify on the data path without a re-read, so it
	// is unconditional (no flag).
	showVersion := flag.Bool("version", false, "print the build version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("gpstream-segreceiver " + version.String())
		os.Exit(0)
	}

	fail := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "gpstream-segreceiver: "+format+"\n", a...)
		os.Exit(1)
	}
	if *check {
		// The dry-run diagnostic runs `COPY (SELECT 1) TO PROGRAM 'gpstream-segreceiver
		// --check --content <SEGID>' ON SEGMENT`: a clean exit 0 on every
		// segment proves the binary is installed on all segment hosts (a
		// missing binary makes the kernel fail the COPY with
		// "could not execute file"). The marker goes to stderr (the segment
		// server log) — stdout belongs to the COPY data path.
		fmt.Fprintln(os.Stderr, "gpstream-segreceiver: ok (check)")
		os.Exit(0)
	}
	// Routed mode (P9, S!=D — both the single-controller and the mesh path) is
	// identified by the route-only flags. Mesh direct_1to1 (P3) ALSO sets
	// --dest-addr (the D-entry address book) plus --dest-count, but has NO
	// --route-d/--route-key; distinguishing by --dest-addr alone misdetected it
	// as routed and failed "--route-d must be >= 1" on the first live A->A run.
	routed := *routeD >= 1 && *routeKey != ""
	if !routed && *listen == "" && *destCount == 0 {
		fail("--listen host:port is required (or --dest-addr for routed mode)")
	}
	if routed {
		if *routeD < 1 {
			fail("--route-d must be >= 1 in routed mode")
		}
		if *routeWorkers < 1 {
			fail("--route-workers must be >= 1 in routed mode")
		}
		if *routeKey == "" {
			fail("--route-key (distribution-key OIDs) is required in routed mode")
		}
	}
	jobID, err := strconv.ParseUint(*job, 10, 32)
	if err != nil {
		fail("--job: %v", err)
	}
	unitID, err := strconv.ParseUint(*unit, 10, 32)
	if err != nil {
		fail("--unit: %v", err)
	}
	contentID, err := strconv.ParseInt(*content, 10, 16)
	if err != nil {
		fail("--content: %v (the kernel must substitute <SEGID>)", err)
	}

	// P3 production-dispatch (mesh direct_1to1): resolve this segreceiver's
	// destination from the D-entry address book — source segment s streams
	// to the destination segment s's mini-agent (entry s). The rest of the
	// (non-routed) path is unchanged: it dials the resolved address.
	if *destCount > 0 {
		if *destAddr == "" {
			fail("--dest-addr (the D-entry address book) is required with --dest-count")
		}
		grid, gerr := segstream.ParseDestAddrs(*destAddr, *destCount)
		if gerr != nil {
			fail("mesh --dest-addr: %v", gerr)
		}
		if contentID < 0 || contentID >= int64(*destCount) {
			fail("mesh: content <SEGID> %d out of range (want 0..%d)", contentID, *destCount-1)
		}
		*listen = grid[contentID]
	}

	// The kernel runs us with a bounded lifetime (the COPY transaction);
	// bound our own connect + stream accordingly so a wedged controller
	// cannot hang the segment forever.
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	if routed {
		runRouted(ctx, fail, contentID, jobID, unitID, *destAddr, *routeKey, *routeD, *routeWorkers, *compressMode, *sslCert, *sslKey, *sslCA, *ackTimeout, *maxReconnects, *retransmitMax, frameAckOn(*frameAck), true)
		return
	}

	// The destination agent may still be binding its listen socket (the
	// controller launches it right before the source agent triggers the
	// ON SEGMENT dispatch), so retry the dial briefly.
	var raw net.Conn
	var derr error
	deadline := time.Now().Add(30 * time.Second)
	for {
		raw, derr = net.DialTimeout("tcp", *listen, 10*time.Second)
		if derr == nil {
			break
		}
		if time.Now().After(deadline) {
			fail("connect %s (retries exhausted): %v", *listen, derr)
		}
		select {
		case <-ctx.Done():
			fail("connect %s: %v", *listen, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	// F-13: data-plane TLS. The controller embedded the cert/key/ca paths in
	// our command line; all three empty = plain TCP (the pre-F-13 behavior).
	// The handshake runs under ctx so a wedged controller cannot hang the
	// segment past the COPY lifetime.
	var conn net.Conn = raw
	if transfer.TLSEnabled(*sslCert, *sslKey, *sslCA) {
		host, herr := hostOf(*listen)
		if herr != nil {
			fail("tls: bad --listen %q: %v", *listen, herr)
		}
		ccfg, cerr := transfer.ClientTLSConfig(host, *sslCert, *sslKey, *sslCA, "")
		if cerr != nil {
			fail("tls: %v", cerr)
		}
		tc := tls.Client(raw, ccfg)
		if hsErr := tc.HandshakeContext(ctx); hsErr != nil {
			fail("tls handshake with controller: %v", hsErr)
		}
		conn = tc
	}
	// Inc3: frame-ack transport (identity HELLO: this slice's StreamID).
	// The pipe owns the conn (clean FIN on close), so the raw conn is not
	// closed separately.
	dataConn := wrapFrameAck(ctx, fail, conn, *listen, *sslCert, *sslKey, *sslCA, *ackTimeout, *maxReconnects, *retransmitMax, frameAckOn(*frameAck),
		transfer.StreamID{JobID: uint32(jobID), UnitID: uint32(unitID), Content: int16(contentID)})
	var rows int64
	if *compressMode != "" {
		// P4: the two-agent on_segment path — the destination agent
		// decompresses each slice with the matching decoder.
		mode, level, perr := compress.Parse(*compressMode)
		if perr != nil {
			fail("compress %q: %v", *compressMode, perr)
		}
		rows, err = segstream.SendCompressed(ctx, dataConn, os.Stdin, uint32(jobID), uint32(unitID), int16(contentID), mode, level, true)
	} else {
		rows, err = segstream.Send(ctx, dataConn, os.Stdin, uint32(jobID), uint32(unitID), int16(contentID), true)
	}
	if err != nil {
		fail("stream: %v", err)
	}
	fmt.Fprintf(os.Stderr, "gpstream-segreceiver: content=%d rows=%d ok\n", contentID, rows)
}

// frameAckOn reports whether the frame-ack transport is in effect.
func frameAckOn(v string) bool { return v != "off" }

// wrapFrameAck upgrades a dialed data connection to a frame-ack ReliablePipe
// (identity HELLO: job/unit/content so the destination's router can route a
// resume re-dial to this slice's own pipe). The dialer reproduces the full
// connection setup (dial + TLS) for ONE attempt on a break — the destination
// receiver is already blocked in its resume accept (ResumeTimeout window), so
// a live destination accepts and a dead one refuses, which must fail the
// slice fast (no retry storm). With frame-ack off it returns the plain conn.
func wrapFrameAck(ctx context.Context, fail func(string, ...any), raw net.Conn, addr, cert, key, ca, ackTimeout string, maxReconnects int, retransmitMax int64, on bool, id transfer.StreamID) transfer.Pipe {
	if !on {
		return raw
	}
	dialOnce := func() (transfer.DupConn, error) {
		r, err := net.DialTimeout("tcp", addr, 10*time.Second)
		if err != nil {
			return nil, err
		}
		var conn net.Conn = r
		if transfer.TLSEnabled(cert, key, ca) {
			host, herr := hostOf(addr)
			if herr != nil {
				_ = r.Close()
				return nil, fmt.Errorf("tls: bad address %q: %v", addr, herr)
			}
			ccfg, cerr := transfer.ClientTLSConfig(host, cert, key, ca, "")
			if cerr != nil {
				_ = r.Close()
				return nil, fmt.Errorf("tls: %v", cerr)
			}
			tc := tls.Client(r, ccfg)
			if hsErr := tc.HandshakeContext(ctx); hsErr != nil {
				_ = r.Close()
				return nil, fmt.Errorf("tls handshake: %v", hsErr)
			}
			conn = tc
		}
		return conn, nil
	}
	rp, err := transfer.NewReliablePipeSenderOnConnID(
		transfer.NewReliablePipeConfig(ackTimeout, maxReconnects, retransmitMax), raw, dialOnce, id)
	if err != nil {
		fail("frame-ack transport: %v", err)
	}
	return rp
}

// hostOf returns the host part of a host:port listen address (the value the
// TLS client uses as its expected controller name, if it is a name).
func hostOf(listen string) (string, error) {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	return host, nil
}

// runRouted is the P9 routed mode: this segreceiver (running on source
// segment s) routes its local slice to D destination segments, fanning each
// (s,d) pair over W sub-streams, and streams them (compressed, content-tagged
// with the DESTINATION segment id) to the destination agent. It dials
// destAddr D×W times (retrying while the agent binds), then hands the
// connection grid to the RoutedSender. Exit 0 on a clean stream; exit 1
// otherwise (a non-zero exit fails the kernel's COPY, so there is no silent
// partial stream).
func runRouted(ctx context.Context, fail func(string, ...any), contentID int64, jobID, unitID uint64, destAddr, routeKey string, routeD, routeWorkers int, compressMode, sslCert, sslKey, sslCA, ackTimeout string, maxReconnects int, retransmitMax int64, on, digest bool) {
	D := routeD
	W := routeWorkers
	// The routing key is embedded by the controller as "oid:pos,oid:pos" —
	// the distribution key's column type OIDs and their positions in the row
	// (internal values, no user input). Single-column keys are "oid:pos".
	keyIdx := make([]int, 0, 8)
	keyOid := make([]uint32, 0, 8)
	for _, part := range strings.Split(routeKey, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		i := strings.IndexByte(part, ':')
		if i < 0 {
			fail("route-key: bad key spec %q (want oid:pos[,oid:pos...])", part)
		}
		oid, perr1 := strconv.ParseUint(part[:i], 10, 32)
		pos, perr2 := strconv.Atoi(part[i+1:])
		if perr1 != nil || perr2 != nil || pos < 0 {
			fail("route-key: bad oid:pos pair %q", part)
		}
		keyOid = append(keyOid, uint32(oid))
		keyIdx = append(keyIdx, pos)
	}
	if len(keyOid) == 0 {
		fail("route-key: no keys parsed")
	}

	// P4 mesh routed: --dest-addr is either one host:port (broadcast to the
	// destination AGENT, P9) or D comma-separated host:port entries (one per
	// destination segment's MINI-AGENT data endpoint, mesh). The grid maps
	// sub-stream (d, w) -> entry d; each sub-stream dials grid[d].
	grid, gerr := segstream.ParseDestAddrs(destAddr, D)
	if gerr != nil {
		fail("dest-addr: %v", gerr)
	}

	mode, level, perr := compress.Parse(compressMode)
	if perr != nil {
		if compressMode == "" {
			mode, level = compress.ModeNone, 0
		} else {
			fail("compress %q: %v", compressMode, perr)
		}
	}

	// Dial the destination agent D×W times (one sub-stream per (d,w)). The
	// agent may still be binding its listen socket (the controller launches
	// it right before the source agent triggers the ON SEGMENT dispatch), so
	// retry the dials briefly.
	conns := make([][]net.Conn, D)
	open := 0
	dialAll := func() error {
		for d := 0; d < D; d++ {
			conns[d] = make([]net.Conn, W)
			for w := 0; w < W; w++ {
				raw, derr := net.DialTimeout("tcp", grid[d], 10*time.Second)
				if derr != nil {
					return fmt.Errorf("dest segment %d: %w", d, derr)
				}
				var conn net.Conn = raw
				if transfer.TLSEnabled(sslCert, sslKey, sslCA) {
					host, herr := hostOf(grid[d])
					if herr != nil {
						_ = raw.Close()
						return fmt.Errorf("tls: bad --dest-addr %q (segment %d): %v", grid[d], d, herr)
					}
					ccfg, cerr := transfer.ClientTLSConfig(host, sslCert, sslKey, sslCA, "")
					if cerr != nil {
						_ = raw.Close()
						return fmt.Errorf("tls: %v", cerr)
					}
					tc := tls.Client(raw, ccfg)
					if hsErr := tc.HandshakeContext(ctx); hsErr != nil {
						_ = raw.Close()
						return fmt.Errorf("tls handshake with destination agent: %v", hsErr)
					}
					conn = tc
				}
				conns[d][w] = conn
				open++
			}
		}
		return nil
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := dialAll(); err == nil {
			break
		} else {
			// Close any partially opened connections before retrying.
			for d := 0; d < D; d++ {
				for w := 0; w < W; w++ {
					if conns[d] != nil && conns[d][w] != nil {
						_ = conns[d][w].Close()
					}
				}
			}
			open = 0
			if time.Now().After(deadline) {
				fail("connect %s (retries exhausted): %v", destAddr, err)
			}
			select {
			case <-ctx.Done():
				fail("connect %s: %v", destAddr, ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	defer func() {
		for d := 0; d < D; d++ {
			for w := 0; w < W; w++ {
				_ = conns[d][w].Close()
			}
		}
	}()

	// Inc3: frame-ack transport per sub-stream (identity HELLO:
	// (src-content, dst-content=d, worker=w) so the destination's router
	// routes each re-dial to its own pipe).
	dataConns := make([][]transfer.Pipe, D)
	for d := 0; d < D; d++ {
		dataConns[d] = make([]transfer.Pipe, W)
		for w := 0; w < W; w++ {
			dataConns[d][w] = wrapFrameAck(ctx, fail, conns[d][w], grid[d], sslCert, sslKey, sslCA, ackTimeout, maxReconnects, retransmitMax, on,
				transfer.StreamID{JobID: uint32(jobID), UnitID: uint32(unitID), Content: int16(contentID), DstContent: int16(d), Worker: int32(w)})
		}
	}
	sender := segstream.NewRoutedSender(ctx, os.Stdin, keyIdx, keyOid, int32(D), W, uint32(jobID), uint32(unitID), int16(contentID), mode, level, true, dataConns)
	rows, err := sender.Run()
	if err != nil {
		fail("route: %v", err)
	}
	fmt.Fprintf(os.Stderr, "gpstream-segreceiver: routed content=%d rows=%d dest=%d workers=%d ok\n", contentID, rows, D, W)
}
