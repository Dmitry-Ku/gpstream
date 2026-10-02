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
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

func main() {
	listen := flag.String("listen", "", "controller listen address host:port (required)")
	job := flag.String("job", "0", "job id (decimal)")
	unit := flag.String("unit", "0", "unit id (decimal)")
	content := flag.String("content", "", "segment content id (decimal; the kernel substitutes <SEGID>)")
	check := flag.Bool("check", false, "print a marker and exit 0 (doctor binary probe; no stream)")
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
	flag.Parse()

	fail := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "gpstream-segreceiver: "+format+"\n", a...)
		os.Exit(1)
	}
	if *check {
		// The doctor runs `COPY (SELECT 1) TO PROGRAM 'gpstream-segreceiver
		// --check --content <SEGID>' ON SEGMENT`: a clean exit 0 on every
		// segment proves the binary is installed on all segment hosts (a
		// missing binary makes the kernel fail the COPY with
		// "could not execute file"). The marker goes to stderr (the segment
		// server log) — stdout belongs to the COPY data path.
		fmt.Fprintln(os.Stderr, "gpstream-segreceiver: ok (check)")
		os.Exit(0)
	}
	if *listen == "" {
		fail("--listen host:port is required")
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

	// The kernel runs us with a bounded lifetime (the COPY transaction);
	// bound our own connect + stream accordingly so a wedged controller
	// cannot hang the segment forever.
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

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
	var rows int64
	if *compressMode != "" {
		// P4: the two-agent on_segment path — the destination agent
		// decompresses each slice with the matching decoder.
		mode, level, perr := compress.Parse(*compressMode)
		if perr != nil {
			fail("compress %q: %v", *compressMode, perr)
		}
		rows, err = segstream.SendCompressed(ctx, conn, os.Stdin, uint32(jobID), uint32(unitID), int16(contentID), mode, level)
	} else {
		rows, err = segstream.Send(ctx, conn, os.Stdin, uint32(jobID), uint32(unitID), int16(contentID))
	}
	if err != nil {
		fail("stream: %v", err)
	}
	fmt.Fprintf(os.Stderr, "gpstream-segreceiver: content=%d rows=%d ok\n", contentID, rows)
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
