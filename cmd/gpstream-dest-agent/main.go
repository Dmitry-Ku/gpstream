// Command gpstream-dest-agent is the two-agent data-path destination
// (ADR-001 / ADR-002 remediation). It runs on a destination-cluster host: it
// listens for the source agent's connection, reads the UnitSpec header,
// builds the staging table, loads it from the COMPRESSED framed stream
// (decompressing on the wire, FR-12) and — when instructed by the controller
// (--swap) — performs the atomic swap (ALTER ... RENAME). It is the
// destination half of the network data path the single-controller build does
// in memory.
//
// The controller (P3) launches it per unit (or keeps it alive for a
// multi-unit run). For this phase it serves exactly one unit per invocation.
//
// Exit codes: 0 clean serve (load complete, swap done when requested); 1
// argument/accept/stream failure (the controller surfaces it as the unit
// error; a non-zero exit never leaves a silent partial load).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "listen address host:port (required; the controller passes a bound port)")
	// Destination pg connection (the agent is a client of the destination DB).
	dest := flag.String("dest", "", "destination postgres conn string (required)")
	sslCert := flag.String("ssl-cert", "", "pg-plane client certificate PEM")
	sslKey := flag.String("ssl-key", "", "pg-plane client key PEM")
	sslCA := flag.String("ssl-ca", "", "pg-plane trusted CA bundle PEM (verify-full)")
	sslMinTLS := flag.String("ssl-min-tls", "1.2", "pg-plane minimum TLS version")
	// Data-plane TLS for the agent<->agent socket (dest agent = server).
	dataCert := flag.String("data-tls-cert", "", "agent-stream serving certificate PEM")
	dataKey := flag.String("data-tls-key", "", "agent-stream serving key PEM")
	dataCA := flag.String("data-tls-ca", "", "agent-stream trusted CA bundle PEM (enables mTLS: require the source's client cert)")
	dataMinTLS := flag.String("data-tls-min-tls", "1.2", "agent-stream minimum TLS version")
	// Control-channel TLS (the controller's dial to this agent's control
	// socket; this agent = server). CA present = mTLS (require the
	// controller's client certificate).
	ctrlCert := flag.String("control-tls-cert", "", "control-socket serving certificate PEM")
	ctrlKey := flag.String("control-tls-key", "", "control-socket serving key PEM")
	ctrlCA := flag.String("control-tls-ca", "", "control-socket trusted CA bundle PEM (enables mTLS)")
	// Swap is a controller decision (the journal's terminal-state semantics
	// live in the controller, ADR-002 plan).
	swap := flag.Bool("swap", false, "perform the atomic swap (rename staging -> final) after the load")
	// P4 on_segment mode: serve N compressed segment slices (merging them
	// with SliceMux) instead of one coordinator stream. The kernel-launched
	// segreceivers cannot carry the unit shape, so the controller passes it
	// at launch as a single UnitSpec JSON (--spec): destination schema /
	// table / staging, the source columns + distribution + partition, and
	// the FR-12 compress mode.
	segments := flag.Int("segments", 0, "P4 on_segment mode: number of source segments to accept (0 = coordinator mode)")
	segSpec := flag.String("spec", "", "P4 on_segment mode: UnitSpec JSON (the controller serialises it; required with --segments)")
	// P5 direct mode: the controller passes the launch payload (staging shape
	// + D + the per-segment utility-mode DSNs) as a single DirectLaunch JSON.
	// The agent accepts D content-tagged compressed slice connections and
	// performs D utility-mode per-segment COPYs into the staging table.
	directSpec := flag.String("direct-spec", "", "P5 direct mode: DirectLaunch JSON (the controller serialises it)")
	// P9 routed mode (S≠D): the controller passes the routed launch payload
	// (staging shape + S + D + W + per-segment utility-mode DSNs) as a single
	// RoutedLaunch JSON. The agent accepts S×D×W sub-stream connections and
	// performs D utility-mode per-segment COPYs (per-segment sub-stream
	// merge).
	routedSpec := flag.String("routed-spec", "", "P9 routed mode (S≠D): RoutedLaunch JSON (the controller serialises it)")
	// Timeouts.
	acceptTO := flag.Duration("accept-timeout", 5*time.Minute, "wait for the source agent's connection")
	streamTO := flag.Duration("stream-timeout", 6*time.Hour, "whole-stream timeout")
	// Doctor probe: print a marker and exit 0 (no serve), proving the binary
	// is installed on the destination host.
	check := flag.Bool("check", false, "print a marker and exit 0 (doctor binary probe)")
	// --serve = LONG-LIVED mode (design 04.10): the agent binds ONE data
	// listener + ONE control listener for the whole run (port economy) and
	// serves units over the control protocol (hello / serve-unit /
	// unit-done / release) instead of one-shot single-unit modes.
	serve := flag.Bool("serve", false, "long-lived mode: serve units over the control protocol (one data + one control listener for the run)")
	dataListen := flag.String("data-listen", "0.0.0.0:0", "long-lived mode: data listener address host:port (0 = pick a free port; the real address is reported in the hello)")
	ctrlListen := flag.String("control-listen", "0.0.0.0:0", "long-lived mode: control listener address host:port (0 = pick a free port)")
	flag.Parse()

	fail := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "gpstream-dest-agent: "+format+"\n", a...)
		os.Exit(1)
	}
	if *check {
		fmt.Fprintln(os.Stderr, "gpstream-dest-agent: ok (check)")
		os.Exit(0)
	}

	// SIGINT/SIGTERM cancels the serve so a controller-side stop unwinds the
	// COPY and the socket cleanly.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if *serve {
		// Long-lived mode: one data + one control listener for the run.
		if *dest == "" {
			fail("--dest is required")
		}
		srv, err := agent.NewDestServer(agent.DestServerOptions{
			DataListen:     *dataListen,
			ControlListen:  *ctrlListen,
			DestConn:       *dest,
			SSLCert:        *sslCert,
			SSLKey:         *sslKey,
			SSLCA:          *sslCA,
			SSLMinTLS:      *sslMinTLS,
			DataTLSCert:    *dataCert,
			DataTLSKey:     *dataKey,
			DataTLSCA:      *dataCA,
			DataTLSMinTLS:  *dataMinTLS,
			ControlTLSCert: *ctrlCert,
			ControlTLSKey:  *ctrlKey,
			ControlTLSCA:   *ctrlCA,
			AcceptTimeout:  *acceptTO,
			StreamTimeout:  *streamTO,
		})
		if err != nil {
			fail("start long-lived dest server: %v", err)
		}
		// Report the real bound addresses (auto :0 mode: the operator or
		// the remote launch reads this line to learn the data endpoint).
		fmt.Fprintf(os.Stderr, "gpstream-dest-agent: serve data=%s control=%s ready\n",
			srv.DataAddr(), srv.ControlAddr())
		// Block until the run ends (release / controller disconnect) or a
		// signal.
		select {
		case <-ctx.Done():
		case <-srv.Done():
		}
		srv.Close()
		fmt.Fprintln(os.Stderr, "gpstream-dest-agent: serve stopped")
		os.Exit(0)
	}

	if *dest == "" || *listen == "" {
		fail("--dest and --listen are required")
	}

	if *routedSpec != "" {
		// P9 routed mode (S≠D): accept S×D×W sub-stream connections (one per
		// (source segment, destination segment, worker) pair, each
		// content-tagged with its destination segment) and perform D
		// utility-mode per-segment COPYs with a per-segment sub-stream merge.
		var launch agent.RoutedLaunch
		if err := json.Unmarshal([]byte(*routedSpec), &launch); err != nil {
			fail("parse --routed-spec: %v", err)
		}
		res, err := agent.ServeRouted(ctx, agent.RoutedOptions{
			Listen:        *listen,
			DestConn:      *dest,
			SSLCert:       *sslCert,
			SSLKey:        *sslKey,
			SSLCA:         *sslCA,
			SSLMinTLS:     *sslMinTLS,
			DataTLSCert:   *dataCert,
			DataTLSKey:    *dataKey,
			DataTLSCA:     *dataCA,
			DataTLSMinTLS: *dataMinTLS,
			Launch:        launch,
			Swap:          *swap,
			AcceptTimeout: *acceptTO,
			StreamTimeout: *streamTO,
		})
		if err != nil {
			fail("%v", err)
		}
		verdict := "loaded"
		if res.Swapped {
			verdict = "swapped"
		}
		fmt.Fprintf(os.Stderr, "gpstream-dest-agent: unit=%d rows=%d logical=%d wire=%d %s ok\n",
			res.Launch.UnitID, res.Rows, res.Logical, res.Wire, verdict)
		return
	}

	if *directSpec != "" {
		// P5 direct mode: accept D content-tagged compressed slice connections
		// (one per destination segment) and perform D utility-mode per-segment
		// COPYs into the staging table.
		var launch agent.DirectLaunch
		if err := json.Unmarshal([]byte(*directSpec), &launch); err != nil {
			fail("parse --direct-spec: %v", err)
		}
		res, err := agent.ServeDirect(ctx, agent.DirectOptions{
			Listen:        *listen,
			DestConn:      *dest,
			SSLCert:       *sslCert,
			SSLKey:        *sslKey,
			SSLCA:         *sslCA,
			SSLMinTLS:     *sslMinTLS,
			DataTLSCert:   *dataCert,
			DataTLSKey:    *dataKey,
			DataTLSCA:     *dataCA,
			DataTLSMinTLS: *dataMinTLS,
			Launch:        launch,
			Swap:          *swap,
			AcceptTimeout: *acceptTO,
			StreamTimeout: *streamTO,
		})
		if err != nil {
			fail("%v", err)
		}
		verdict := "loaded"
		if res.Swapped {
			verdict = "swapped"
		}
		fmt.Fprintf(os.Stderr, "gpstream-dest-agent: unit=%d rows=%d logical=%d wire=%d %s ok\n",
			res.Launch.UnitID, res.Rows, res.Logical, res.Wire, verdict)
		return
	}

	if *segments > 0 {
		// P4 on_segment mode: merge N compressed segment slices (SliceMux)
		// and load them into the staging table.
		var spec agent.UnitSpec
		if err := json.Unmarshal([]byte(*segSpec), &spec); err != nil {
			fail("parse --spec: %v", err)
		}
		res, err := agent.ServeSegments(ctx, agent.SegmentsOptions{
			Listen:        *listen,
			DestConn:      *dest,
			SSLCert:       *sslCert,
			SSLKey:        *sslKey,
			SSLCA:         *sslCA,
			SSLMinTLS:     *sslMinTLS,
			DataTLSCert:   *dataCert,
			DataTLSKey:    *dataKey,
			DataTLSCA:     *dataCA,
			DataTLSMinTLS: *dataMinTLS,
			Spec:          spec,
			Segments:      *segments,
			Swap:          *swap,
			AcceptTimeout: *acceptTO,
			StreamTimeout: *streamTO,
		})
		if err != nil {
			fail("%v", err)
		}
		verdict := "loaded"
		if res.Swapped {
			verdict = "swapped"
		}
		fmt.Fprintf(os.Stderr, "gpstream-dest-agent: unit=%d rows=%d logical=%d wire=%d %s ok\n",
			res.Spec.UnitID, res.Rows, res.Logical, res.Wire, verdict)
		return
	}

	res, err := agent.Serve(ctx, agent.DestOptions{
		Listen:        *listen,
		DestConn:      *dest,
		SSLCert:       *sslCert,
		SSLKey:        *sslKey,
		SSLCA:         *sslCA,
		SSLMinTLS:     *sslMinTLS,
		DataTLSCert:   *dataCert,
		DataTLSKey:    *dataKey,
		DataTLSCA:     *dataCA,
		DataTLSMinTLS: *dataMinTLS,
		Swap:          *swap,
		AcceptTimeout: *acceptTO,
		StreamTimeout: *streamTO,
	})
	if err != nil {
		fail("%v", err)
	}
	// The report goes to stderr (stdout is not a data path here). The
	// controller (P3) parses this to journal the unit (rows + logical are the
	// authoritative destination counts).
	verdict := "loaded"
	if res.Swapped {
		verdict = "swapped"
	}
	fmt.Fprintf(os.Stderr, "gpstream-dest-agent: unit=%d rows=%d logical=%d %s ok\n",
		res.Spec.UnitID, res.Rows, res.Logical, verdict)
}
