package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// cmdBench runs `gpstream bench` (F-14 input-stream benchmark). It streams
// the SELECTED source tables through the real data path (COPY ... TO STDOUT
// -> frame v2 -> compression -> adaptive rate limiter) and DISCARDS the
// payload instead of writing a destination, so the operator measures the
// input stream's production rate (source read + framing + compression)
// without a destination cluster. All selection/perf options (CLI > env >
// toml) are honored exactly as for `run`; --dest is not used (and, if given,
// ignored with a note). --timeout is the benchmark's wall-time limit:
// unlimited when not passed, since a benchmark capped by default would
// truncate the very number it reports.
func cmdBench(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		if errors.Is(err, errHelpRequested) {
			return printHelp()
		}
		return err
	}
	if cfg.Source == "" {
		return fmt.Errorf("bench: --source <connstr> is required (the destination is not used)")
	}
	if cfg.Dest != "" {
		fmt.Println("bench: note: --dest is ignored — the input-stream benchmark reads the source only and discards the payload")
	}
	if cfg.MetadataOnly {
		return fmt.Errorf("bench: --metadata-only has no data to measure; run it without the flag")
	}

	ctx := context.Background()
	// Same pool sizing as a run: the data path runs WorkersPerPair parallel
	// stream workers; the pool also serves the planning queries and the
	// (default --consistent-db) snapshot pin.
	maxConns := cfg.WorkersPerPair + 4
	srcQ, err := pg.NewPool(ctx, cfg.Source, cfg.SSLCert, cfg.SSLKey, cfg.SSLCA, cfg.SSLMinTLS, maxConns)
	if err != nil {
		return fmt.Errorf("bench: connect source %q: %w", cfg.Source, err)
	}
	defer srcQ.Close()

	// Planning phase (selection + volume) is bounded by --timeout, exactly
	// like run/dry-run.
	plctx, cancelPlan := context.WithTimeout(ctx, cfg.Timeout)
	units, err := plan.Bench(plctx, &cfg, srcQ)
	cancelPlan()
	if err != nil {
		return fmt.Errorf("bench: %w", err)
	}

	comp := string(cfg.Compression)
	limiter := "none"
	if cfg.RateLimit > 0 {
		limiter = fmt.Sprintf("%.3g Gbps (adaptive)", cfg.RateLimit)
	}
	timeout := "unlimited"
	if d := cfg.BenchTimeout(); d > 0 {
		timeout = d.String()
	}

	fmt.Println("gpstream bench")
	fmt.Println("==============")
	fmt.Println("mode:           input stream only (source read -> frame -> compress -> discard; no destination)")
	fmt.Printf("units:          %d table(s), estimated %s\n", len(units), plan.BenchSummaryLine(units))
	fmt.Printf("workers:        %d\n", cfg.WorkersPerPair)
	fmt.Printf("compression:    %s\n", comp)
	fmt.Printf("rate-limit:     %s\n", limiter)
	fmt.Printf("timeout:        %s\n", timeout)
	if cfg.ConsistentDB || cfg.ConsistentCluster || cfg.ConsistentTables {
		fmt.Println("snapshot:       on (source reads share one REPEATABLE READ snapshot, as in a run)")
	}

	// The benchmark drives the real Manager data path with the destination
	// agent inert: no destination connection, no DDL, no journal writes.
	// The journal is a bare file-less instance (events/metrics paths empty).
	mgr := manifest.NewManager(&cfg, srcQ, nil, manifest.NewJournal(nil, "")).Bench()

	benchCtx := ctx
	var cancelBench context.CancelFunc
	if d := cfg.BenchTimeout(); d > 0 {
		benchCtx, cancelBench = context.WithTimeout(ctx, d)
		defer cancelBench()
	}

	fmt.Println()
	st, berr := mgr.BenchAll(benchCtx, units)
	if st == nil {
		return fmt.Errorf("bench: %w", berr)
	}

	// Per-table table (always emitted, even when a unit failed or the
	// deadline cut the run short — the operator wants the partial picture).
	const hdr = "table                              rows          wire       logical   elapsed     rate"
	fmt.Println(hdr)
	fmt.Println(strings.Repeat("-", len(hdr)))
	for _, r := range st.Results {
		name := r.Schema + "." + r.Name
		if len(name) > 30 {
			name = "…" + name[len(name)-29:]
		}
		status := ""
		if r.TimedOut {
			status = "  [timeout]"
		} else if r.Err != nil {
			status = "  [ERROR]"
		} else if r.CompOff {
			status = "  [comp-off]"
		}
		rate := "-"
		if r.Elapsed > 0 && r.Logical > 0 {
			rate = fmt.Sprintf("%.0f MB/s", float64(r.Logical)/r.Elapsed.Seconds()/1e6)
		}
		fmt.Printf("%-30s %10d %12s %12s %8s %8s%s\n",
			name, r.Rows, humanBytesU(r.Wire), humanBytesU(r.Logical),
			humanDur(r.Elapsed), rate, status)
		if r.Err != nil {
			fmt.Printf("  error: %v\n", r.Err)
		}
	}
	fmt.Println(strings.Repeat("-", len(hdr)))
	totalRate := "-"
	if st.Elapsed > 0 && st.TotalLogical > 0 {
		totalRate = fmt.Sprintf("%.0f MB/s", st.LogicalRate()/1e6)
	}
	fmt.Printf("%-30s %10d %12s %12s %8s %8s\n",
		fmt.Sprintf("TOTAL (%d)", len(st.Results)), st.TotalRows,
		humanBytesU(st.TotalWire), humanBytesU(st.TotalLogical),
		humanDur(st.Elapsed), totalRate)

	fmt.Printf("\nbench: elapsed %s, input-stream rate %.0f MB/s (logical), %.0f MB/s (wire)\n",
		humanDur(st.Elapsed), st.LogicalRate()/1e6, st.WireRate()/1e6)
	if st.TimedOut {
		fmt.Printf("bench: STOPPED by --timeout %s — partial results above\n", cfg.BenchTimeout())
	}
	if berr != nil {
		return fmt.Errorf("bench: %w", berr)
	}
	return nil
}

// humanDur renders a duration as "350ms" / "12.3s" / "1m05s" for the bench
// table.
func humanDur(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.0fms", float64(d.Microseconds())/1e3)
	}
	if d < 90*time.Second {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.Round(time.Second).String()
}
