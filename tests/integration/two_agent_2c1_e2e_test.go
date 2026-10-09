//go:build gp6lab

package integration

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestDryRunEndToEndTwoAgent (2C-1/2/4/5, architecture 08.10) drives the REAL
// `gpstream dry-run` CLI end to end: the controller launches BOTH agents,
// gathers the two sides' dry-run values over the control plane (the agents
// read their OWN local databases), diffs them in memory, builds the plan ON
// THE SOURCE AGENT, and releases the agents — with the controller holding NO
// direct database connection and the destination database UNTOUCHED (2C-5:
// read-only). The agents are the binaries built next to the gpstream binary
// (resolveSibling), so this is the production launch path.
func TestDryRunEndToEndTwoAgent(t *testing.T) {
	const (
		srcPort = 5791
		dstPort = 5792
	)
	bin := buildTwoAgentBinaries(t)

	ctx := context.Background()
	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	// Source: two tables (the plan must see them). Dest: empty (a dry-run must
	// create nothing).
	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(src.Close)
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(dst.Close)
	mustExec(t, src, `CREATE TABLE public.alpha (id int, val text)`)
	mustExec(t, src, `INSERT INTO public.alpha SELECT g, 'a' || g FROM generate_series(1, 50) g`)
	mustExec(t, src, `CREATE TABLE public.bravo (id int, val text)`)
	mustExec(t, src, `INSERT INTO public.bravo SELECT g, 'b' || g FROM generate_series(1, 20) g`)

	// Run the real CLI: `gpstream dry-run --source <url> --dest <url>`.
	cmd := exec.Command(bin.gpstream, "dry-run",
		"--source", srcCl.URL(), "--dest", dstCl.URL(), "--timeout", "120s")
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("dry-run CLI failed: %v\n--- output ---\n%s", err, out.String())
	}
	o := out.String()

	// The report + the plan summary must be printed.
	if !strings.Contains(o, "gpstream dry-run") {
		t.Errorf("dry-run report banner missing:\n%s", o)
	}
	if !strings.Contains(o, "coordinator reachable") {
		t.Errorf("both sides must report reachability PASS:\n%s", o)
	}
	if !strings.Contains(o, "units:") {
		t.Errorf("plan summary (units:) missing:\n%s", o)
	}

	// 2C-5: the destination database is UNTOUCHED (read-only) — no tables, no
	// staging, no journal.
	// The destination's public schema must be EMPTY (the dry-run created
	// nothing: no staging, no journal, no swapped tables). Match the schema
	// BY NAME (its OID varies across clusters — 11 is pg_catalog, not public).
	got := mustCount(t, dst,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE c.relkind IN ('r','p') AND n.nspname = 'public'`)
	if got != 0 {
		t.Fatalf("dry-run must not create any table on the destination (2C-5), got %d", got)
	}

}
