//go:build gp6lab

package integration

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/dryrun"
	"github.com/gpstream/gpstream/internal/pgtest"
)

// TestDryRunChecksWireRoundTrip (2C-3, architecture 08.10) proves the new
// AGENT-SIDE dry-run-checks control op over the REAL control sockets (the
// production path): the source agent reads its side's values on its LOCAL
// source pool, the destination agent on a fresh LOCAL coordinator connection —
// and the controller collects both and diffs them in memory (dryrun.FromSides)
// with NO direct database connection of its own. The values are JSON-safe: the
// wire round-trip is value-identical.
func TestDryRunChecksWireRoundTrip(t *testing.T) {
	const (
		srcPort = 5781
		dstPort = 5782
	)
	ctx := context.Background()

	srcCl, err := pgtest.Start(srcPort, "gp_dryrun_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "gp_dryrun_dst")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	// SOURCE agent over a real control socket (the production path).
	srcCtrl := freeAddr(t)
	srv := agent.NewSourceServer(agent.ServerOptions{
		SrcConn:  srcCl.URL(),
		Snapshot: agent.SnapNone, // no FR-02 snapshot: dry-run reads only
		Listen:   srcCtrl,
	})
	if _, err := srv.Start(ctx); err != nil {
		t.Fatalf("start source agent: %v", err)
	}
	t.Cleanup(srv.Close)
	sclient, err := agent.DialControl(ctx, srcCtrl)
	if err != nil {
		t.Fatalf("dial source control: %v", err)
	}
	t.Cleanup(func() { sclient.Close() })

	srcResp, err := sclient.DryRunChecks(agent.DryRunChecksReq{Side: "source", Workers: 1})
	if err != nil {
		t.Fatalf("source dry-run-checks: %v", err)
	}
	if !srcResp.Ok || srcResp.Side == nil {
		t.Fatalf("source dry-run-checks not ok: %s (side=%v)", srcResp.Error, srcResp.Side)
	}
	srcSV := srcResp.Side
	if !srcSV.Reachable {
		t.Fatalf("source side not reachable: %s", srcSV.ReachErr)
	}
	if srcSV.Version == "" {
		t.Fatalf("source side version empty (the reachability ping passed but version() failed)")
	}
	if srcSV.Greenplum {
		t.Fatalf("pgtest source is plain PostgreSQL, must not report Greenplum")
	}
	if !srcSV.DiskOK {
		t.Fatalf("source side disk probe failed: %s", srcSV.DiskErr)
	}

	// DESTINATION agent over a real control socket (coordinator mode).
	dataAddr := freeAddr(t)
	dstCtrl := freeAddr(t)
	dsrv, err := agent.NewDestServer(agent.DestServerOptions{
		DataListen:    dataAddr,
		ControlListen: dstCtrl,
		DestConn:      dstCl.URL(),
		AcceptTimeout: 2 * time.Second,
		StreamTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDestServer: %v", err)
	}
	t.Cleanup(dsrv.Close)
	dclient, err := agent.DialDestControl(ctx, dstCtrl)
	if err != nil {
		t.Fatalf("DialDestControl: %v", err)
	}
	t.Cleanup(func() { dclient.Close() })

	dstResp, err := dclient.DryRunChecks(agent.DryRunChecksReq{Side: "dest", Workers: 1})
	if err != nil {
		t.Fatalf("dest dry-run-checks: %v", err)
	}
	if !dstResp.Ok || dstResp.Side == nil {
		t.Fatalf("dest dry-run-checks not ok: %s (side=%v)", dstResp.Error, dstResp.Side)
	}
	dstSV := dstResp.Side
	if !dstSV.Reachable {
		t.Fatalf("dest side not reachable: %s", dstSV.ReachErr)
	}
	if dstSV.Version == "" {
		t.Fatalf("dest side version empty")
	}
	if !dstSV.DiskOK {
		t.Fatalf("dest side disk probe failed: %s", dstSV.DiskErr)
	}

	// The values are JSON-safe: marshal/unmarshal must be value-identical.
	if !sideValuesJSONStable(t, srcSV) || !sideValuesJSONStable(t, dstSV) {
		t.Fatalf("SideValues is not JSON-stable over the wire round-trip")
	}

	// The controller diffs the two sides IN MEMORY (no DB access). Both are
	// plain PostgreSQL here, so the report is healthy (single-node WARNs).
	rep := dryrun.FromSides(srcSV, dstSV, dryrun.Options{MinTLS: "1.2", Workers: 1})
	if !rep.Healthy {
		t.Fatalf("plain-PG dry-run must be healthy, got %d FAILs: %+v", rep.Failures(), rep.Sorted())
	}
}

// sideValuesJSONStable marshals+unmarshals sv and compares it to the original
// (the wire round-trip must be value-identical — no function fields, no
// unmarshal-time defaults that diverge).
func sideValuesJSONStable(t *testing.T, sv *dryrun.SideValues) bool {
	t.Helper()
	b, err := json.Marshal(sv)
	if err != nil {
		t.Fatalf("marshal SideValues: %v", err)
	}
	var back dryrun.SideValues
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal SideValues: %v", err)
	}
	b2, _ := json.Marshal(&back)
	if string(b) != string(b2) {
		t.Fatalf("SideValues not JSON-stable:\n  first: %s\n  round: %s", b, b2)
	}
	return true
}

// freeAddr returns a free loopback host:port (listen, capture, close) for a
// control/data listener.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
