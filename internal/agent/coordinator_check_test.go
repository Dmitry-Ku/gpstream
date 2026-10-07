//go:build gp6lab

package agent

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// These tests prove the P1e coordinator guard (requirement 2 — 30986/30989):
// the agent must run on the coordinator of EXACTLY the cluster its DSN points
// to. A local pgtest postgres has no gp_segment_configuration, so the
// "wrong cluster" and "segment" cases are simulated by creating that catalog
// table with the rows a real Greenplum cluster would report.

const coordTableDDL = `CREATE TABLE gp_segment_configuration (
	hostname text,
	address  text,
	port     int,
	content  int
)`

// TestCheckOnCoordinatorRemoteDSN: a DSN whose host is not this machine is a
// hard error BEFORE any connection — the agent was started on the wrong host
// (the 07.10 incident topology). No live pg needed.
func TestCheckOnCoordinatorRemoteDSN(t *testing.T) {
	dsn := "host=192.0.2.99 port=5432 user=gp dbname=d sslmode=disable"
	err := CheckOnCoordinator(context.Background(), dsn, "", "", "", "")
	if err == nil {
		t.Fatal("expected error for remote DSN host, got nil")
	}
	if !strings.Contains(err.Error(), "remote") {
		t.Fatalf("expected 'remote' in error, got: %v", err)
	}
}

// TestCheckOnCoordinatorLocalNonGP: a local DSN against a plain (non-Greenplum)
// postgres passes — the connected node is the coordinator by definition.
func TestCheckOnCoordinatorLocalNonGP(t *testing.T) {
	cl, err := pgtest.Start(5690, "gp_coord_nongp")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(cl.Stop)
	if err := CheckOnCoordinator(context.Background(), cl.URL(), "", "", "", ""); err != nil {
		t.Fatalf("expected pass for local non-GP cluster, got: %v", err)
	}
}

// TestCheckOnCoordinatorCorrectCluster: a local DSN whose cluster's catalog
// names THIS host as its coordinator (content=-1 row) and whose connected
// backend port is the coordinator port → pass.
func TestCheckOnCoordinatorCorrectCluster(t *testing.T) {
	cl, err := pgtest.Start(5691, "gp_coord_ok")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(cl.Stop)
	if err := seedCoordTable(cl.URL(), cl.Port, "localhost", "127.0.0.1"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := CheckOnCoordinator(context.Background(), cl.URL(), "", "", "", ""); err != nil {
		t.Fatalf("expected pass for correct cluster, got: %v", err)
	}
}

// TestCheckOnCoordinatorWrongCluster: the DSN points to a cluster whose
// catalog names a DIFFERENT host as its coordinator (both hostname and
// address are foreign), while this agent runs on a local postgres. This is
// exactly the 30986 scenario (DSN = cluster X, agent launched on cluster Y's
// coordinator) and MUST be rejected.
func TestCheckOnCoordinatorWrongCluster(t *testing.T) {
	cl, err := pgtest.Start(5692, "gp_coord_wrong")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(cl.Stop)
	if err := seedCoordTable(cl.URL(), cl.Port, "some-other-coordinator-host", "192.0.2.50"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err = CheckOnCoordinator(context.Background(), cl.URL(), "", "", "", "")
	if err == nil {
		t.Fatal("expected error when DSN's cluster coordinator is not this host, got nil")
	}
	if !strings.Contains(err.Error(), "coordinator") {
		t.Fatalf("expected 'coordinator' in error, got: %v", err)
	}
}

// TestCheckOnCoordinatorSegment: the DSN's cluster coordinator is this host,
// but the backend we connected to is a SEGMENT (content >= 0 for this port) →
// the agent is on the wrong node of the right cluster → reject.
func TestCheckOnCoordinatorSegment(t *testing.T) {
	cl, err := pgtest.Start(5693, "gp_coord_seg")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(cl.Stop)
	// Coordinator row points at a DIFFERENT port so it doesn't match this
	// backend; this backend's port is a segment (content=0).
	if err := execDDL(cl.URL(), coordTableDDL); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	if err := execDDL(cl.URL(), fmt.Sprintf(
		"INSERT INTO gp_segment_configuration VALUES ('localhost','127.0.0.1',%d,-1), ('localhost','127.0.0.1',%d,0)",
		cl.Port+1000, cl.Port)); err != nil {
		t.Fatalf("seed segment: %v", err)
	}
	err = CheckOnCoordinator(context.Background(), cl.URL(), "", "", "", "")
	if err == nil {
		t.Fatal("expected error when connected node is a segment, got nil")
	}
	if !strings.Contains(err.Error(), "content=") {
		t.Fatalf("expected 'content=' in error, got: %v", err)
	}
}

// seedCoordTable creates gp_segment_configuration with a single coordinator
// row (content=-1) whose hostname/address are the given foreign-or-local
// values and whose port is this cluster's port (so step 4 sees the
// coordinator for this backend).
func seedCoordTable(dsn string, port uint32, coordHostname, coordAddress string) error {
	if err := execDDL(dsn, coordTableDDL); err != nil {
		return err
	}
	return execDDL(dsn, fmt.Sprintf(
		"INSERT INTO gp_segment_configuration VALUES ('%s','%s',%d,-1)",
		coordHostname, coordAddress, port))
}

func execDDL(dsn, sql string) error {
	ctx := context.Background()
	conn, err := pg.NewConn(ctx, dsn, "", "", "", "")
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec(ctx, sql)
	return err
}
