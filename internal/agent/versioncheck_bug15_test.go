package agent

import (
	"context"
	"fmt"
	"testing"

	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestGatherSourceVersionNoSegBinary (BUG #15): in the coordinator copy mode
// cfg.SegBinary is empty (segreceivers are part of the mesh data path only).
// gatherSourceVersion must then report the segment count WITHOUT SSHing the
// segments for `--version` (the old code ran `” --version` remotely and
// failed the whole build-plan with "exit status 127" on a Greenplum source).
func TestGatherSourceVersionNoSegBinary(t *testing.T) {
	cl, err := pgtest.Start(5705, "gp_segver_noseg")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(cl.Stop)

	// A fake Greenplum catalog: the source agent's query filters role = 'p'.
	ddl := `CREATE TABLE gp_segment_configuration (
		hostname text,
		address  text,
		port     int,
		content  int,
		role     text
	)`
	if err := execDDL(cl.URL(), ddl); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	if err := execDDL(cl.URL(), fmt.Sprintf(
		"INSERT INTO gp_segment_configuration VALUES ('segA','10.0.0.1',6000,0,'p'), ('segB','10.0.0.2',6000,1,'p'), ('segC','10.0.0.3',6000,2,'p')")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx := context.Background()
	pool, err := pg.NewPool(ctx, cl.URL(), "", "", "", "", 2)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	s := &SourceServer{pool: pool}
	// segBinary empty (coordinator mode): NO ssh is attempted, S is reported,
	// no segreceiver identities to cross-check.
	sv, err := s.gatherSourceVersion(ctx, "", "gp@")
	if err != nil {
		t.Fatalf("gatherSourceVersion(empty segBinary) must not error, got: %v", err)
	}
	if sv.S != 3 {
		t.Fatalf("S = %d, want 3", sv.S)
	}
	if len(sv.SegReceivers) != 0 {
		t.Fatalf("SegReceivers = %v, want empty (no segreceivers in this mode)", sv.SegReceivers)
	}
}
