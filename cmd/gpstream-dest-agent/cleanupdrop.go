// 2D-B (architecture 08.10, full zero-DB): the one-shot drop mode.
//
// `gpstream cleanup --mode drop` must NOT open a direct destination
// connection in the controller (the zero-DB rule). Instead the controller
// computes the drop targets from the operation journal mirror (it never
// touches the DB for that — the journal IS the ownership registration) and
// launches THIS agent in one-shot mode (--cleanup-drop) on the destination
// coordinator host. The agent connects to its OWN coordinator locally,
// drops the gpstream-owned objects (staging tables and tables first, the
// created schemas last, only when empty), and reports a JSON result on
// stdout.
//
// The mode ALWAYS exits 0 and encodes failures in the JSON result: the
// controller reaches the agent over SSH (RunRemoteOutput uses cmd.Output,
// which errors on a non-zero exit), so a partial failure must not be
// misreported as a transport failure — the same lesson as the 2C-3b probe
// script. The JSON is the single source of truth for the controller.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gpstream/gpstream/internal/journaldb"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// dropResult is the JSON the agent prints on stdout (the controller's
// single source of truth for the drop outcome).
type dropResult struct {
	OK      bool              `json:"ok"`
	Dropped int               `json:"dropped"`
	Errors  []string          `json:"errors,omitempty"`
	Details []dest.DropDetail `json:"details,omitempty"`
}

// runCleanupDrop performs the one-shot drop. It opens a LOCAL pool to the
// agent's OWN coordinator (the agent runs on the destination coordinator
// host — local connections only, architecture 08.10), drops the targets in
// ownership order (objects first, schemas last + only-when-empty), and
// prints the JSON result. It returns the process exit code: ALWAYS 0
// (failures are encoded in the result, see the package comment).
func runCleanupDrop(destConn, sslCert, sslKey, sslCA, minTLS, targetsJSON, jobID, operationID string) int {
	fail := func(format string, a ...any) {
		// A setup failure (bad JSON, no connection) is reported as a JSON
		// result too (the controller cannot distinguish stderr from the
		// drop outcome over SSH; stdout JSON is the contract).
		res := dropResult{OK: false, Errors: []string{fmt.Sprintf(format, a...)}}
		out, _ := json.Marshal(res)
		fmt.Println(string(out))
	}

	var targets []dest.DropTarget
	if err := json.Unmarshal([]byte(targetsJSON), &targets); err != nil {
		fail("parse --targets: %v", err)
		return 0
	}
	if len(targets) == 0 {
		res := dropResult{OK: true, Dropped: 0}
		out, _ := json.Marshal(res)
		fmt.Println(string(out))
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	q, err := pg.NewPool(ctx, destConn, sslCert, sslKey, sslCA, minTLS, 1)
	if err != nil {
		fail("connect coordinator: %v", err)
		return 0
	}
	defer q.Close()

	a := dest.New(q)
	// The FR-02 cleanup core (objects first, created schemas last +
	// only-when-empty) lives in the dest package (2D-B): the agent runs it
	// on its LOCAL coordinator connection and echoes the per-object outcome.
	out, derr := a.DropObjects(ctx, targets)
	res := dropResult{OK: derr == nil, Dropped: out.Dropped, Errors: out.Errors, Details: out.Details}
	// 2E-2: mark this operation's gpstream_units rows dropped so a re-run of
	// the same operation does NOT adopt the dropped rows as already-copied
	// (the stale-row fix). The 2E-2 already-copied query only reads
	// 'swapped'/'completed' rows, so 'dropped' is invisible to it. Best-effort
	// (the drop itself is the authoritative effect; the marking only affects
	// the resume keep decision): a failure is surfaced in the result, not
	// fatal.
	if derr == nil && jobID != "" && operationID != "" {
		if merr := markJournalDropped(ctx, q, jobID, operationID); merr != nil {
			res.Errors = append(res.Errors, "mark journal dropped: "+merr.Error())
		}
	}

	json, _ := json.Marshal(res)
	fmt.Println(string(json))
	return 0
}

// markJournalDropped (2E-2) flips the operation's gpstream_units terminal
// rows to 'dropped' after a successful cleanup --mode drop. It is scoped on
// job_id AND operation_id, so a plan-hash (job_id) shared by several
// operations is never cross-marked. The table is ensured (journaldb.Init,
// idempotent) so a destination that only ever saw the drop still has the
// column.
func markJournalDropped(ctx context.Context, q pg.StreamingQuerier, jobID, operationID string) error {
	if err := journaldb.Init(ctx, q); err != nil {
		return err
	}
	_, err := q.Exec(ctx,
		`UPDATE gpstream_units SET state = 'dropped' WHERE job_id = $1 AND operation_id = $2 AND state IN ('swapped', 'completed')`,
		jobID, operationID)
	return err
}
