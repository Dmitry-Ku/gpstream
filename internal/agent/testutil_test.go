//go:build gp6lab

package agent

import (
	"context"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// execDDL runs one DDL/DML statement on the test cluster. Shared by the
// gp6lab-tagged agent tests (coordinator_check_test.go,
// versioncheck_bug15_test.go). It is tagged gp6lab because only the tagged
// tests use it: an untagged copy is U1000 dead code in the default build
// (the 06517ec untagged staticcheck cleanup deleted the shared copy, which
// broke the gp6lab build — this restores the single definition under the
// tag that actually uses it). A duplicate in a tag-less file breaks the
// gp6lab build with a redeclaration, so this file is the sole definition.
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
