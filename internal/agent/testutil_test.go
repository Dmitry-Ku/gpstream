package agent

import (
	"context"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// execDDL runs one DDL/DML statement on the test cluster. Shared by the
// tagged (gp6lab) and untagged agent tests — a single package-level helper
// (a duplicate in a tag-less file breaks the gp6lab build with a
// redeclaration).
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
