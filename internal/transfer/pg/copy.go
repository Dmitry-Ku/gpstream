package pg

import (
	"context"
	"io"
)

// ReadCopy implements StreamingQuerier: it streams the rows of a
// `COPY ... TO STDOUT` statement into w (FR-01). This is the source side of
// the data path. It uses the named pgx.Conn (c.c), not the promoted method,
// because the embedded *pgx.Conn is nil (only the named field is populated).
func (c *Conn) ReadCopy(ctx context.Context, w io.Writer, sql string) (int64, error) {
	tag, err := c.c.PgConn().CopyTo(ctx, w, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// WriteCopy implements StreamingQuerier: it streams r into the destination via
// a `COPY ... FROM STDIN` statement (FR-01). This is the destination side of
// the data path.
func (c *Conn) WriteCopy(ctx context.Context, r io.Reader, sql string) (int64, error) {
	tag, err := c.c.PgConn().CopyFrom(ctx, r, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
