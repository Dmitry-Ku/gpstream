// Command segdump is a diagnostic driver: it accepts n segment streams,
// merges them through SliceMux (as gpstream run does) and writes the
// merged binary-COPY stream to --out for byte-level inspection.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "listen address")
	n := flag.Int("n", 2, "segment connections to accept")
	out := flag.String("out", "/tmp/merged.bin", "merged stream output file")
	timeout := flag.Duration("timeout", 5*time.Minute, "deadline")
	flag.Parse()

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "segdump: listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("segdump: listening on %s for %d segment(s)\n", l.Addr().String(), *n)

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "segdump: create out: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	mux := segstream.NewSliceMux(f, *n)
	if err := mux.Begin(); err != nil {
		fmt.Fprintf(os.Stderr, "segdump: begin: %v\n", err)
		os.Exit(1)
	}
	results, rerr := segstream.Receive(ctx, l, *n, func(contentID int16, p []byte) error {
		return mux.Write(contentID, p)
	})
	if rerr != nil {
		fmt.Fprintf(os.Stderr, "segdump: receive: %v\n", err)
	}
	var total int64
	for _, r := range results {
		total += r.Rows
		fmt.Printf("  content=%d rows=%d bytes=%d sha=%x err=%v\n",
			r.ContentID, r.Rows, r.Bytes, r.SHA256, r.Err)
	}
	if err := mux.End(); err != nil {
		fmt.Fprintf(os.Stderr, "segdump: end: %v\n", err)
	}
	f.Close()
	fmt.Printf("segdump: total rows=%d, all_done=%v, out=%s\n", total, mux.AllDone(), *out)
}
