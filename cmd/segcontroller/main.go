// Command segcontroller is the controller side of the segment-direct
// transport (Fallback B). It listens for one connection per primary segment,
// reads the framed streams produced by gpstream-segreceiver (launched by the
// GPDB kernel via COPY ... TO PROGRAM ... ON SEGMENT), verifies each slice's
// trailer (row count + sha256) and optionally writes each slice's raw
// binary-COPY bytes to --out/slice_<content>.bin for inspection.
//
// It is a thin, dependency-free driver over internal/transfer/segstream —
// the production controller (gpstream run) uses the same Receive API. Exit
// 0 when every slice is clean, 1 otherwise.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "listen address host:port")
	n := flag.Int("n", 1, "number of segment connections to accept")
	out := flag.String("out", "", "write each slice to <out>/slice_<content>.bin (empty = discard)")
	timeout := flag.Duration("timeout", 10*time.Minute, "deadline for all segment streams")
	flag.Parse()

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "segcontroller: listen %s: %v\n", *listen, err)
		os.Exit(1)
	}
	fmt.Printf("segcontroller: listening on %s for %d segment(s)\n", l.Addr().String(), *n)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Per-slice raw byte sink (only when --out is set).
	var mu sync.Mutex
	sinks := map[int16]*os.File{}
	writer := func(contentID int16, p []byte) error {
		if *out == "" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		f, ok := sinks[contentID]
		if !ok {
			if err := os.MkdirAll(*out, 0o755); err != nil {
				return err
			}
			f, err = os.Create(filepath.Join(*out, fmt.Sprintf("slice_%d.bin", contentID)))
			if err != nil {
				return err
			}
			sinks[contentID] = f
		}
		_, werr := f.Write(p)
		return werr
	}

	results, err := segstream.Receive(ctx, l, *n, writer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "segcontroller: %v\n", err)
	}
	var totalRows int64
	allOK := true
	for _, r := range results {
		totalRows += r.Rows
		if r.Err != nil {
			allOK = false
			fmt.Printf("  content=%d rows=%d bytes=%d sha=%x ERR=%v\n", r.ContentID, r.Rows, r.Bytes, r.SHA256, r.Err)
		} else {
			fmt.Printf("  content=%d rows=%d bytes=%d sha=%x ok\n", r.ContentID, r.Rows, r.Bytes, r.SHA256)
		}
	}
	// Flush + close sinks.
	mu.Lock()
	for _, f := range sinks {
		f.Close()
	}
	mu.Unlock()
	fmt.Printf("segcontroller: %d slice(s), total rows=%d, all_ok=%v\n", len(results), totalRows, allOK)
	if !allOK || err != nil {
		os.Exit(1)
	}
}
