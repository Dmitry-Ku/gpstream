package transfer

import (
	"fmt"
	"os"
)

// dataLog is the data-path debug log (Inc3.4): OFF by default — the data path
// is deliberately quiet in production (observability is the counters and the
// journal). Enable with GPSTREAM_DATA_LOG=1 (any non-empty value other than
// 0/off). One line per event, to stderr:
//
//	gpstream <component>: <event> key=value ...
//
// The agent's stderr is captured in the controller's run log; the
// segreceiver's stderr lands in the GPDB segment log.

var dataLogEnabled = func() bool {
	v := os.Getenv("GPSTREAM_DATA_LOG")
	return v != "" && v != "0" && v != "off"
}()

func dataLogf(component, format string, a ...any) {
	if !dataLogEnabled {
		return
	}
	fmt.Fprintf(os.Stderr, "gpstream "+component+": "+format+"\n", a...)
}
