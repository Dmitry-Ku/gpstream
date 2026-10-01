package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// LatestOperationID is the magic --operation-id value on the read-side
// commands (status/resume/cleanup): it resolves to the most recent operation
// folder under --state-dir, so `gpstream status --operation-id latest` always
// reports the newest run without the operator hunting for its name.
const LatestOperationID = "latest"

// defaultOperationID returns a fresh, chronologically-sorted operation folder
// name: the current UTC time as YYYYMMDDTHHMMSSZ (e.g. 20261001T153000Z). The
// fixed-width UTC format means lexicographic order == chronological order, so
// `ls <state-dir>` lists runs newest-last and "find the latest run" is trivial.
func defaultOperationID() string {
	return time.Now().UTC().Format("20060102T150405Z")
}

// resolveOperationID applies the two --operation-id rules:
//
//   - run: when the id is empty, a fresh timestamp id is generated
//     (collision-safe: if the folder already exists, -2, -3, … is appended,
//     so two runs in the same second never collide);
//   - status/resume/cleanup: the value "latest" resolves to the newest
//     operation folder under stateDir.
//
// It returns the concrete operation id and a note to surface (empty when
// nothing special happened). An explicit, non-latest id is returned
// unchanged (human-readable names are first-class, not just timestamps).
func resolveOperationID(opID, stateDir string, isRun bool) (string, string, error) {
	if opID == "" {
		if !isRun {
			return "", "", fmt.Errorf("--operation-id is required (or 'latest')")
		}
		id := defaultOperationID()
		for n := 2; dirExists(filepath.Join(stateDir, id)); n++ {
			id = fmt.Sprintf("%s-%d", defaultOperationID(), n)
		}
		return id, fmt.Sprintf("operation-id:   %s (generated; pass --operation-id to name it)", id), nil
	}
	if opID == LatestOperationID {
		id, err := latestOperationID(stateDir)
		if err != nil {
			return "", "", err
		}
		return id, fmt.Sprintf("operation-id:   %s (resolved from 'latest')", id), nil
	}
	return opID, "", nil
}

func dirExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

// latestOperationID returns the newest operation folder under stateDir.
// "Newest" is by folder-name timestamp when the names carry one (the fixed
// YYYYMMDDTHHMMSSZ[-n] default format is lexicographic == chronological);
// folders without a recognizable timestamp fall back to mtime. It errors when
// no operation folder exists.
func latestOperationID(stateDir string) (string, error) {
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		return "", fmt.Errorf("latest: read state dir %s: %w", stateDir, err)
	}
	var (
		bestTSID string // newest timestamp-named folder (full name, lexicographic)
		bestMT   time.Time
		bestMTID string
		found    bool
	)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		found = true
		name := e.Name()
		// Among timestamp-named folders the full name sorts lexicographically
		// == chronologically (fixed-width UTC; a collision suffix -2 sorts
		// after its base second, i.e. it is the newer run).
		if _, ok := folderTimestamp(name); ok && name > bestTSID {
			bestTSID = name
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		if bestMTID == "" || info.ModTime().After(bestMT) {
			bestMT = info.ModTime()
			bestMTID = name
		}
	}
	if !found {
		return "", fmt.Errorf("latest: no operation folders under %s (nothing has run here yet)", stateDir)
	}
	// A folder with a recognizable timestamp name is authoritative (the default
	// format sorts lexicographically == chronologically); mtime is the fallback
	// for operator-chosen names without that shape.
	if bestTSID != "" {
		return bestTSID, nil
	}
	return bestMTID, nil
}

// folderTimestamp extracts the sortable timestamp prefix of a default
// operation folder name (20261001T150405Z or 20261001T150405Z-2). It returns
// false for operator-chosen names without that shape.
func folderTimestamp(name string) (string, bool) {
	const want = 16 // len("20060102T150405Z")
	if len(name) < want {
		return "", false
	}
	ts := name[:want]
	if ts[8] != 'T' {
		return "", false
	}
	if _, err := time.Parse("20060102T150405Z", ts); err != nil {
		return "", false
	}
	return ts, true
}
