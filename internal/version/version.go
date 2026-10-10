// Package version carries the build identity every gpstream binary reports
// (1F, architecture 08.10: every binary reports its version; the dest agent
// cross-checks all of them before the first unit).
//
// Version and Commit are set at build time with -ldflags:
//
//	-X github.com/gpstream/gpstream/internal/version.Version=<ver>
//	-X github.com/gpstream/gpstream/internal/version.Commit=<git short hash>
//
// (scripts/package.sh / .goreleaser.yaml do this for all four binaries; the
// same commit builds all of them, so a healthy deployment has ONE shared
// build identity). A build without ldflags (go test, a local go build)
// reports "dev (dev)" — consistent across a dev test stand, so the
// cross-check still passes.
package version

var Version = "dev"
var Commit = "dev"

// String is the build identity reported in the hellos and the version
// exchange ("ver (commit)"; a bare version when no commit was injected).
func String() string {
	if Commit == "" {
		return Version
	}
	return Version + " (" + Commit + ")"
}
