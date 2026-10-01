#!/usr/bin/env bash
# F-05: one-command release artifact build (tar.gz + deb + rpm, amd64 only).
#
#   scripts/package.sh            # snapshot build (dev, no git tag needed)
#   scripts/package.sh release    # real build, requires being on a git tag v*
#                                 # (CI uses this; goreleaser takes the version
#                                 #  from the tag and can publish to GitHub)
#
# Produces dist/:
#   gpstream-<ver>-linux-amd64.tar.gz
#   gpstream-segreceiver-<ver>-linux-amd64.tar.gz
#   gpstream-<ver>-amd64.deb
#   gpstream-<ver>-amd64.rpm
#
# goreleaser is pinned (GORELEASER_VERSION) and auto-downloaded to .tools/
# when not already on PATH.
set -euo pipefail
cd "$(dirname "$0")/.."

GORELEASER_VERSION="${GORELEASER_VERSION:-v1.24.0}"

# --- locate or fetch goreleaser ---
if command -v goreleaser >/dev/null 2>&1; then
  GR=goreleaser
else
  GR=".tools/goreleaser"
  if [ ! -x "$GR" ]; then
    echo "downloading goreleaser ${GORELEASER_VERSION} -> ${GR}"
    mkdir -p .tools
    tmp=$(mktemp)
    curl -sSL -o "$tmp" \
      "https://github.com/goreleaser/goreleaser/releases/download/${GORELEASER_VERSION}/goreleaser_Linux_x86_64.tar.gz"
    tar xzf "$tmp" -C .tools goreleaser
    rm -f "$tmp"
    chmod +x "$GR"
  fi
fi

# --- build mode ---
if [ "${1:-}" = "release" ]; then
  # real release: version comes from the git tag; CI sets GITHUB_TOKEN so
  # goreleaser v1.24 auto-publishes to the GitHub release (no --publish flag
  # in v1.24 — that is v2 syntax and fails `unknown flag: --publish`).
  PUBLISH_FLAG="${PUBLISH_FLAG:-}"
  $GR release --clean --skip=sign,docker $PUBLISH_FLAG
else
  # snapshot: no tag required, versions like v0.0.0-alpha.snapshot...
  $GR release --clean --snapshot --skip=sign,publish,docker
fi

echo
echo "artifacts in dist/:"
ls -1 dist/ | sed 's/^/  /'
