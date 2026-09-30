#!/usr/bin/env bash
# Generate the man-page family (TASK-SPEC-v4 F-05: "man gpstream, man
# gpstream-run, ... are generated ... and installed into /usr/share/man").
#
# The single source of truth is man/gpstream.8 (one binary, subcommands are
# flags — so each subcommand page is a `.so` alias of the main page:
# `man gpstream-run` renders the full manual under the subcommand name).
#
# Usage: scripts/gen-man-pages.sh   (writes man/gpstream-<cmd>.8)
set -euo pipefail
cd "$(dirname "$0")/.."

MAIN=man/gpstream.8
[ -f "$MAIN" ] || { echo "missing $MAIN" >&2; exit 1; }

for cmd in run dry-run status resume cleanup doctor selftest version; do
  out="man/gpstream-${cmd}.8"
  {
    echo ".so ${MAIN##*/}"
  } > "$out"
  echo "generated $out"
done
