#!/bin/bash
# check-english.sh — CI guard: no Cyrillic in git-tracked content.
#
# Rule (project convention): everything that lands in git — code comments,
# documentation, man pages, scripts — is English, the universal working
# language of the project. This guard scans every git-tracked text file for
# Cyrillic characters and fails if any are found, EXCEPT the allowlist below
# (functional test data, not prose).
#
# Usage: bash scripts/check-english.sh   (from the repo root; CI runs it as-is)
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Allowlist: content that may contain Cyrillic DELIBERATELY.
# Two forms, one rule per line:  FILE<TAB>MUST-CONTAIN
#   - MUST-CONTAIN == "*"  -> the WHOLE file is exempt (operational state,
#     not project prose).
#   - MUST-CONTAIN <string> -> only lines containing that string are exempt
#     (proves the Cyrillic is functional data, e.g. a collation fixture).
ALLOWLIST="$(mktemp)"
trap 'rm -f "$ALLOWLIST"' EXIT
cat > "$ALLOWLIST" <<'EOF'
scripts/f16-acceptance.sh	cyrillic
# Autonomous-cron operational state (read/written by the gpstream-autonomy
# cron every 30 min; its prompt is Russian, so this working-state file is).
# It is agent scratch state, not project documentation, so it is exempt as a
# whole file.
STATE-CHECKPOINT-2026-09-16.md	*
EOF

fail=0
while IFS= read -r f; do
  [ -f "$f" ] || continue
  # skip binary files
  if grep -qI . "$f" 2>/dev/null; then
    is_text=1
  else
    is_text=0
  fi
  [ "$is_text" = "1" ] || continue
  # find lines with Cyrillic
  # (a) whole-file exemption: if the file is allowlisted with MUST-CONTAIN "*"
  if grep -qP "^$f\t\*\s*$" "$ALLOWLIST"; then
    continue
  fi
  while IFS=: read -r ln rest; do
    # check allowlist for this file
    ok=0
    while IFS=$'\t' read -r af amust; do
      [ "$af" = "$f" ] || continue
      case "$rest" in
        *"$amust"*) ok=1; break ;;
      esac
    done < "$ALLOWLIST"
    if [ "$ok" = "1" ]; then
      continue
    fi
    echo "Cyrillic found: $f:$ln: $rest"
    fail=1
  done < <(grep -nP '[\x{0400}-\x{04FF}]' "$f" 2>/dev/null || true)
done < <(git ls-files)

if [ "$fail" = "1" ]; then
  echo ""
  echo "ERROR: git-tracked content must be English (project convention)."
  echo "Translate the lines above, or add a justified allowlist entry."
  exit 1
fi
echo "check-english: OK (no unexpected Cyrillic in git-tracked content)"
