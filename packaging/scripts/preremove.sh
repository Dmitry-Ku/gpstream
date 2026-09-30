#!/bin/sh
# gpstream preremove: stop the service if running (do not remove the unit yet).
if command -v systemctl >/dev/null 2>&1; then
  systemctl stop gpstream.service >/dev/null 2>&1 || true
fi
exit 0
