#!/bin/sh
# gpstream postremove: drop the unit from the systemd manager on final removal
# ($1 = 2 means purge/uninstall, not upgrade).
if [ "$1" = "2" ] && command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || true
  systemctl reset-failed gpstream.service >/dev/null 2>&1 || true
fi
exit 0
