#!/bin/sh
# One-command rollback: stops and disables the soak units, removes their systemd links and
# deletes /opt/soak. Touches nothing else.
set -u
for u in soak-loadgen smart-soak stock-soak; do
  systemctl disable --now "$u.service" >/dev/null 2>&1
  if [ -L "/etc/systemd/system/$u.service" ]; then rm -f "/etc/systemd/system/$u.service"; fi
done
systemctl daemon-reload
systemctl reset-failed soak-loadgen.service smart-soak.service stock-soak.service >/dev/null 2>&1
rm -rf /opt/soak
echo "soak removed"
