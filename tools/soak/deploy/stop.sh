#!/bin/sh
# Stops the soak but keeps /opt/soak (data and report). Used by the Home Assistant guard and at
# completion; safe to run by hand. /opt/soak/remove.sh removes everything.
echo "$(date -Iseconds) ${1:-manual stop}" >> /opt/soak/STOPPED
systemctl disable --now smart-soak.service stock-soak.service
systemctl disable --now --no-block soak-loadgen.service
