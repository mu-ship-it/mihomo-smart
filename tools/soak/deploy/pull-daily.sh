#!/bin/sh
# Run on the Mini from the user crontab: appends each finished soak day's one-line summary to
# the lane report, and the final comparison + "PHASE 5 DONE" once the Surface has written it.
# Read-only on the Surface. Idempotent: lines already in the report are not added again.
REPORT=/tmp/pll-orange-report.md
HOST=root@100.79.49.101
LINES=$(ssh -o BatchMode=yes -o ConnectTimeout=20 $HOST 'cat /opt/soak/daily.log 2>/dev/null') || exit 0
[ -f "$REPORT" ] || exit 0
printf '%s\n' "$LINES" | while IFS= read -r l; do
  [ -n "$l" ] || continue
  grep -qF -- "$l" "$REPORT" || printf -- '- %s\n' "$l" >> "$REPORT"
done
if ! grep -q '^PHASE 5 DONE' "$REPORT"; then
  FINAL=$(ssh -o BatchMode=yes -o ConnectTimeout=20 $HOST 'cat /opt/soak/FINAL.md 2>/dev/null')
  if [ -n "$FINAL" ]; then
    printf '\n%s\n\nFull report: /opt/soak/report.md on the Surface.\n\nPHASE 5 DONE\n' "$FINAL" >> "$REPORT"
  fi
fi
