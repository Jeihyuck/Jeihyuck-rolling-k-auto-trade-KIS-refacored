#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
echo "[SCHEDULE_AUDIT][START] repo=$REPO"
echo "[SCHEDULE_AUDIT][CRONTAB]"; crontab -l 2>/dev/null || true
echo "[SCHEDULE_AUDIT][USER_TIMERS]"; systemctl --user list-timers --all 2>/dev/null || true
echo "[SCHEDULE_AUDIT][ETC_CRONTAB]"; cat /etc/crontab 2>/dev/null || true
echo "[SCHEDULE_AUDIT][ETC_CRON_D]"; for f in /etc/cron.d/*; do [[ -f "$f" ]] && { echo "--- $f"; cat "$f"; }; done 2>/dev/null || true
echo "[SCHEDULE_AUDIT][WSL_SCRIPT_REFERENCES]"
TMP=$(mktemp)
{ crontab -l 2>/dev/null || true; cat /etc/crontab 2>/dev/null || true; cat /etc/cron.d/* 2>/dev/null || true; systemctl --user list-timers --all 2>/dev/null || true; } > "$TMP"
rg -n "scripts/wsl|run-kr-|run-us-|wsl-kr|wsl-us" "$TMP" || true
if rg -q "run-kr-trader\.sh" "$TMP"; then echo "[SCHEDULE_AUDIT][ERROR] run-kr-trader.sh referenced by scheduler"; exit 2; fi
echo "[SCHEDULE_AUDIT][DONE]"
