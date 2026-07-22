#!/usr/bin/env bash
# Compatibility name: this removes legacy NULLIM user-cron entries; it never installs jobs.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKUP_DIR="${NULLIM_CRON_BACKUP_DIR:-$APP_DIR/runtime/scheduler-backups}"
mkdir -p "$BACKUP_DIR"
existing="$(crontab -l 2>/dev/null || true)"
backup="$BACKUP_DIR/crontab-before-$(date +%Y%m%d-%H%M%S).txt"
printf '%s\n' "$existing" > "$backup"
echo "[CRON_CLEANUP][POLICY] owner=WINDOWS_TASK_SCHEDULER"
echo "[CRON_CLEANUP][BACKUP] path=$backup"
# Never guess across an unbalanced legacy block: preserve all user cron and fail safely.
start_markers=$(printf '%s\n' "$existing" | awk '/# NULLIM_CRON_START/{n++} END{print n+0}')
end_markers=$(printf '%s\n' "$existing" | awk '/# NULLIM_CRON_END/{n++} END{print n+0}')
if [[ "$start_markers" != "$end_markers" ]]; then
  echo "[CRON_CLEANUP][VERIFY][FAIL] reason=UNBALANCED_NULLIM_MARKERS start=$start_markers end=$end_markers"
  exit 1
fi
# Remove managed blocks only after marker balance is established.
block_count=$(printf '%s\n' "$existing" | awk '/# NULLIM_CRON_START/{n++} END{print n+0}')
without_blocks=$(printf '%s\n' "$existing" | awk '
  /# NULLIM_CRON_START/ {skip=1; next}
  /# NULLIM_CRON_END/ {skip=0; next}
  !skip {print}
')
# Only remove cron lines that execute a NULLIM scheduler runner; unrelated cron remains intact.
forbidden='run_pb1_kr\.sh[[:space:]]+(prep|am|pm|close)|scripts/wsl/run-kr-(prep|am|afternoon|close)\.sh|scripts/wsl/run-us-(prep|prep-recovery|am|afternoon|close|session)\.sh|send-market-log-mail\.sh[[:space:]]+(kr|us)|check-nullim-day-health\.sh[[:space:]]+(kr|us)|python[[:space:]]+-m[[:space:]]+trader\.(pb1_runner|kr\.runner\.trade_session_runner|us\.runner\.trade_session_runner)'
orphan_count=$(printf '%s\n' "$without_blocks" | awk -v pat="$forbidden" '$0 ~ pat {n++} END{print n+0}')
cleaned=$(printf '%s\n' "$without_blocks" | awk -v pat="$forbidden" '$0 !~ pat')
printf '%s\n' "$cleaned" | crontab -
remaining=$(crontab -l 2>/dev/null | awk -v pat="$forbidden" '$0 ~ pat {n++} END{print n+0}')
echo "[CRON_CLEANUP][REMOVED_BLOCK] count=$block_count"
echo "[CRON_CLEANUP][REMOVED_ORPHAN] count=$orphan_count"
if [[ "$remaining" != 0 ]]; then
  echo "[CRON_CLEANUP][VERIFY][FAIL] forbidden_entries=$remaining"
  exit 1
fi
echo "[CRON_CLEANUP][VERIFY][OK] forbidden_entries=0"
echo "[CRON_INSTALL][BLOCK] reason=WINDOWS_TASK_SCHEDULER_ONLY"
