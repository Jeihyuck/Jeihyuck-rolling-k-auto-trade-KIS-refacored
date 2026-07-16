#!/usr/bin/env bash
set -euo pipefail
# Windows Task Scheduler is the only automatic scheduler for KR/US WSL trading.
# WSL cron must not run trading sessions under WSL; this installer only removes legacy NULLIM blocks.
existing="$(crontab -l 2>/dev/null || true)"
cleaned="$(printf '%s\n' "$existing" | sed '/# NULLIM_CRON_START/,/# NULLIM_CRON_END/d')"
printf '%s\n' "$cleaned" | sed '/^[[:space:]]*$/d' | crontab - 2>/dev/null || true
echo "[CRON_INSTALL][BLOCK] reason=WINDOWS_TASK_SCHEDULER_ONLY removed=NULLIM_CRON_BLOCK"
