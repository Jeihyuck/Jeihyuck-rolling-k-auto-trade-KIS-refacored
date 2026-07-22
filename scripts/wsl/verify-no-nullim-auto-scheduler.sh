#!/usr/bin/env bash
set -euo pipefail
# Inspect scheduler sources only; repository documentation and tests are deliberately excluded.
pattern='run_pb1_kr\.sh[[:space:]]+(prep|am|pm|close)|run-kr-(prep|am|afternoon|close)\.sh|run-us-(prep|prep-recovery|am|afternoon|close|session)\.sh|send-market-log-mail\.sh[[:space:]]+(kr|us)|check-nullim-day-health\.sh[[:space:]]+(kr|us)|python[[:space:]]+-m[[:space:]]+trader\.(pb1_runner|kr\.runner\.trade_session_runner|us\.runner\.trade_session_runner)'
count=0
check_source() { local source="$1" content="$2"; while IFS= read -r line; do [[ -z "$line" ]] && continue; echo "[SCHEDULER_POLICY][WSL][FAIL] source=$source line=$line"; count=$((count+1)); done < <(printf '%s\n' "$content" | grep -E "$pattern" || true); }
check_file() { [[ -f "$1" ]] && check_source "$1" "$(cat "$1")"; }
check_source "user-crontab" "$(crontab -l 2>/dev/null || true)"
check_file /etc/crontab
for f in /etc/cron.d/* /etc/systemd/system/* /etc/systemd/user/* "$HOME"/.config/systemd/user/*; do check_file "$f"; done
check_source "systemctl-list-timers" "$(systemctl list-timers --all 2>/dev/null || true)"
check_source "systemctl-user-list-timers" "$(systemctl --user list-timers --all 2>/dev/null || true)"
if ((count)); then echo "[SCHEDULER_POLICY][WSL][SUMMARY] forbidden_sources=$count"; exit 1; fi
echo "[SCHEDULER_POLICY][WSL][OK] owner=WINDOWS_TASK_SCHEDULER forbidden_sources=0"
