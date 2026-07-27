#!/usr/bin/env bash
set -euo pipefail
pattern='run_pb1_kr\.sh|run-kr-[A-Za-z0-9_-]+\.sh|run-us-[A-Za-z0-9_-]+\.sh|(send-market-log-mail|send-kr-log-mail|send-us-log-mail)\.sh([[:space:]]|$)|check-nullim-day-health\.sh([[:space:]]|$)|python[[:space:]]+-m[[:space:]]+trader\.(pb1_runner|kr\.runner\.trade_session_runner|us\.runner\.trade_session_runner)'
count=0
check_source(){ local source="$1" content="$2"; while IFS= read -r line; do [[ -z "$line" ]]&&continue; echo "[SCHEDULER_POLICY][WSL][FAIL] source=$source line=$line"; count=$((count+1)); done < <(printf '%s\n' "$content"|grep -E "$pattern"||true); }
check_file(){
  [[ -f "$1" ]] || return 0
  check_source "$1" "$(cat "$1")"
}
check_source user-crontab "$(crontab -l 2>/dev/null||true)"; check_file /etc/crontab
for f in /etc/cron.d/* /etc/systemd/{system,user}/* /lib/systemd/system/* /usr/lib/systemd/system/* "$HOME"/.config/systemd/user/* /etc/systemd/{system,user}/*.d/* /lib/systemd/system/*.d/* /usr/lib/systemd/system/*.d/*; do check_file "$f"; done
# Timer listings alone are insufficient: inspect the service paired with each active timer.
for scope in system user; do
  timer_out=$( [[ "$scope" == user ]] && systemctl --user list-timers --all 2>/dev/null || systemctl list-timers --all 2>/dev/null || true )
  check_source "systemctl-$scope-list-timers" "$timer_out"
  while read -r unit; do [[ "$unit" == *.timer ]] || continue; service="${unit%.timer}.service"; content=$( [[ "$scope" == user ]] && systemctl --user cat "$service" 2>/dev/null || systemctl cat "$service" 2>/dev/null || true ); check_source "systemctl-$scope:$service" "$content"; done < <(printf '%s\n' "$timer_out"|awk 'NR>1 {for(i=1;i<=NF;i++)if($i ~ /\.timer$/)print $i}')
done
if ((count)); then echo "[SCHEDULER_POLICY][WSL][SUMMARY] forbidden_sources=$count"; exit 1; fi
echo "[SCHEDULER_POLICY][WSL][OK] owner=WINDOWS_TASK_SCHEDULER forbidden_sources=0"
