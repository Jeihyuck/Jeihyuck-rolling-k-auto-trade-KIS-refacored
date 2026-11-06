#!/usr/bin/env bash
# Read-only evidence capture for a KR AM process exit code 137.
# Does NOT invoke broker APIs, sudo, kill, scheduler mutation or DB writes.
set -u -o pipefail

OUT_DIR="${1:-runtime/diagnostics/kr-exit137-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd -P)"
LOG="$OUT_DIR/evidence.txt"

section() { printf '\\n===== %s =====\\n' "$1" | tee -a "$LOG"; }
capture() {
  local label="$1"
  shift
  section "$label"
  if "$@" >>"$LOG" 2>&1; then
    printf 'captured: %s\\n' "$label"
  else
    printf 'unavailable or permission denied: %s\\n' "$label" | tee -a "$LOG"
  fi
}
: >"$LOG"
printf '[KR_EXIT137][READ_ONLY] captured_at=%s\\n' "$(date -Is)" >>"$LOG"
capture "utc and local time" date -Is
capture "uname" uname -a
capture "uptime" uptime
capture "memory" free -h
capture "swap" swapon --show
capture "vmstat" vmstat 1 2
capture "process rss snapshot (no command args)" ps -eo pid,ppid,stat,rss,comm --sort=-rss
for f in /sys/fs/cgroup/memory.events /sys/fs/cgroup/memory.max \
         /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.swap.current; do
  if [[ -r "$f" ]]; then capture "cgroup ${f##*/}" cat "$f"; fi
done

section "kernel OOM and kill evidence"
# Intentional filters avoid bulk sensitive kernel logs while preserving direct cause.
if command -v journalctl >/dev/null 2>&1; then
  journalctl --no-pager -k --since "36 hours ago" 2>&1 \
    | grep -Ei 'out of memory|oom.?kill|killed process|memory cgroup|sigkill' \
    | tail -n 100 >>"$LOG" || true
fi
if command -v dmesg >/dev/null 2>&1; then
  dmesg -T 2>&1 \
    | grep -Ei 'out of memory|oom.?kill|killed process|memory cgroup|sigkill' \
    | tail -n 100 >>"$LOG" || true
fi

section "Windows scheduled-task read-only evidence"
if command -v powershell.exe >/dev/null 2>&1; then
  powershell.exe -NoProfile -NonInteractive -Command '
    $ErrorActionPreference = "Continue"
    Get-ScheduledTask -TaskName "PB1 KR Prep WSL","PB1 KR AM WSL","PB1 KR Afternoon WSL","PB1 KR Close WSL" -ErrorAction SilentlyContinue |
      ForEach-Object {
        $i = $_ | Get-ScheduledTaskInfo
        [PSCustomObject]@{
          TaskName = $_.TaskName
          State = $_.State
          LastRunTime = $i.LastRunTime
          LastTaskResult = $i.LastTaskResult
          NextRunTime = $i.NextRunTime
          NumberOfMissedRuns = $i.NumberOfMissedRuns
        }
      } | Format-Table -AutoSize
  ' >>"$LOG" 2>&1 || true
else
  printf 'PowerShell unavailable in this environment.\\n' >>"$LOG"
fi

section "operator conclusion pending"
printf '%s\\n' \
  'Exit 137 alone means the caller observed signal-related 128+9=137.' \
  'OOM, cgroup kill, manual termination, scheduler cancellation, and external timeouts are alternatives.' \
  'Do not declare any one cause without timestamp-matched process, kernel and task evidence.' >>"$LOG"

printf '[KR_EXIT137][EVIDENCE_SAVED] path=%s\\n' "$LOG"
