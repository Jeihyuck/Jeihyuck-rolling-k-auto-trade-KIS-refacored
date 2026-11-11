#!/usr/bin/env bash
# Read-only diagnostic: no broker orders, no schedule modifications.
set -uo pipefail
# Safe to run by streaming the GitHub PR version into bash -s while the
# installed dual-agent worktree stays unchanged and code-pinning remains valid.
if [[ -d "$PWD/scripts/wsl" && -d "$PWD/trader" ]]; then
  cd "$PWD" || exit 1
else
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1
fi
DAY="2026-11-11"
echo "[KR_DIAG] date=$DAY"
echo "[KR_DIAG] branch=$(git branch --show-current 2>/dev/null) sha=$(git rev-parse HEAD 2>/dev/null)"
echo "[KR_DIAG] time=$(TZ=Asia/Seoul date -Is)"
if command -v powershell.exe >/dev/null 2>&1; then
 powershell.exe -NoProfile -NonInteractive -Command '
$names=@("PB1 KR Prep WSL","PB1 KR AM WSL","PB1 KR Afternoon WSL","PB1 KR Close WSL","PB1 KR Mail WSL","PB1 KR Health WSL")
foreach($name in $names){
 $task=Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
 if($null -eq $task){Write-Output ("MISSING_TASK "+$name);continue}
 $info=$task | Get-ScheduledTaskInfo
 Write-Output ("TASK {0} state={1} last={2} next={3} result={4}" -f $name,$task.State,$info.LastRunTime,$info.NextRunTime,$info.LastTaskResult)
}' 2>&1 | tr -d '\r'
else echo "[KR_DIAG] POWERSHELL_UNAVAILABLE"; fi
echo "[KR_DIAG] LOCAL_SESSION_LOGS"
find "runtime/logs/kr/$DAY" -maxdepth 3 -type f -name '*.log' -printf '%p %TY-%Tm-%TdT%TH:%TM\n' 2>/dev/null | head -50 || true
echo "[KR_DIAG] PREP_ARTIFACTS"
for file in "runtime/kr/watchlist/$DAY/prep_done.json" "runtime/kr/watchlist/$DAY/prep_contract.json" "signals/kr/latest_prep_contract.json"; do
 if test -f "$file"; then stat -c 'PRESENT %n %y' "$file"; else echo "MISSING $file"; fi
done
echo "[KR_DIAG] LOCK_STATUS"
for s in prep am afternoon close; do
 f="runtime/locks/kr-$s.lock"
 if test -e "$f"; then
  if flock -n "$f" -c true 2>/dev/null; then echo "FREE $f"; else echo "BUSY $f"; fi
 else echo "MISSING $f"; fi
done
echo "[KR_DIAG] DEPLOY_MARKER"
if test -r runtime/health/windows-scheduler-install.json; then
 python3 - <<'PY'
import json
d=json.load(open("runtime/health/windows-scheduler-install.json"))
for key in ("scheduler_owner","status","installed_commit_sha"):
 print(key+"="+str(d.get(key)))
PY
else echo "MISSING_DEPLOY_MARKER"; fi

echo "[KR_DIAG] MAIL_SCHEDULER_WRAPPER"
mail_wrapper="runtime/scheduler/windows/$DAY/PB1-KR-Mail-WSL.log"
if [[ -f "$mail_wrapper" ]]; then
  grep -E 'WINDOWS_TASK.*(START|END)' "$mail_wrapper" | tail -12 || true
else echo "MISSING $mail_wrapper"; fi
echo "[KR_DIAG] MAIL_WINDOWS_TASK_EVENTS"
if command -v powershell.exe >/dev/null 2>&1; then
  powershell.exe -NoProfile -NonInteractive -Command '
    $start=[datetime]"2026-11-11 15:50:00"
    $finish=[datetime]"2026-11-11 16:25:00"
    $events=@(Get-WinEvent -FilterHashtable @{
      LogName="Microsoft-Windows-TaskScheduler/Operational"
      StartTime=$start; EndTime=$finish
    } -MaxEvents 300 -ErrorAction SilentlyContinue | Where-Object {
      $_.Message -like "*PB1 KR Mail WSL*"
    })
    if($events.Count -eq 0){Write-Output "MAIL_WINDOWS_EVENTS_NOT_FOUND_OR_LOG_DISABLED"}
    foreach($event in $events){
      Write-Output ("MAIL_EVENT at={0:o} id={1} message={2}" -f
        $event.TimeCreated,$event.Id,($event.Message -replace "[\\r\\n]+"," "))
    }
  ' 2>&1 | tr -d '\r' || true
else echo "[KR_DIAG] MAIL_WINDOWS_EVENT_QUERY_UNAVAILABLE"; fi
echo "[KR_DIAG] MAIL_RESULT_MARKER"
mail_marker="runtime/health/kr-mail-$DAY.json"
if [[ -r "$mail_marker" ]]; then
  python3 - "$mail_marker" <<'PY_MAIL_MARKER'
import json, sys
from pathlib import Path
try:
    data=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    for k in ("status", "sent_at", "mail_sent", "attempt_count", "failure_reason", "required_missing_count"):
        print("MAIL_MARKER "+k+"="+str(data.get(k)))
except (OSError, ValueError, TypeError) as exc:
    print("MAIL_MARKER_INVALID "+type(exc).__name__)
PY_MAIL_MARKER
else echo "MISSING $mail_marker"; fi
echo "[KR_DIAG] PREP_LAST_STAGE"
prep_stage="runtime/state/kr/prep_last_stage.json"
if [[ -r "$prep_stage" ]]; then
  python3 - "$prep_stage" <<'PY_PREP'
import json, sys
from pathlib import Path
try:
    data=json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    for k in ("stage", "expected_as_of", "final30_rows", "updated_at", "elapsed_sec_from_start"):
        print("PREP_STAGE "+k+"="+str(data.get(k)))
except (OSError, ValueError, TypeError) as exc:
    print("PREP_STAGE_INVALID "+type(exc).__name__)
PY_PREP
else echo "MISSING $prep_stage"; fi
echo "[KR_DIAG] Do not infer Windows task failure solely from DB runs=0."
