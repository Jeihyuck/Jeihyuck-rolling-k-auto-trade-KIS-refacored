#!/usr/bin/env bash
# Read-only diagnostic: no broker orders, no schedule modifications.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
DAY="2026-11-11"
echo "[KR_DIAG] date=$DAY"
echo "[KR_DIAG] branch=$(git branch --show-current 2>/dev/null) sha=$(git rev-parse HEAD 2>/dev/null)"
echo "[KR_DIAG] time=$(TZ=Asia/Seoul date -Is)"
if command -v powershell.exe >/dev/null 2>&1; then
 powershell.exe -NoProfile -NonInteractive -Command '
$names=@("PB1 KR Prep WSL","PB1 KR AM WSL","PB1 KR Afternoon WSL","PB1 KR Close WSL")
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
echo "[KR_DIAG] Do not infer Windows task failure solely from DB runs=0."
