#!/usr/bin/env bash
# Source immediately after SCRIPT_DIR is known, before preflight, locks, or .env.
set -euo pipefail

nullim_init_session_log() {
  [[ -z "${NULLIM_SESSION_LOG:-}" ]] || return 0
  local market="${1:?market required}" session="${2:?session required}"
  local purpose="${3:-$session}" wrapper="${4:-${BASH_SOURCE[1]}}"
  local root run_date_kst trade_date_et partition run_id log_dir manifest lock
  root="$(cd "$(dirname "$(readlink -f "$wrapper")")/../.." && pwd -P)"
  mkdir -p "$root/runtime/locks"
  # Sessions hold a shared lock for their lifetime; mail packaging takes it exclusively.
  exec {NULLIM_SNAPSHOT_FD}>"$root/runtime/locks/${market,,}-mail-snapshot.lock"
  flock -s "$NULLIM_SNAPSHOT_FD"
  run_date_kst="${NULLIM_KST_RUN_DATE:-$(TZ=Asia/Seoul date +%F)}"
  trade_date_et="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
  if [[ "${market,,}" == us ]]; then partition="$trade_date_et"; else partition="$run_date_kst"; trade_date_et="$run_date_kst"; fi
  run_id="${NULLIM_RUN_ID:-${purpose}-$(TZ=UTC date +%Y%m%dT%H%M%S)-$$-${RANDOM}}"
  log_dir="$root/runtime/logs/${market,,}/$partition/$purpose"
  mkdir -p "$log_dir"
  find "$root/runtime/logs/${market,,}" -mindepth 1 -maxdepth 1 -type d -mtime +"${NULLIM_LOG_RETENTION_DAYS:-90}" ! -name "$partition" -exec rm -rf -- {} + 2>/dev/null || true
  NULLIM_SESSION_LOG="$log_dir/$run_id.log"
  NULLIM_SESSION_MANIFEST="$root/runtime/logs/${market,,}/$partition/session-manifest.json"
  export NULLIM_SESSION_LOG NULLIM_SESSION_MANIFEST NULLIM_RUN_ID="$run_id"
  export NULLIM_KST_RUN_DATE="$run_date_kst" NULLIM_TRADE_DATE="$trade_date_et" NULLIM_LOG_PARTITION="$partition"
  export NULLIM_SESSION_PURPOSE="$purpose"
  exec >>"$NULLIM_SESSION_LOG" 2>&1
  printf '[NULLIM_RUN][START] market=%s session=%s purpose=%s run_id=%s scheduler_task=%s scheduler_owner=%s wrapper=%s resolved_repo_root=%s branch=%s commit_sha=%s started_at_kst=%s started_at_et=%s trade_date_et=%s pid=%s parent_pid=%s\n' \
    "${market^^}" "$session" "$purpose" "$run_id" "${NULLIM_SCHEDULER_TASK_NAME:-manual}" "${NULLIM_SCHEDULER_OWNER:-MANUAL}" "$wrapper" "$root" \
    "$(git -C "$root" branch --show-current 2>/dev/null || echo unknown)" "$(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)" \
    "$(TZ=Asia/Seoul date -Is)" "$(TZ=America/New_York date -Is)" "$trade_date_et" "$$" "$PPID"
  lock="${NULLIM_SESSION_MANIFEST}.lock"; exec {NULLIM_MANIFEST_FD}>"$lock"; flock "$NULLIM_MANIFEST_FD"
  python3 - "$NULLIM_SESSION_MANIFEST" "${market^^}" "$run_date_kst" "$trade_date_et" "$session" "$purpose" "$run_id" "$NULLIM_SESSION_LOG" "$root" <<'PY'
import json, os, subprocess, sys, tempfile
from datetime import datetime, timezone
from pathlib import Path
p,market,kst,trade,session,purpose,run_id,log_file,root=sys.argv[1:]; path=Path(p)
try: data=json.loads(path.read_text())
except Exception: data={"market":market,"scheduler_owner":"WINDOWS_TASK_SCHEDULER","trade_date":trade,"trade_date_et":trade,"sessions":{}}
data["run_date_kst"]=kst; data["trade_date_et"]=trade
for key,cmd in (("branch",["git","-C",root,"branch","--show-current"]),("commit_sha",["git","-C",root,"rev-parse","HEAD"])):
    try:data[key]=subprocess.check_output(cmd,text=True).strip()
    except Exception:data[key]="unknown"
entry=data["sessions"].setdefault(purpose,{"attempts":[],"effective_run_id":None,"effective_status":None})
entry["attempts"].append({"run_id":run_id,"session":session,"status":"RUNNING","reason":"STARTED","run_date_kst":kst,"trade_date_et":trade,"log_file":str(Path(log_file).relative_to(root)),"started_at":datetime.now(timezone.utc).isoformat(),"ended_at":None,"exit_code":None})
fd,tmp=tempfile.mkstemp(prefix=path.name+".",dir=path.parent); os.close(fd)
Path(tmp).write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n"); os.replace(tmp,path)
PY
  flock -u "$NULLIM_MANIFEST_FD"; eval "exec ${NULLIM_MANIFEST_FD}>&-"
  trap 'nullim_finish_session_log $?' EXIT
}

nullim_finish_session_log() {
  local rc="${1:-0}" status reason lock
  [[ -n "${NULLIM_SESSION_MANIFEST:-}" && -f "$NULLIM_SESSION_MANIFEST" ]] || return "$rc"
  status="${NULLIM_SESSION_FINAL_STATUS:-}"; reason="${NULLIM_SESSION_FINAL_REASON:-}"
  [[ -n "$status" ]] || { if [[ "$rc" == 0 ]]; then status=OK; reason="${reason:-SESSION_END}"; elif [[ "$rc" == 124 || "$rc" == 137 ]]; then status=TIMEOUT; reason="${reason:-SESSION_TIMEOUT}"; else status=FAILED; reason="${reason:-EXIT_CODE_${rc}}"; fi; }
  lock="${NULLIM_SESSION_MANIFEST}.lock"; exec {NULLIM_MANIFEST_FD}>"$lock"; flock "$NULLIM_MANIFEST_FD"
  python3 - "$NULLIM_SESSION_MANIFEST" "$NULLIM_RUN_ID" "$rc" "$status" "$reason" <<'PY'
import json,os,sys,tempfile
from datetime import datetime,timezone
from pathlib import Path
p,run_id,rc,status,reason=sys.argv[1:]; path=Path(p); data=json.loads(path.read_text())
for entry in data.get("sessions",{}).values():
    for attempt in entry.get("attempts",[]):
        if attempt.get("run_id")==run_id:
            attempt.update(status=status,reason=reason,exit_code=int(rc),ended_at=datetime.now(timezone.utc).isoformat())
            entry.update(effective_run_id=run_id,effective_status=status)
fd,tmp=tempfile.mkstemp(prefix=path.name+".",dir=path.parent); os.close(fd)
Path(tmp).write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n"); os.replace(tmp,path)
PY
  flock -u "$NULLIM_MANIFEST_FD"; eval "exec ${NULLIM_MANIFEST_FD}>&-"
  printf '[NULLIM_RUN][END] run_id=%s status=%s reason=%s exit_code=%s ended_at=%s\n' "$NULLIM_RUN_ID" "$status" "$reason" "$rc" "$(date -Is)"
  return "$rc"
}

nullim_require_trading_day() {
  local market="${1:?market required}" trade_date="${2:?trade date required}" root python_bin output rc
  root="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../.." && pwd -P)"
  source "$root/scripts/wsl/resolve-nullim-python.sh"
  python_bin="$(nullim_resolve_python "$root")" || return 1
  output="$("$python_bin" "$root/scripts/wsl/check-nullim-trading-day.py" --market "${market,,}" --date "$trade_date" 2>&1)"
  rc=$?
  printf '%s\n' "$output"
  if [[ "$rc" == 10 ]]; then
    export NULLIM_SESSION_FINAL_STATUS=SKIPPED_NON_TRADING_DAY NULLIM_SESSION_FINAL_REASON=MARKET_CLOSED
    echo "[NULLIM_RUN][SKIP] reason=SKIPPED_NON_TRADING_DAY market=${market^^} trade_date=$trade_date"
  fi
  return "$rc"
}
