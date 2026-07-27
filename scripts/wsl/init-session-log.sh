#!/usr/bin/env bash
# Source this immediately after SCRIPT_DIR is known.  It deliberately does not
# read .env: scheduler/bootstrap failures must be captured before configuration.
set -euo pipefail

nullim_init_session_log() {
  [[ -z "${NULLIM_SESSION_LOG:-}" ]] || return 0
  local market="${1:?market required}" session="${2:?session required}"
  local purpose="${3:-$session}" wrapper="${4:-${BASH_SOURCE[1]}}"
  local root run_date trade_date run_id log_dir manifest
  root="$(cd "$(dirname "$(readlink -f "$wrapper")")/../.." && pwd -P)"
  run_date="${NULLIM_KST_RUN_DATE:-$(TZ=Asia/Seoul date +%F)}"
  trade_date="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
  [[ "${market,,}" == kr ]] && trade_date="$run_date"
  run_id="${NULLIM_RUN_ID:-$(TZ=UTC date +%Y%m%dT%H%M%S)-$$-${RANDOM}}"
  log_dir="$root/runtime/logs/${market,,}/$run_date/$purpose"
  mkdir -p "$log_dir"
  # Bound raw dated logs while protecting the active execution date.
  find "$root/runtime/logs/${market,,}" -mindepth 1 -maxdepth 1 -type d -mtime +"${NULLIM_LOG_RETENTION_DAYS:-90}" ! -name "$run_date" -exec rm -rf -- {} + 2>/dev/null || true
  NULLIM_SESSION_LOG="$log_dir/$run_id.log"
  NULLIM_SESSION_MANIFEST="$root/runtime/logs/${market,,}/$run_date/session-manifest.json"
  export NULLIM_SESSION_LOG NULLIM_SESSION_MANIFEST NULLIM_RUN_ID="$run_id"
  export NULLIM_KST_RUN_DATE="$run_date" NULLIM_TRADE_DATE="$trade_date"
  exec >>"$NULLIM_SESSION_LOG" 2>&1
  printf '[NULLIM_RUN][START] market=%s session=%s run_id=%s scheduler_task=%s scheduler_owner=%s wrapper=%s resolved_repo_root=%s branch=%s commit_sha=%s started_at_kst=%s started_at_et=%s trade_date_et=%s pid=%s parent_pid=%s\n' \
    "${market^^}" "$session" "$run_id" "${NULLIM_SCHEDULER_TASK_NAME:-manual}" \
    "${NULLIM_SCHEDULER_OWNER:-MANUAL}" "$wrapper" "$root" \
    "$(git -C "$root" branch --show-current 2>/dev/null || echo unknown)" \
    "$(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)" \
    "$(TZ=Asia/Seoul date -Is)" "$(TZ=America/New_York date -Is)" "$trade_date" "$$" "$PPID"
  python3 - "$NULLIM_SESSION_MANIFEST" "${market^^}" "$run_date" "$trade_date" "$session" "$purpose" "$run_id" "$NULLIM_SESSION_LOG" "$root" <<'PY'
import json, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
p, market, run_date, trade_date, session, purpose, run_id, log_file, root=sys.argv[1:]
path=Path(p)
try: data=json.loads(path.read_text())
except Exception: data={"market":market,"scheduler_owner":"WINDOWS_TASK_SCHEDULER","run_date_kst":run_date,"trade_date":trade_date,"trade_date_et":trade_date,"sessions":{}}
try: data["branch"]=subprocess.check_output(["git","-C",root,"branch","--show-current"],text=True).strip()
except Exception: data["branch"]="unknown"
try: data["commit_sha"]=subprocess.check_output(["git","-C",root,"rev-parse","HEAD"],text=True).strip()
except Exception: data["commit_sha"]="unknown"
data["sessions"][purpose]={"session":session,"status":"RUNNING","run_id":run_id,"log_file":str(Path(log_file).relative_to(root)),"started_at":datetime.now(timezone.utc).isoformat(),"exit_code":None}
path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n")
PY
  trap 'nullim_finish_session_log $?' EXIT
}

nullim_finish_session_log() {
  local rc="${1:-0}"
  [[ -n "${NULLIM_SESSION_MANIFEST:-}" && -f "$NULLIM_SESSION_MANIFEST" ]] || return "$rc"
  python3 - "$NULLIM_SESSION_MANIFEST" "$NULLIM_RUN_ID" "$rc" <<'PY'
import json, sys
from datetime import datetime, timezone
from pathlib import Path
p, run_id, rc=sys.argv[1:]; path=Path(p); data=json.loads(path.read_text())
for item in data.get("sessions",{}).values():
    if item.get("run_id")==run_id:
        item.update(status="OK" if rc=="0" else "FAILED",exit_code=int(rc),ended_at=datetime.now(timezone.utc).isoformat())
path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n")
PY
  printf '[NULLIM_RUN][END] run_id=%s exit_code=%s ended_at=%s\n' "$NULLIM_RUN_ID" "$rc" "$(date -Is)"
  return "$rc"
}
