#!/usr/bin/env bash
# Shared KR wrapper lock contract. Never terminates a lock holder.
kr_lock_owner() { local lock_file="$1"; printf '{"pid":%s,"hostname":"%s","command":"%s","started_at":"%s","run_source":"%s"}\n' "$$" "$(hostname)" "${0##*/}" "$(date -Is)" "${WSL_RUN_SOURCE:-unknown}" > "${lock_file}.owner"; }
kr_duplicate_result() {
  local lock_file="$1" session="$2" owner_file="${lock_file}.owner" date result log_name
  date="$(TZ=Asia/Seoul date +%F)"; result="runtime/kr/session/${date}/${session}/duplicates/duplicate-$(TZ=Asia/Seoul date +%Y%m%dT%H%M%S)-$$.json"
  # Write only an evidence record: the canonical pb1_result.json belongs exclusively to the lock owner.
  mkdir -p "$(dirname "$result")" "runtime/logs/kr/${date}"
  python - "$owner_file" "$result" <<'PY'
import json, sys
from pathlib import Path
owner={"pid":0,"hostname":"","command":"","started_at":"","run_source":""}
try: owner.update(json.loads(Path(sys.argv[1]).read_text()))
except Exception: pass
Path(sys.argv[2]).write_text(json.dumps({"status":"SKIP_DUPLICATE","reason":"SESSION_LOCK_HELD","completed":True,"orders_intent":0,"orders_ack":0,"lock_owner":owner})+'\n')
PY
  log_name="$session"; [[ "$session" == afternoon ]] && log_name=afternoon
  echo "[$(date -Is)] [KR_SESSION_LOCK][SKIP_DUPLICATE] session=$session reason=SESSION_LOCK_HELD lock=$lock_file evidence=$result" >> "runtime/logs/kr/${date}/wsl-kr-${log_name}.log"
}
