#!/usr/bin/env bash
# Session-scoped, flock-backed lock metadata. A leftover file is evidence, not a lock.

nullim_session_lock_acquire() {
  local lock_file="${1:?lock file required}" market="${2:?market required}" session="${3:?session required}"
  local trade_date="${4:?trade date required}" log_file="${5:-/dev/stderr}" ttl="${NULLIM_SESSION_LOCK_TTL_SEC:-43200}"
  local had_file=0
  [[ -e "$lock_file" ]] && had_file=1
  mkdir -p "$(dirname "$lock_file")"
  exec {NULLIM_SESSION_LOCK_FD}<>"$lock_file"
  if ! flock -n "$NULLIM_SESSION_LOCK_FD"; then
    local active
    active="$(python3 - "$lock_file" "$market" "$session" "$trade_date" <<'PY'
import json, os, sys
try:
 d=json.load(open(sys.argv[1])); pid=d['pid']; command=str(d.get('command',''))
 cmdline=open(f'/proc/{pid}/cmdline','rb').read().replace(b'\0',b' ').decode(errors='replace')
 valid=(isinstance(pid,int) and pid>0 and d.get('market')==sys.argv[2] and
        d.get('session')==sys.argv[3] and d.get('trade_date')==sys.argv[4] and
        command and os.path.basename(command) in cmdline and ('run-kr-' in command or 'run-us-' in command))
 print('1' if valid else '0')
except Exception: print('0')
PY
)"
    if [[ "$active" == 1 ]]; then
      printf '[LOCK][ACTIVE][SAME_SESSION][SKIP] market=%s session=%s lock=%s\n' "$market" "$session" "$lock_file" >>"$log_file"
      printf '[SESSION][DUPLICATE][SKIP] market=%s session=%s\n' "$market" "$session" >>"$log_file"
      return 75
    fi
    printf '[LOCK][STALE][DETECTED] reason=HELD_BY_UNEXPECTED_PROCESS lock=%s\n' "$lock_file" >>"$log_file"
    # The unexpected holder owns the old inode. Replacing its directory entry
    # lets trading recover without signalling or terminating that process.
    if rm -f -- "$lock_file"; then
      printf '[LOCK][STALE][REMOVED] lock=%s\n' "$lock_file" >>"$log_file"
      flock -u "$NULLIM_SESSION_LOCK_FD" 2>/dev/null || true
      eval "exec ${NULLIM_SESSION_LOCK_FD}>&-"
      unset NULLIM_SESSION_LOCK_FD
      nullim_session_lock_acquire "$lock_file" "$market" "$session" "$trade_date" "$log_file"
      return $?
    fi
    printf '[LOCK][STALE][REMOVE_FAILED][WARN] lock=%s action=CONTINUE_WITH_RECOVERY_LOCK\n' "$lock_file" >>"$log_file"
    lock_file="${lock_file}.recovery-${market}-${session}"
    flock -u "$NULLIM_SESSION_LOCK_FD" 2>/dev/null || true
    eval "exec ${NULLIM_SESSION_LOCK_FD}>&-"
    unset NULLIM_SESSION_LOCK_FD
    nullim_session_lock_acquire "$lock_file" "$market" "$session" "$trade_date" "$log_file"
    return $?
  fi

  # Once flock is held no live owner exists. Classify any prior contents before replacing them.
  if [[ "$had_file" == 1 ]]; then
    local stale_reason
    stale_reason="$(python3 - "$lock_file" "$market" "$session" "$trade_date" "$ttl" <<'PY'
import json, os, sys
from datetime import datetime, timezone
from pathlib import Path
p, market, session, day, ttl = sys.argv[1:]
path=Path(p)
if path.stat().st_size == 0: print('EMPTY_FILE'); raise SystemExit
try: d=json.loads(path.read_text())
except Exception: print('INVALID_METADATA'); raise SystemExit
pid=d.get('pid')
if not isinstance(pid, int) or pid <= 0: print('INVALID_PID'); raise SystemExit
if d.get('market') != market or d.get('session') != session: print('OUT_OF_SCOPE'); raise SystemExit
if d.get('trade_date') != day: print('PREVIOUS_TRADE_DATE'); raise SystemExit
try:
    created=datetime.fromisoformat(str(d['created_at']).replace('Z','+00:00'))
    if (datetime.now(timezone.utc)-created).total_seconds() > int(ttl): print('TTL_EXCEEDED'); raise SystemExit
except (KeyError, TypeError, ValueError): print('INVALID_CREATED_AT'); raise SystemExit
try: os.kill(pid, 0)
except (ProcessLookupError, PermissionError): print('DEAD_PID'); raise SystemExit
print('UNLOCKED_OWNER')
PY
)"
    if [[ -n "$stale_reason" ]]; then
      printf '[LOCK][STALE][DETECTED] reason=%s lock=%s\n' "$stale_reason" "$lock_file" >>"$log_file"
    fi
  fi
  NULLIM_SESSION_LOCK_TOKEN="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  export NULLIM_SESSION_LOCK_FD NULLIM_SESSION_LOCK_TOKEN NULLIM_SESSION_LOCK_FILE="$lock_file"
  python3 - "$NULLIM_SESSION_LOCK_FD" "$$" "$NULLIM_SESSION_LOCK_TOKEN" "$market" "$session" "$trade_date" "${0}" <<'PY'
import json, os, sys
fd,pid,token,market,session,day,command=sys.argv[1:]
data={'pid':int(pid),'owner_token':token,'market':market,'session':session,'trade_date':day,
      'created_at':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
      'command':command}
os.lseek(int(fd),0,os.SEEK_SET); os.ftruncate(int(fd),0)
os.write(int(fd),(json.dumps(data,separators=(',',':'))+'\n').encode()); os.fsync(int(fd))
PY
  [[ "$had_file" == 0 ]] || printf '[LOCK][STALE][REMOVED] lock=%s action=METADATA_REPLACED\n' "$lock_file" >>"$log_file"
  printf '[LOCK][ACQUIRED] market=%s session=%s lock=%s\n' "$market" "$session" "$lock_file" >>"$log_file"
}

nullim_session_lock_release() {
  [[ -n "${NULLIM_SESSION_LOCK_FILE:-}" && -n "${NULLIM_SESSION_LOCK_TOKEN:-}" ]] || return 0
  local owned=0
  owned="$(python3 - "$NULLIM_SESSION_LOCK_FILE" "$NULLIM_SESSION_LOCK_TOKEN" <<'PY'
import json,sys
try: print(int(json.load(open(sys.argv[1])).get('owner_token') == sys.argv[2]))
except Exception: print(0)
PY
)"
  if [[ "$owned" == 1 ]]; then
    if rm -f -- "$NULLIM_SESSION_LOCK_FILE"; then
      echo "[LOCK][RELEASED] lock=$NULLIM_SESSION_LOCK_FILE"
    else
      echo "[LOCK][STALE][REMOVE_FAILED][WARN] lock=$NULLIM_SESSION_LOCK_FILE" >&2
    fi
  fi
  flock -u "$NULLIM_SESSION_LOCK_FD" 2>/dev/null || true
  eval "exec ${NULLIM_SESSION_LOCK_FD}>&-" 2>/dev/null || true
  unset NULLIM_SESSION_LOCK_FILE NULLIM_SESSION_LOCK_TOKEN NULLIM_SESSION_LOCK_FD
}
