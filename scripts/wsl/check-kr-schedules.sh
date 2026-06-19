#!/usr/bin/env bash
set -euo pipefail

scripts=(run-kr-prep run-kr-am run-kr-afternoon run-kr-close)
pattern='kr|trade|nullim'
script_pattern='run-kr-prep|run-kr-am|run-kr-afternoon|run-kr-close'
GREP_TIMEOUT_SEC="${KR_SCHEDULE_GREP_TIMEOUT_SEC:-8}"
GREP_EXCLUDES=(
  --exclude-dir=.cache
  --exclude-dir=.cargo
  --exclude-dir=.codex
  --exclude-dir=.local
  --exclude-dir=.npm
  --exclude-dir=.rustup
  --exclude-dir=node_modules
  --exclude-dir=venv
  --exclude-dir=.venv
)
declare -A lock_files=(
  [prep]='/tmp/nullim-kr-prep.lock'
  [am]='/tmp/nullim-kr-am.lock'
  [afternoon]='/tmp/nullim-kr-afternoon.lock'
  [close]='/tmp/nullim-kr-close.lock'
)

section() {
  echo
  echo "========== $* =========="
}

grep_refs() {
  local pattern="$1"
  shift
  local rc=0
  (($# == 0)) && return 0
  timeout "${GREP_TIMEOUT_SEC}s" grep -RInE "${GREP_EXCLUDES[@]}" "$pattern" "$@" 2>/dev/null || rc=$?
  if [[ "$rc" -eq 124 ]]; then
    echo "[KR_SCHEDULE][WARN] grep_refs timed out pattern=$pattern"
  elif [[ "$rc" -ne 0 && "$rc" -ne 1 ]]; then
    echo "[KR_SCHEDULE][WARN] grep_refs failed rc=$rc pattern=$pattern"
  fi
  return 0
}

grep_files() {
  local pattern="$1"
  shift
  local rc=0
  (($# == 0)) && return 0
  timeout "${GREP_TIMEOUT_SEC}s" grep -RIlE "${GREP_EXCLUDES[@]}" "$pattern" "$@" 2>/dev/null || rc=$?
  if [[ "$rc" -eq 124 ]]; then
    echo "[KR_SCHEDULE][WARN] grep_files timed out pattern=$pattern" >&2
  elif [[ "$rc" -ne 0 && "$rc" -ne 1 ]]; then
    echo "[KR_SCHEDULE][WARN] grep_files failed rc=$rc pattern=$pattern" >&2
  fi
  return 0
}

check_lock() {
  local session="$1"
  local lock="$2"
  if flock -n "$lock" -c true 2>/dev/null; then
    echo "[KR_LOCK][FREE] session=$session lock=$lock"
    return 0
  fi

  echo "[KR_LOCK][BUSY] session=$session lock=$lock"
  echo "[KR_LOCK][OWNER] session=$session"
  if command -v fuser >/dev/null 2>&1; then
    fuser -v "$lock" 2>&1 || true
  else
    echo "fuser not found"
  fi
}

warn_external_flock() {
  local session="$1"
  local script="$2"
  local lock="$3"
  local warn="$4"
  local line
  local warned=0

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if [[ "$line" == *"$lock"* && ( "$line" == *"$script"* || "$line" == *"flock -n $lock"* || "$line" == *"/usr/bin/flock -n $lock"* ) ]]; then
      echo "$warn"
      warned=1
      break
    fi
  done <<< "$cron_text"

  if ((warned == 0)); then
    echo "[KR_SCHEDULE][OK] $session has no external flock lock=$lock"
  fi
}

section "KR lock status"
for session in prep am afternoon close; do
  check_lock "$session" "${lock_files[$session]}"
done

section "KR prep stale process candidates"
ps -ef | grep -E "run-kr-prep|kr-prep|nullim-kr-prep|prep_runner|trader.kr|python" | grep -v grep || true

section "systemd timers matching kr / trade / nullim"
if command -v systemctl >/dev/null 2>&1; then
  systemctl list-timers --all 2>/dev/null | grep -E -i "$pattern" || true
else
  echo "systemctl not found"
fi

section "systemd units matching kr / trade / nullim"
if command -v systemctl >/dev/null 2>&1; then
  systemctl list-units --all 2>/dev/null | grep -E -i "$pattern" || true
else
  echo "systemctl not found"
fi

section "current user crontab"
cron_text="$(crontab -l 2>/dev/null || true)"
printf '%s\n' "$cron_text"

section "canonical crontab checks"
warn_external_flock \
  "kr-prep" \
  "run-kr-prep.sh" \
  "/tmp/nullim-kr-prep.lock" \
  "[KR_SCHEDULE][WARN] kr-prep external flock detected; remove outer flock because run-kr-prep.sh has internal lock"
warn_external_flock \
  "kr-am" \
  "run-kr-am.sh" \
  "/tmp/nullim-kr-am.lock" \
  "[KR_SCHEDULE][WARN] kr-am external flock detected; remove outer flock because run-kr-am.sh has internal lock"
warn_external_flock \
  "kr-afternoon" \
  "run-kr-afternoon.sh" \
  "/tmp/nullim-kr-afternoon.lock" \
  "[KR_SCHEDULE][WARN] kr-afternoon external flock detected; remove outer flock because run-kr-afternoon.sh has internal lock"
warn_external_flock \
  "kr-close" \
  "run-kr-close.sh" \
  "/tmp/nullim-kr-close.lock" \
  "[KR_SCHEDULE][WARN] kr-close external flock detected; remove outer flock because run-kr-close.sh has internal lock"
cat <<'CRON_NOTE'
[KR_SCHEDULE][CANONICAL] KR crontab should not wrap KR run scripts with /usr/bin/flock because each script owns its internal lock.
[KR_SCHEDULE][CANONICAL] KR prep recommended example with script-internal timeout/lock:
50 6 * * 1-5 cd $APP && $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-prep.sh >> $APP/runtime/cron/kr-prep.log 2>&1
[KR_SCHEDULE][CANONICAL] Optional crontab-level timeout example if operations explicitly wants an outer timeout too:
50 6 * * 1-5 cd $APP && timeout --kill-after=60s 7200 $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-prep.sh >> $APP/runtime/cron/kr-prep.log 2>&1
CRON_NOTE

section "/etc/cron* references to KR WSL run scripts"
cron_roots=()
while IFS= read -r path; do
  [[ -e "$path" ]] && cron_roots+=("$path")
done < <(compgen -G "/etc/cron*" || true)
if ((${#cron_roots[@]})); then
  grep_refs "$script_pattern" "${cron_roots[@]}"
else
  echo "no /etc/cron* paths found"
fi

section "/etc/systemd, ~/.config/systemd, and ~/ references to KR WSL run scripts"
search_roots=(/etc/systemd "$HOME/.config/systemd" "$HOME")
existing_roots=()
for root in "${search_roots[@]}"; do
  [[ -e "$root" ]] && existing_roots+=("$root")
done
if ((${#existing_roots[@]})); then
  grep_refs "$script_pattern" "${existing_roots[@]}"
else
  echo "no search roots found"
fi

section "summary: duplicate schedule hints"
for script in "${scripts[@]}"; do
  count=0
  cron_count=0
  systemd_home_count=0

  if ((${#cron_roots[@]})); then
    cron_count=$(grep_files "$script" "${cron_roots[@]}" | wc -l || true)
  fi
  if ((${#existing_roots[@]})); then
    systemd_home_count=$(grep_files "$script" "${existing_roots[@]}" | wc -l || true)
  fi
  count=$((cron_count + systemd_home_count))

  if ((count >= 2)); then
    echo "[DUPLICATE_POSSIBLE] $script found in $count files; review overlapping cron/systemd/manual schedule sources."
  else
    echo "[OK_OR_MANUAL_ONLY] $script found in $count files."
  fi
done
