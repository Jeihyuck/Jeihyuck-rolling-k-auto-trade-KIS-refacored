#!/usr/bin/env bash
set -euo pipefail

scripts=(run-kr-prep.sh run-kr-am.sh run-kr-afternoon.sh run-kr-close.sh)
pattern='kr|trade|nullim'
script_pattern='run-kr-prep\.sh|run-kr-am\.sh|run-kr-afternoon\.sh|run-kr-close\.sh'
APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
user_crontab_file="$tmp_dir/user-crontab.txt"

crontab -l >"$user_crontab_file" 2>/dev/null || true
cron_text="$(cat "$user_crontab_file")"

declare -a scheduler_files=("$user_crontab_file")
declare -a system_scheduler_files=()
declare -A scheduler_labels=(["$user_crontab_file"]="current user crontab")

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

add_scheduler_file() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  scheduler_files+=("$file")
  system_scheduler_files+=("$file")
  scheduler_labels["$file"]="$file"
}

shopt -s nullglob
for file in \
  /etc/crontab \
  /etc/cron.d/* \
  /etc/cron.daily/* \
  /etc/cron.hourly/* \
  /etc/cron.weekly/* \
  /etc/cron.monthly/* \
  /etc/systemd/system/* \
  /etc/systemd/user/* \
  "$HOME"/.config/systemd/user/*; do
  add_scheduler_file "$file"
done
shopt -u nullglob

grep_scheduler_refs() {
  local grep_pattern="$1"
  shift
  (($# == 0)) && return 0

  local file label matches
  for file in "$@"; do
    [[ -f "$file" ]] || continue
    label="${scheduler_labels[$file]:-$file}"
    matches="$(grep -nE "$grep_pattern" "$file" 2>/dev/null || true)"
    [[ -z "$matches" ]] && continue
    while IFS= read -r line; do
      [[ -n "$line" ]] && printf '%s:%s\n' "$label" "$line"
    done <<<"$matches"
  done
}

count_scheduler_files() {
  local grep_pattern="$1"
  shift
  if (($# == 0)); then
    echo 0
    return 0
  fi
  (grep -lE "$grep_pattern" "$@" 2>/dev/null || true) | sort -u | wc -l | tr -d ' '
}

count_crontab_lines() {
  local grep_pattern="$1"
  (grep -E "$grep_pattern" "$user_crontab_file" 2>/dev/null || true) | wc -l | tr -d ' '
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
  local warned=0
  local file line label

  for file in "${scheduler_files[@]}"; do
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      if [[ "$line" == *flock* && "$line" == *"$lock"* && "$line" == *"$script"* ]]; then
        label="${scheduler_labels[$file]:-$file}"
        echo "[KR_SCHEDULE][WARN] $session external flock detected in $label; remove outer flock because $script has internal lock"
        warned=1
      fi
    done <"$file"
  done

  if ((warned == 0)); then
    echo "[KR_SCHEDULE][OK] $session has no external flock lock=$lock"
  fi
}

section "KR lock status"
for session in prep am afternoon close; do
  check_lock "$session" "${lock_files[$session]}"
done

section "KR prep stale process candidates"
stale_candidates="$(ps -eo user:20,pid,ppid,etime,args | awk -v app="$APP" '
  NR == 1 { next }
  /networkd-dispatcher/ || /unattended-upgrades/ { next }
  index($0, app) || /run-kr-prep\.sh/ || /nullim-kr-prep\.lock/ || /trader\.kr\.runner\.trade_session_runner/ || /kr-prep/ || /KR_SESSION=prep/ || /session[ =]prep/ { print }
')"
if [[ -n "$stale_candidates" ]]; then
  while IFS= read -r line; do
    [[ -n "$line" ]] && echo "[KR_PREP][STALE_PROCESS][CANDIDATE] $line"
  done <<<"$stale_candidates"
else
  echo "[KR_PREP][STALE_PROCESS][NONE]"
fi

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
printf '%s\n' "$cron_text"

section "canonical crontab checks"
warn_external_flock "kr-prep" "run-kr-prep.sh" "/tmp/nullim-kr-prep.lock"
warn_external_flock "kr-am" "run-kr-am.sh" "/tmp/nullim-kr-am.lock"
warn_external_flock "kr-afternoon" "run-kr-afternoon.sh" "/tmp/nullim-kr-afternoon.lock"
warn_external_flock "kr-close" "run-kr-close.sh" "/tmp/nullim-kr-close.lock"
cat <<'CRON_NOTE'
[KR_SCHEDULE][CANONICAL] KR crontab should not wrap KR run scripts with /usr/bin/flock because each script owns its internal lock.
[KR_SCHEDULE][CANONICAL] KR prep recommended example with script-internal timeout/lock:
50 6 * * 1-5 cd $APP && $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-prep.sh >> $APP/runtime/cron/kr-prep.log 2>&1
[KR_SCHEDULE][CANONICAL] Optional crontab-level timeout example if operations explicitly wants an outer timeout too:
50 6 * * 1-5 cd $APP && timeout --kill-after=60s 7200 $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-prep.sh >> $APP/runtime/cron/kr-prep.log 2>&1
CRON_NOTE

section "actual scheduler references to KR WSL run scripts"
grep_scheduler_refs "$script_pattern" "${scheduler_files[@]}"

section "summary: duplicate schedule hints"
for script in "${scripts[@]}"; do
  crontab_line_count="$(count_crontab_lines "$script")"
  system_file_count="$(count_scheduler_files "$script" "${system_scheduler_files[@]}")"
  count=$((crontab_line_count + system_file_count))

  if ((count >= 2)); then
    echo "[DUPLICATE_POSSIBLE] $script schedule_sources=$count; review actual scheduler sources only."
  elif ((count == 1)); then
    echo "[KR_SCHEDULE][OK] $script schedule_sources=$count"
  else
    echo "[KR_SCHEDULE][WARN] $script schedule_sources=0"
  fi
done
