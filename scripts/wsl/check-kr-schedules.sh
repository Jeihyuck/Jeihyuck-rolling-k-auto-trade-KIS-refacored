#!/usr/bin/env bash
set -euo pipefail

scripts=(run-kr-prep run-kr-am run-kr-afternoon run-kr-close)
pattern='kr|trade|nullim'
script_pattern='run-kr-prep|run-kr-am|run-kr-afternoon|run-kr-close'
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

rg_or_true() {
  if command -v rg >/dev/null 2>&1; then
    rg "$@" || true
  else
    grep "$@" || true
  fi
}

check_lock() {
  local name="$1"
  local lock="$2"
  if flock -n "$lock" -c true 2>/dev/null; then
    if [[ "$name" == "prep" ]]; then
      echo "[KR_LOCK][FREE] prep lock=$lock"
    else
      echo "[KR_LOCK][FREE] $name lock=$lock"
    fi
    return 0
  fi

  if [[ "$name" == "prep" ]]; then
    echo "[KR_LOCK][BUSY] prep lock=$lock"
  else
    echo "[KR_LOCK][BUSY] $name lock=$lock"
  fi
  if command -v fuser >/dev/null 2>&1; then
    fuser -v "$lock" 2>&1 || true
  else
    echo "fuser not found"
  fi
}

section "KR lock status"
for name in prep am afternoon close; do
  check_lock "$name" "${lock_files[$name]}"
done

section "prep stale process hints"
ps -ef | grep -E "run-kr-prep|kr-prep|nullim-kr-prep|prep_runner|python" | grep -v grep || true

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
if [[ "$cron_text" == *"/usr/bin/flock -n /tmp/nullim-kr-prep.lock"* ]] && grep -Eq 'KR_PREP\]\[LOCK_ACQUIRED|/tmp/nullim-kr-prep.lock' scripts/wsl/run-kr-prep.sh; then
  echo "[KR_SCHEDULE][WARN] kr-prep external flock detected; remove outer flock because run-kr-prep.sh has internal lock"
fi
for item in \
  "kr-prep:/tmp/nullim-kr-prep.lock" \
  "kr-am:/tmp/nullim-kr-am.lock" \
  "kr-afternoon:/tmp/nullim-kr-afternoon.lock" \
  "kr-close:/tmp/nullim-kr-close.lock"; do
  script="${item%%:*}"
  lock="${item#*:}"
  if [[ "$cron_text" == *"/usr/bin/flock -n $lock"* ]]; then
    echo "[KR_SCHEDULE][WARN] $script external flock detected; remove outer flock because run script has internal lock lock=$lock"
  else
    echo "[KR_SCHEDULE][OK] $script has no external flock lock=$lock"
  fi
done
cat <<'CRON_NOTE'
[KR_SCHEDULE][CANONICAL] KR prep crontab should not wrap /tmp/nullim-kr-prep.lock with /usr/bin/flock.
[KR_SCHEDULE][CANONICAL] Example with script-internal timeout/lock:
50 6 * * 1-5 cd $APP && $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-prep.sh >> $APP/runtime/cron/kr-prep.log 2>&1
CRON_NOTE

section "/etc/cron* references to KR WSL run scripts"
if compgen -G "/etc/cron*" >/dev/null; then
  rg_or_true -n -E "$script_pattern" /etc/cron* 2>/dev/null
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
  rg_or_true -n -E "$script_pattern" "${existing_roots[@]}" 2>/dev/null
else
  echo "no search roots found"
fi

section "summary: duplicate schedule hints"
for script in "${scripts[@]}"; do
  count=0
  cron_count=0
  systemd_home_count=0

  if compgen -G "/etc/cron*" >/dev/null; then
    cron_count=$(rg_or_true -l -E "$script" /etc/cron* 2>/dev/null | wc -l || true)
  fi
  if ((${#existing_roots[@]})); then
    systemd_home_count=$(rg_or_true -l -E "$script" "${existing_roots[@]}" 2>/dev/null | wc -l || true)
  fi
  count=$((cron_count + systemd_home_count))

  if ((count >= 2)); then
    echo "[DUPLICATE_POSSIBLE] $script found in $count files; review overlapping cron/systemd/manual schedule sources."
  else
    echo "[OK_OR_MANUAL_ONLY] $script found in $count files."
  fi
done
