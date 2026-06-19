#!/usr/bin/env bash
set -euo pipefail

scripts=(run-kr-am run-kr-afternoon run-kr-close)
pattern='kr|trade|nullim'
script_pattern='run-kr-am|run-kr-afternoon|run-kr-close'

section() {
  echo
  echo "========== $* =========="
}

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
crontab -l 2>/dev/null || true

section "/etc/cron* references to KR WSL run scripts"
if compgen -G "/etc/cron*" >/dev/null; then
  grep -R -n -E "$script_pattern" /etc/cron* 2>/dev/null || true
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
  grep -R -n -E "$script_pattern" "${existing_roots[@]}" 2>/dev/null || true
else
  echo "no search roots found"
fi

section "summary: duplicate schedule hints"
for script in "${scripts[@]}"; do
  count=0
  cron_count=0
  systemd_home_count=0

  if compgen -G "/etc/cron*" >/dev/null; then
    cron_count=$(grep -R -l -E "$script" /etc/cron* 2>/dev/null | wc -l || true)
  fi
  if ((${#existing_roots[@]})); then
    systemd_home_count=$(grep -R -l -E "$script" "${existing_roots[@]}" 2>/dev/null | wc -l || true)
  fi
  count=$((cron_count + systemd_home_count))

  if ((count >= 2)); then
    echo "[DUPLICATE_POSSIBLE] $script found in $count files; review overlapping cron/systemd/manual schedule sources."
  else
    echo "[OK_OR_MANUAL_ONLY] $script found in $count files."
  fi
done
