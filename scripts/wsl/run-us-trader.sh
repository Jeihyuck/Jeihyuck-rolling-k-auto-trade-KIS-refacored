#!/usr/bin/env bash
set -euo pipefail

repo_dir="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
cd "${repo_dir}"
mkdir -p runtime

# Dispatcher wrapper for WSL. Prefer calling the session-specific scripts from
# Windows Task Scheduler; this auto mode is a convenience fallback.
session="${1:-auto}"
if [[ "${session}" == "auto" ]]; then
  et_hhmm="$(TZ=America/New_York date +%H%M)"
  case "${et_hhmm}" in
    05*|06*|07*) session="prep" ;;
    09*|10*|11*|12[0-2]*) session="am" ;;
    12[3-5]*|13*|14*|15[0-4]*) session="afternoon" ;;
    15[5-9]*|16*) session="close" ;;
    *)
      echo "[WSL_US_TRADER][SKIP] no US session for et_hhmm=${et_hhmm}" >> runtime/wsl-us-trader.log
      exit 0
      ;;
  esac
fi

echo "[WSL_US_TRADER][DISPATCH] session=${session}" >> runtime/wsl-us-trader.log
case "${session}" in
  prep) exec "${repo_dir}/scripts/wsl/run-us-prep.sh" ;;
  am|trade-am|session-am) exec "${repo_dir}/scripts/wsl/run-us-am.sh" ;;
  afternoon|trade-afternoon|session-afternoon) exec "${repo_dir}/scripts/wsl/run-us-afternoon.sh" ;;
  close|trade-close) exec "${repo_dir}/scripts/wsl/run-us-close.sh" ;;
  *)
    echo "[WSL_US_TRADER][ERROR] unknown session=${session}" >> runtime/wsl-us-trader.log
    exit 2
    ;;
esac
