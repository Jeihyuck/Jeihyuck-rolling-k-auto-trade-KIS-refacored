#!/usr/bin/env bash
set -euo pipefail
# Canonical Ubuntu cron contract.  All jobs enter through the apps checkout so
# deploy-preflight can reject a stale or accidentally patched checkout.
existing="$(crontab -l 2>/dev/null || true)"
cleaned="$(printf '%s\n' "$existing" | sed '/# NULLIM_CRON_START/,/# NULLIM_CRON_END/d')"
APP="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
# Cron opens redirection targets before invoking the runner.  Create this at
# install time so a fresh canonical checkout cannot silently skip KR sessions.
mkdir -p "$APP/runtime/cron_logs"
{
  printf '%s\n' "$cleaned" | sed '/^[[:space:]]*$/d'
  cat <<EOF
# NULLIM_CRON_START
CRON_TZ=Asia/Seoul
TZ=Asia/Seoul
30 6 * * 1-5 cd $APP && ./run_pb1_kr.sh prep >> runtime/cron_logs/kr-prep.log 2>&1
0 9 * * 1-5 cd $APP && ./run_pb1_kr.sh am >> runtime/cron_logs/kr-am.log 2>&1
0 13 * * 1-5 cd $APP && ./run_pb1_kr.sh pm >> runtime/cron_logs/kr-pm.log 2>&1
15 15 * * 1-5 cd $APP && ./run_pb1_kr.sh close >> runtime/cron_logs/kr-close.log 2>&1
CRON_TZ=America/New_York
0 6 * * 1-5 cd $APP && /usr/bin/env bash scripts/wsl/run-us-prep.sh
30 9 * * 1-5 cd $APP && /usr/bin/env bash scripts/wsl/run-us-am.sh
0 13 * * 1-5 cd $APP && /usr/bin/env bash scripts/wsl/run-us-afternoon.sh
10 16 * * 1-5 cd $APP && /usr/bin/env bash scripts/wsl/run-us-close.sh
# NULLIM_CRON_END
EOF
} | crontab -
echo "[CRON_INSTALL][OK] app_dir=$APP"
