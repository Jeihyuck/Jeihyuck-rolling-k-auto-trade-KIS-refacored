#!/usr/bin/env bash
set -euo pipefail

APP="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"

mkdir -p "$APP/runtime/cron"

NULLIM_CRON_BLOCK="$(cat <<'CRON'
# NULLIM_CRON_START
SHELL=/bin/bash
TZ=Asia/Seoul
APP=/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored

CRON_TZ=Asia/Seoul
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# =========================
# KR market schedule
# =========================

# KR prep: 06:50 KST
50 6 * * 1-5 cd $APP && /usr/bin/flock -n /tmp/nullim-kr-prep.lock env LOCK_DELEGATED=1 $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-prep.sh >> $APP/runtime/cron/kr-prep.log 2>&1

# KR AM: 09:00 KST
0 9 * * 1-5 cd $APP && /usr/bin/flock -n /tmp/nullim-kr-am.lock env LOCK_DELEGATED=1 $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-am.sh >> $APP/runtime/cron/kr-am.log 2>&1

# KR afternoon: 13:30 KST
30 13 * * 1-5 cd $APP && /usr/bin/flock -n /tmp/nullim-kr-afternoon.lock env LOCK_DELEGATED=1 $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-afternoon.sh >> $APP/runtime/cron/kr-afternoon.log 2>&1

# KR close: 15:20 KST
20 15 * * 1-5 cd $APP && /usr/bin/flock -n /tmp/nullim-kr-close.lock env LOCK_DELEGATED=1 $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-kr-close.sh >> $APP/runtime/cron/kr-close.log 2>&1

# =========================
# US market schedule
# KST / US daylight saving time 기준
# =========================

# US prep: 21:30 KST, before US open
30 21 * * 1-5 cd $APP && /usr/bin/flock -n /tmp/nullim-us-prep.lock $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-us-prep.sh >> $APP/runtime/cron/us-prep.log 2>&1

# US AM/open session: 22:30 KST
30 22 * * 1-5 cd $APP && /usr/bin/flock -n /tmp/nullim-us-am.lock $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-us-am.sh >> $APP/runtime/cron/us-am.log 2>&1

# US afternoon: 02:00 KST, next calendar day
0 2 * * 2-6 cd $APP && /usr/bin/flock -n /tmp/nullim-us-afternoon.lock $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-us-afternoon.sh >> $APP/runtime/cron/us-afternoon.log 2>&1

# US close: 05:05 KST, next calendar day
5 5 * * 2-6 cd $APP && /usr/bin/flock -n /tmp/nullim-us-close.lock $APP/scripts/wsl/with-venv.sh $APP/scripts/wsl/run-us-close.sh >> $APP/runtime/cron/us-close.log 2>&1

# =========================
# Log mail schedule
# =========================

# Clean stale KR locks before KR prep
45 6 * * 1-5 cd $APP && $APP/scripts/wsl/cleanup-stale-kr-locks.sh >> $APP/runtime/cron/kr-lock-cleanup.log 2>&1

# US overnight log mail: Tue-Sat 07:00 KST
0 7 * * 2-6 cd $APP && $APP/scripts/wsl/send-market-log-mail.sh us >> $APP/runtime/cron/mail-us.log 2>&1

# KR regular log mail: Mon-Fri 16:00 KST
0 16 * * 1-5 cd $APP && $APP/scripts/wsl/send-market-log-mail.sh kr >> $APP/runtime/cron/mail-kr.log 2>&1
# NULLIM_CRON_END
CRON
)"

existing="$(crontab -l 2>/dev/null || true)"
cleaned="$(printf '%s\n' "$existing" | sed '/# NULLIM_CRON_START/,/# NULLIM_CRON_END/d')"
{
  printf '%s\n' "$cleaned" | sed '/^[[:space:]]*$/d'
  [ -n "${cleaned//[[:space:]]/}" ] && printf '\n'
  printf '%s\n' "$NULLIM_CRON_BLOCK"
} | crontab -

echo "[CRON_INSTALL][OK] updated NULLIM_CRON_START/END block"
crontab -l
