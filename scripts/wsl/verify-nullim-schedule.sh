#!/usr/bin/env bash
set -euo pipefail

APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$APP"

echo "===== BRANCH ====="
git branch --show-current || true

echo "===== CRONTAB ====="
crontab -l | nl -ba

echo "===== REQUIRED CRON CHECK ====="
crontab -l | grep -q "flock -n /tmp/nullim-kr-prep.lock" && echo "[OK] KR prep uses flock" || echo "[FAIL] KR prep flock missing"
crontab -l | grep -q "flock -n /tmp/nullim-kr-am.lock" && echo "[OK] KR am uses flock" || echo "[FAIL] KR am flock missing"
crontab -l | grep -q "flock -n /tmp/nullim-kr-afternoon.lock" && echo "[OK] KR afternoon uses flock" || echo "[FAIL] KR afternoon flock missing"
crontab -l | grep -q "flock -n /tmp/nullim-kr-close.lock" && echo "[OK] KR close uses flock" || echo "[FAIL] KR close flock missing"
crontab -l | grep -q "send-market-log-mail.sh us" && echo "[OK] US mail cron exists" || echo "[FAIL] US mail cron missing"
crontab -l | grep -q "send-market-log-mail.sh kr" && echo "[OK] KR mail cron exists" || echo "[FAIL] KR mail cron missing"
crontab -l | grep -q "cleanup-stale-kr-locks.sh" && echo "[OK] KR lock cleanup cron exists" || echo "[FAIL] KR lock cleanup cron missing"

echo "===== SCRIPT CHECK ====="
for f in \
  scripts/wsl/with-venv.sh \
  scripts/notify/send_mail_attachment.py \
  scripts/wsl/send-market-log-mail.sh \
  scripts/wsl/cleanup-stale-kr-locks.sh
do
  if [ -x "$f" ] || [[ "$f" == *.py ]]; then
    echo "[OK] $f"
  else
    echo "[FAIL] missing or not executable: $f"
  fi
done

echo "===== MAIL ENV CHECK ====="
scripts/wsl/with-venv.sh env | grep -E "MAIL_TO|MAIL_FROM|SMTP_HOST|SMTP_PORT|SMTP_USER|SMTP_PASS" | sed 's/SMTP_PASS=.*/SMTP_PASS=<MASKED>/'

echo "===== KR LOCK CHECK ====="
ps -ef | grep -E "run-kr|kr-prep|kr-am|kr-afternoon|kr-close|pb1_runner|trader.kr|trader.pb1" | grep -v grep || true
ls -la /tmp/nullim-kr-*.lock 2>/dev/null || echo "no /tmp KR lock files"
ls -la runtime/locks/kr*.lock 2>/dev/null || echo "no runtime KR lock files"

echo "===== RECENT MAIL LOGS ====="
tail -100 runtime/cron/mail-us.log 2>/dev/null || true
tail -100 runtime/cron/mail-kr.log 2>/dev/null || true
