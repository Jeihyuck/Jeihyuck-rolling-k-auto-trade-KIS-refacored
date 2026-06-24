#!/usr/bin/env bash
set -euo pipefail

APP="${APP:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$APP"

# 1) 기존에 저장해둔 메일 설정 우선 로드
MAIL_ENV_PRIMARY="/home/infiny/.config/pb1-trader/mail.env"
MAIL_ENV_LOCAL="$APP/runtime/private/mail.env"

if [[ -f "$MAIL_ENV_PRIMARY" ]]; then
  set -a
  source "$MAIL_ENV_PRIMARY"
  set +a
  echo "[ENV][MAIL][LOADED] $MAIL_ENV_PRIMARY"
elif [[ -f "$MAIL_ENV_LOCAL" ]]; then
  set -a
  source "$MAIL_ENV_LOCAL"
  set +a
  echo "[ENV][MAIL][LOADED] $MAIL_ENV_LOCAL"
else
  echo "[ENV][MAIL][WARN] no mail env found"
fi

# 2) 예전 변수명과 새 변수명 호환
export MAIL_TO="${MAIL_TO:-${EMAIL_TO:-${NAVER_MAIL_TO:-${REPORT_MAIL_TO:-}}}}"
export MAIL_FROM="${MAIL_FROM:-${EMAIL_FROM:-${SMTP_FROM:-${EMAIL_USER:-${SMTP_USER:-${NAVER_SMTP_USER:-}}}}}}"
export SMTP_HOST="${SMTP_HOST:-${EMAIL_HOST:-${MAIL_HOST:-${NAVER_SMTP_HOST:-smtp.naver.com}}}}"
export SMTP_PORT="${SMTP_PORT:-${EMAIL_PORT:-${MAIL_PORT:-${NAVER_SMTP_PORT:-587}}}}"
export SMTP_USER="${SMTP_USER:-${EMAIL_USER:-${MAIL_USER:-${NAVER_SMTP_USER:-${MAIL_FROM:-}}}}}"
export SMTP_PASS="${SMTP_PASS:-${EMAIL_PASS:-${EMAIL_PASSWORD:-${MAIL_PASS:-${NAVER_SMTP_PASS:-}}}}}"

if [[ ! -x "$APP/.venv/bin/python" ]]; then
  echo "[VENV][ERROR] missing python=$APP/.venv/bin/python"
  exit 127
fi

export VIRTUAL_ENV="$APP/.venv"
export PATH="$APP/.venv/bin:$PATH"
export PYTHONPATH="$APP:${PYTHONPATH:-}"

echo "[VENV][OK] python=$("$APP/.venv/bin/python" -c 'import sys; print(sys.executable)')"
echo "[VENV][OK] version=$("$APP/.venv/bin/python" --version)"

if [[ $# -lt 1 ]]; then
  echo "[VENV][ERROR] no command provided"
  exit 2
fi

exec "$@"
