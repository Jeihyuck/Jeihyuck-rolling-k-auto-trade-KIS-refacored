#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
MAIL_ENV_PRIMARY="/home/infiny/.config/pb1-trader/mail.env"
MAIL_ENV_LOCAL="$APP_DIR/runtime/private/mail.env"
if [[ -f "$MAIL_ENV_PRIMARY" ]]; then
  set -a; source "$MAIL_ENV_PRIMARY"; set +a
elif [[ -f "$MAIL_ENV_LOCAL" ]]; then
  set -a; source "$MAIL_ENV_LOCAL"; set +a
fi
export MAIL_TO="${MAIL_TO:-${EMAIL_TO:-${NAVER_MAIL_TO:-${REPORT_MAIL_TO:-}}}}"
export MAIL_FROM="${MAIL_FROM:-${EMAIL_FROM:-${SMTP_FROM:-${EMAIL_USER:-${SMTP_USER:-${NAVER_SMTP_USER:-}}}}}}"
export SMTP_HOST="${SMTP_HOST:-${EMAIL_HOST:-${MAIL_HOST:-${NAVER_SMTP_HOST:-smtp.naver.com}}}}"
export SMTP_PORT="${SMTP_PORT:-${EMAIL_PORT:-${MAIL_PORT:-${NAVER_SMTP_PORT:-587}}}}"
export SMTP_USER="${SMTP_USER:-${EMAIL_USER:-${MAIL_USER:-${NAVER_SMTP_USER:-${MAIL_FROM:-}}}}}"
export SMTP_PASS="${SMTP_PASS:-${EMAIL_PASS:-${EMAIL_PASSWORD:-${MAIL_PASS:-${NAVER_SMTP_PASS:-}}}}}"
if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi
export PYTHONPATH="$APP_DIR:${PYTHONPATH:-}"
[[ $# -gt 0 ]] || { echo "[VENV][ERROR] no command provided"; exit 2; }
exec "$@"
