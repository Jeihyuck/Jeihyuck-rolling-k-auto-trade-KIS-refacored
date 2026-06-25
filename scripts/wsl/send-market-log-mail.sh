#!/usr/bin/env bash
set -euo pipefail

APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MARKET="${1:-all}"
TS="$(date +%Y%m%d-%H%M%S)"
HUMAN_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"

cd "$APP"
mkdir -p runtime/cron

BRANCH="$(git branch --show-current 2>/dev/null || echo unknown)"
BRANCH="${BRANCH:-unknown}"
BRANCH_SAFE="$(printf '%s' "$BRANCH" | sed 's/[^A-Za-z0-9._-]/-/g')"
BRANCH_SAFE="${BRANCH_SAFE:-unknown}"
KR_LOG_DATE="$(TZ=Asia/Seoul date +%F)"

OUT="/tmp/nullim-${BRANCH_SAFE}-${MARKET}-logs-${TS}.tar.gz"
WARN="/tmp/nullim-${BRANCH_SAFE}-${MARKET}-logs-${TS}.warn"

case "$MARKET" in
  us)
    SUBJECT="[NULLIM][${BRANCH}][US][LOG] overnight logs ${HUMAN_TS}"
    BODY="미국장 overnight 로그 자동 발송입니다. branch=${BRANCH}"
    FILES=(runtime/cron runtime/wsl-us-prep.log runtime/wsl-us-am.log runtime/wsl-us-afternoon.log runtime/wsl-us-close.log runtime/wsl-us-trader.log reports/us_liveness reports/us_daily/latest_us_daily_report.md reports/us_daily/latest_us_daily_report.json reports/us_prep/latest_us_prep_summary.md reports/us_prep/latest_us_prep_summary.json reports/us_schedule_health)
    ;;
  kr)
    SUBJECT="[NULLIM][${BRANCH}][KR][LOG] regular logs ${HUMAN_TS}"
    BODY="한국장 정규장 로그 자동 발송입니다. branch=${BRANCH}"
    FILES=(runtime/cron runtime/wsl-kr-prep.log runtime/wsl-kr-am.log runtime/wsl-kr-afternoon.log runtime/wsl-kr-close.log runtime/wsl-kr-trader.log runtime/logs/kr/${KR_LOG_DATE} runtime/logs/kr runtime/locks reports)
    ;;
  *)
    SUBJECT="[NULLIM][${BRANCH}][ALL][LOG] logs ${HUMAN_TS}"
    BODY="미국장/한국장 전체 로그 자동 발송입니다. branch=${BRANCH}"
    FILES=(runtime/cron runtime/wsl-*.log runtime/locks reports)
    ;;
esac

echo "[LOG_MAIL][START] market=${MARKET} branch=${BRANCH} out=${OUT}"

EXISTING_FILES=()
for item in "${FILES[@]}"; do
  matches=()
  if [[ "$item" == *'*'* ]]; then
    shopt -s nullglob
    matches=( $item )
    shopt -u nullglob
  elif [ -e "$item" ]; then
    matches=("$item")
  fi
  if [ "${#matches[@]}" -gt 0 ]; then
    EXISTING_FILES+=("${matches[@]}")
  else
    echo "[LOG_MAIL][MISSING_OPTIONAL] $item"
  fi
done

if [ "${#EXISTING_FILES[@]}" -eq 0 ]; then
  FALLBACK="/tmp/nullim-${BRANCH_SAFE}-${MARKET}-no-logs-${TS}.txt"
  { echo "No logs found."; echo "market=${MARKET}"; echo "branch=${BRANCH}"; echo "ts=${HUMAN_TS}"; } > "$FALLBACK"
  EXISTING_FILES=("$FALLBACK")
fi

tar -czf "$OUT" "${EXISTING_FILES[@]}" 2>"$WARN" || true

if [ ! -s "$OUT" ]; then
  FALLBACK="/tmp/nullim-${BRANCH_SAFE}-${MARKET}-empty-archive-${TS}.txt"
  { echo "Archive was empty."; echo "market=${MARKET}"; echo "branch=${BRANCH}"; echo "ts=${HUMAN_TS}"; } > "$FALLBACK"
  tar -czf "$OUT" "$FALLBACK"
fi

echo "[LOG_MAIL][TAR] out=${OUT} size=$(stat -c%s "$OUT" 2>/dev/null || echo 0)"

if [ -s "$WARN" ]; then
  echo "[LOG_MAIL][TAR_WARN]"
  cat "$WARN"
fi

"$APP/scripts/wsl/with-venv.sh" python "$APP/scripts/notify/send_mail_attachment.py" \
  --subject "$SUBJECT" \
  --body "$BODY" \
  --attach "$OUT"

echo "[LOG_MAIL][OK] market=${MARKET} branch=${BRANCH} out=${OUT}"
