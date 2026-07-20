#!/usr/bin/env bash
set -euo pipefail

phase="${1:-}"
case "$phase" in prep|am|pm|close) ;; *) echo "usage: $0 {prep|am|pm|close}" >&2; exit 64;; esac
APP_DIR="${NULLIM_APP_DIR:-/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored}"
cd "$APP_DIR"
# shellcheck disable=SC1091
source scripts/wsl/deploy-preflight.sh
deploy_preflight
for env_file in .env .env.kr .env.local; do
  [[ -f "$env_file" ]] && { set -a; source "$env_file"; set +a; }
done
[[ ! -f .venv/bin/activate ]] || source .venv/bin/activate
export STRATEGY_ENV="${STRATEGY_ENV:-practice}" KIS_ENV="${KIS_ENV:-practice}"
export ENV="$STRATEGY_ENV" STRATEGY=best_k_meta PB1_MARKET_SCOPE=KRX MARKET=KRX TRADE_MARKET=KR PB1_BUYABLE_BACKFILL_ENABLED=1
case "$phase" in
  prep) export PB1_JOB=build-watchlist PB1_SESSION_KIND=prep FORCE_MARKET_WINDOW=prep FORCE_PB1_PHASE=prep PB1_LOOP_ENABLED=0; script=scripts/wsl/run-kr-prep.sh;;
  am) export PB1_JOB=trade PB1_SESSION_KIND=am FORCE_MARKET_WINDOW=am FORCE_PB1_PHASE=am PB1_LOOP_ENABLED=1; script=scripts/wsl/run-kr-am.sh;;
  pm) export PB1_JOB=trade PB1_SESSION_KIND=pm FORCE_MARKET_WINDOW=pm FORCE_PB1_PHASE=pm PB1_LOOP_ENABLED=1; script=scripts/wsl/run-kr-afternoon.sh;;
  close) export PB1_JOB=close PB1_SESSION_KIND=close FORCE_MARKET_WINDOW=close FORCE_PB1_PHASE=close PB1_LOOP_ENABLED=0; script=scripts/wsl/run-kr-close.sh;;
esac
echo "[RUN][START] phase=$phase app_dir=$(pwd -P) branch=$(git branch --show-current) head=$(git rev-parse --short HEAD)"
echo "[RUN][ENV] STRATEGY_ENV=$STRATEGY_ENV KIS_ENV=$KIS_ENV ENV=$ENV STRATEGY=$STRATEGY"
set +e
bash "$script"; status=$?
set -e
echo "[RUN][DONE] phase=$phase status=$status"
exit "$status"
