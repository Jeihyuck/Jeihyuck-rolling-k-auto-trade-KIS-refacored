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
  prep) export PB1_JOB=BUILD_WATCHLIST PB1_SESSION_KIND=prep FORCE_MARKET_WINDOW=preopen FORCE_PB1_PHASE=verify PB1_LOOP_ENABLED=0; window=preopen; pb1_phase=verify;;
  am) export PB1_JOB=TRADE_INTRADAY PB1_SESSION_KIND=am FORCE_MARKET_WINDOW=morning FORCE_PB1_PHASE=entry PB1_LOOP_ENABLED=1; window=morning; pb1_phase=entry;;
  pm) export PB1_JOB=TRADE_INTRADAY PB1_SESSION_KIND=pm FORCE_MARKET_WINDOW=day FORCE_PB1_PHASE=entry PB1_LOOP_ENABLED=1; window=day; pb1_phase=entry;;
  close) export PB1_JOB=TRADE_INTRADAY PB1_SESSION_KIND=close FORCE_MARKET_WINDOW=close FORCE_PB1_PHASE=exit PB1_LOOP_ENABLED=0; window=close; pb1_phase=exit;;
esac
result_dir="runtime/kr/session/$(TZ=Asia/Seoul date +%F)/${phase}"
mkdir -p "$result_dir"
export PB1_SESSION_RESULT_PATH="$result_dir/pb1_result.json"
echo "[RUN][START] phase=$phase app_dir=$(pwd -P) branch=$(git branch --show-current) head=$(git rev-parse --short HEAD)"
echo "[RUN][ENV] STRATEGY_ENV=$STRATEGY_ENV KIS_ENV=$KIS_ENV ENV=$ENV STRATEGY=$STRATEGY"
set +e
python -m trader.pb1_runner --window "$window" --phase "$pb1_phase" --env "$STRATEGY_ENV"; status=$?
set -e
echo "[RUN][DONE] phase=$phase status=$status"
exit "$status"
