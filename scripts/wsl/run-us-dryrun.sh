#!/usr/bin/env bash
set -euo pipefail
export DRY_RUN=1
export DISABLE_LIVE_TRADING=1
export LIVE_TRADING_ENABLED=0
export STRATEGY_MODE=INTENT_ONLY
export FORCE_STRATEGY_MODE=INTENT_ONLY
exec /home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored/scripts/wsl/run-us-trader.sh
