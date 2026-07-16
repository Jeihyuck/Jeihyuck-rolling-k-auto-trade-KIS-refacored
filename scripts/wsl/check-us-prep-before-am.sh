#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime runtime/locks

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

LOG_FILE="runtime/wsl-us-prep-guard.log"
PYTHON_BIN="${PYTHON_BIN:-python}"
if [[ -x .venv/bin/python ]]; then PYTHON_BIN=.venv/bin/python; fi

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"
export MARKET="US"
export EXCHANGE="US"
export PB1_MARKET_SCOPE="US"
export WSL_RUN_SOURCE="local-wsl"
export WSL_RUN_MARKET="US"
export WSL_RUN_SESSION="prep_guard"

if [[ -n "${US_FORCE_NOW:-}" ]]; then
  export FORCE_NOW_INPUT="${US_FORCE_NOW}"
fi

cmd=("${PYTHON_BIN}" scripts/guard_us_prep_contract.py --session am)

echo "[$(date -Is)] [US_PREP_GUARD_WSL][START] cmd=${cmd[*]} force_now_input=${FORCE_NOW_INPUT:-}" >> "${LOG_FILE}"
"${cmd[@]}" >> "${LOG_FILE}" 2>&1
