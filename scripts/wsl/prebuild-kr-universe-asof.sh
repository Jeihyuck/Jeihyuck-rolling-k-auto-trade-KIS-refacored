#!/usr/bin/env bash
# Read-only KR universe prebuild for an explicitly completed KRX trading date.
# This wrapper is NOT a scheduler and must not submit broker orders.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$APP_DIR"

AS_OF="${1:-}"
if [[ ! "$AS_OF" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Usage: bash scripts/wsl/prebuild-kr-universe-asof.sh YYYY-MM-DD" >&2
  exit 2
fi

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

# A prebuild must never use a future date, or read today's unfinalized data.
# Operator must still verify KRX publication availability before scheduling.
AS_OF="$AS_OF" python - <<'PY'
import os
from datetime import date, time
from trader.time_utils import now_kst

requested = date.fromisoformat(os.environ["AS_OF"])
now = now_kst()
if requested > now.date():
    raise SystemExit("KR_UNIVERSE_PREBUILD_FUTURE_ASOF_BLOCKED")
if requested == now.date() and now.time() < time(16, 30):
    raise SystemExit("KR_UNIVERSE_PREBUILD_BEFORE_EOD_BLOCKED")
print(f"[KR_UNIVERSE][PREBUILD][DATE_GUARD] as_of={requested}")
PY

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ORDER_ALLOWED=0
export US_KIS_ORDER_ALLOWED=0
export PB1_KIS_ORDER_ALLOWED=0
TIMEOUT="${KR_UNIVERSE_PREBUILD_TIMEOUT_SEC:-3600}"
echo "[KR_UNIVERSE][PREBUILD][START] as_of=$AS_OF env=$STRATEGY_ENV max_sec=$TIMEOUT"
timeout --kill-after=30s "$TIMEOUT" \
  python -m trader.universe.build \
    --env "$STRATEGY_ENV" --strategy best_k_meta \
    --date "$AS_OF" --provider krx_marketcap_asof --prep-recovery
echo "[KR_UNIVERSE][PREBUILD][DONE] as_of=$AS_OF"
