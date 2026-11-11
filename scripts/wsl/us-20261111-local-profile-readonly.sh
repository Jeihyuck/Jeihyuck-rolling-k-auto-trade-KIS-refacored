#!/usr/bin/env bash
# Read-only WSL acceptance gate for the 2026-11-11 US practice rollout.
# Does not connect to KIS, submit an order, or modify Supabase.
# Run only after an actual dual-agent deployment and .env loading.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
if [[ -f .env ]]; then
  set -a
  # Matches the existing production WSL session runner's .env load.
  source .env
  set +a
fi
export KIS_ENV="${KIS_ENV:-practice}"
export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
source scripts/wsl/us-swing-v3-paper-defaults.sh
export US_STAGE4_EXIT_ONLY_FALLBACK_ENABLED="${US_STAGE4_EXIT_ONLY_FALLBACK_ENABLED:-1}"
PYTHON_BIN=python3
[[ -x .venv/bin/python ]] && PYTHON_BIN=.venv/bin/python
"${PYTHON_BIN}" - <<'PY'
import json
import os
import subprocess
from trader.us.entry_exit_contract import (
    US_SWING_V3_CONTRACT_VERSION, build_us_entry_exit_contract,
)
from trader.us.infinite.config import InfiniteConfig

branch = subprocess.check_output(
    ["git","branch","--show-current"],text=True,timeout=10,
).strip()
sha = subprocess.check_output(
    ["git","rev-parse","HEAD"],text=True,timeout=10,
).strip()
policy = build_us_entry_exit_contract({
    "symbol":"AAPL", "strategy_owner":"US_STANDARD",
    "position_action":"NEW_POSITION_BUY",
    "book":"SWING_BOOK", "horizon":"SWING_CARRY",
})
tqqq = InfiniteConfig.from_env()
profile = {
    "branch":branch, "head_short":sha[:12],
    "env":os.getenv("KIS_ENV"),
    "strategy_env":os.getenv("STRATEGY_ENV"),
    "v3_new_buy_active":policy.get("version")==US_SWING_V3_CONTRACT_VERSION,
    "v3_approved":os.getenv("US_SWING_V3_DEPLOY_APPROVED")=="1",
    "tqqq_micro_enabled":tqqq.defensive_micro_first_buy_enabled,
    "tqqq_micro_max_usd":tqqq.defensive_micro_first_buy_max_usd,
    "stage4_verified_exit_only_fallback_enabled":(
        os.getenv("US_STAGE4_EXIT_ONLY_FALLBACK_ENABLED")=="1"),
    "native_kis_broker_order_submitted":False,
    "proof_level":"LOCAL_WSL_CONFIG_AND_CODE_ONLY",
    "live_kis_fill_and_protective_sell_verified":False,
}
passed = (
    branch=="dual-agent"
    and profile["env"]=="practice"
    and profile["strategy_env"]=="practice"
    and profile["v3_new_buy_active"] and profile["v3_approved"]
    and profile["tqqq_micro_enabled"]
    and 0 < tqqq.defensive_micro_first_buy_max_usd <= 125.0
    and profile["stage4_verified_exit_only_fallback_enabled"]
)
profile["status"]="LOCAL_PROFILE_PASS" if passed else "LOCAL_PROFILE_BLOCKED"
print(json.dumps(profile,sort_keys=True,ensure_ascii=False))
raise SystemExit(0 if passed else 43)
PY
