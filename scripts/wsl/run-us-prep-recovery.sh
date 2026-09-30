#!/usr/bin/env bash
set -euo pipefail
export NULLIM_RUN_PURPOSE="prep-recovery"

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/init-session-log.sh"
nullim_init_session_log US prep-recovery prep-recovery "${BASH_SOURCE[0]}"
set +e
nullim_require_trading_day us "$NULLIM_TRADE_DATE"
trading_day_rc=$?
set -e
[[ "$trading_day_rc" == 10 ]] && exit 0
[[ "$trading_day_rc" == 0 ]] || exit "$trading_day_rc"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
export WSL_RUN_MARKET="US"
export MARKET="US"
export WSL_RUN_SESSION="prep_recovery"
export PB1_SESSION="prep_recovery"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi
mkdir -p runtime runtime/locks runtime/health

is_effective_prep() {
python - "$NULLIM_TRADE_DATE" <<'PY'
import sys
trade_date = sys.argv[1]
from trader.us.path_contract import load_us_final30_scored, load_us_prep_contract
from trader.us.prep_effective import is_effective_prep_contract
contract = load_us_prep_contract(trade_date) or {}
rows = load_us_final30_scored(trade_date) or []
status = str(contract.get("status") or "").upper()
if is_effective_prep_contract(contract, rows, trade_date=trade_date, min_rows=10):
    print(f"[US_PREP_RECOVERY][SKIP_EFFECTIVE_PREP] trade_date={trade_date} status={status} final30={len(rows)}")
    raise SystemExit(0)
raise SystemExit(1)
PY
}

# A completed same-day PREP is the effective contract. Never create a newer
# STARTED recovery row over an already valid contract merely because a caller
# asks recovery to run again. Entry-blocked OK_WITH_WARNINGS_* contracts are
# still effective because exit/close liveness remains valid.
if is_effective_prep; then
  rm -f "runtime/health/us-prep-missing-${NULLIM_TRADE_DATE}.json"
  exit 0
fi

export US_PREP_RECOVERY_RUN="${US_PREP_RECOVERY_RUN:-1}"
export US_ALLOW_DEGRADED_IN_TRADE="${US_ALLOW_DEGRADED_IN_TRADE:-1}"
export US_WSL_RECOVERY_SOURCE="scheduler-pre-am-recovery"

# Preserve the single-owner/shared-lock process handoff.  run-us-prep.sh owns
# post-PREP effective-contract cleanup, including stale EXIT_ONLY marker removal.
exec bash scripts/wsl/run-us-prep.sh
