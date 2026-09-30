#!/usr/bin/env bash
set -euo pipefail
# Set scope before any Python helper import; US jobs must never load KR providers.
export MARKET_SCOPE="us"
export TRADING_MARKET="us"
export DISABLE_KR_IMPORTS_IN_US="1"
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/init-session-log.sh"
nullim_init_session_log US prep "${NULLIM_RUN_PURPOSE:-prep}" "${BASH_SOURCE[0]}"
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
export WSL_RUN_SESSION="prep"
export PB1_SESSION="prep"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime runtime/locks runtime/health

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi
nullim_reassert_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"

# .env may carry Korean defaults; pin the process back to US before lock helpers run.
export MARKET_SCOPE="us"
export TRADING_MARKET="us"
export DISABLE_KR_IMPORTS_IN_US="1"

SESSION_NAME="prep"
LOCK_FILE="runtime/locks/us-${SESSION_NAME}.lock"
LOG_FILE="$NULLIM_SESSION_LOG"
source scripts/wsl/session-lock.sh
set +e
nullim_session_lock_acquire "$LOCK_FILE" US "$SESSION_NAME" "$NULLIM_TRADE_DATE" "$LOG_FILE"
lock_rc=$?
set -e
case "$lock_rc" in
  0) ;;
  75|76)
    export NULLIM_SESSION_FINAL_STATUS=SKIP_DUPLICATE NULLIM_SESSION_FINAL_REASON=SESSION_LOCK_HELD
    exit 0
    ;;
  *)
    echo "[LOCK][ACQUIRE][FAIL] rc=$lock_rc lock=${LOCK_FILE:-${lock_file:-unknown}}" >&2
    exit "$lock_rc"
    ;;
esac
cleanup() {
  exit_code=$?
  trap - EXIT INT TERM
  nullim_finish_session_log "$exit_code" || true
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"
export MARKET="US"
export EXCHANGE="US"
export PB1_MARKET_SCOPE="US"
export WSL_RUN_SOURCE="local-wsl"
export US_RUN_SOURCE="${US_RUN_SOURCE:-WINDOWS_SCHEDULE}"
export WSL_RUN_MARKET="US"
export WSL_RUN_SESSION="prep"

# PREP scans a large candidate universe and must not own the shared realtime
# KIS WebSocket/AppKey. Current-price lookups keep their existing REST/DB
# fallback path; only the process-local realtime subscription service is off.
# Trade sessions retain the repository default (WebSocket-first).
export KIS_WS_PRICE_ENABLED="0"
echo "[US_PREP][WS_OWNERSHIP] websocket_enabled=0 owner=TRADE_ONLY fallback=REST_DB" >> "$LOG_FILE"

export TRADING_REGION="${TRADING_REGION:-US}"
export US_AGENT_ENABLED="${US_AGENT_ENABLED:-1}"
export US_PAPER_TRADING_ENABLED="${US_PAPER_TRADING_ENABLED:-1}"
export US_LIVE_TRADING_ENABLED="${US_LIVE_TRADING_ENABLED:-0}"
export DISABLE_REAL_TRADING="${DISABLE_REAL_TRADING:-1}"
export ALLOW_REAL_ORDER="${ALLOW_REAL_ORDER:-0}"
export US_STRATEGY_ENGINE="${US_STRATEGY_ENGINE:-pb1}"
export US_ENTRY_ENABLED="${US_ENTRY_ENABLED:-1}"
export US_EXIT_ENABLED="${US_EXIT_ENABLED:-1}"
export US_DEFAULT_ENTRY_BOOK="${US_DEFAULT_ENTRY_BOOK:-SWING_BOOK}"
export US_DEFAULT_ENTRY_HORIZON="${US_DEFAULT_ENTRY_HORIZON:-SWING_CARRY}"
export US_DEFAULT_EXIT_POLICY="${US_DEFAULT_EXIT_POLICY:-US_SWING_DEFAULT}"
export US_MAX_ORDER_USD="${US_MAX_ORDER_USD:-2500}"
export US_MAX_DAILY_NOTIONAL_USD="${US_MAX_DAILY_NOTIONAL_USD:-10000}"
export US_MAX_POSITIONS="${US_MAX_POSITIONS:-30}"
export US_SESSION_INTERVAL_SEC="${US_SESSION_INTERVAL_SEC:-300}"
export US_WATCHLIST_LOAD_TIMEOUT_SEC="${US_WATCHLIST_LOAD_TIMEOUT_SEC:-20}"
export US_ENTRY_EVAL_TIMEOUT_SEC="${US_ENTRY_EVAL_TIMEOUT_SEC:-120}"
export US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-240}"
export US_TICK_TIMEOUT_MIN_SEC="${US_TICK_TIMEOUT_MIN_SEC:-240}"

export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"

PYTHON_BIN="python"
if [[ -x .venv/bin/python ]]; then PYTHON_BIN=.venv/bin/python; fi
cmd=("${PYTHON_BIN}" -m trader.us.runner.dispatcher --mode prep --env practice)
if [[ -n "${US_FORCE_NOW:-}" ]]; then
  cmd+=(--force-now "${US_FORCE_NOW}")
  export FORCE_NOW_INPUT="${US_FORCE_NOW}"
fi
if [[ "${US_OFFLINE:-0}" == "1" ]]; then
  cmd+=(--offline)
fi

set +e
"${cmd[@]}" >> "$LOG_FILE" 2>&1
prep_rc=$?
set -e

# A preflight may have written EXIT_ONLY while a genuine recovery PREP was still
# STARTED. Remove that marker only after this PREP has finished successfully and
# both the canonical artifact and DB locked-watchlist provenance prove the live
# contract. Entry policy is not changed; entry-blocked OK_WITH_WARNINGS_* stays
# entry-blocked according to its own split permissions.
if [[ "$prep_rc" == 0 ]]; then
  set +e
  "$PYTHON_BIN" - "$NULLIM_TRADE_DATE" <<'PY' >> "$LOG_FILE" 2>&1
import sys
from pathlib import Path
trade_date = sys.argv[1]
from trader.us.path_contract import load_us_final30_scored, load_us_prep_contract
from trader.us.prep_effective import (
    is_effective_prep_contract,
    is_effective_prep_status,
    locked_rows_match_prep_run,
)
from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
from trader.us.score_columns import validate_us_entry_provenance_contract
contract = load_us_prep_contract(trade_date) or {}
artifact_rows = load_us_final30_scored(trade_date) or []
if not is_effective_prep_contract(contract, artifact_rows, trade_date=trade_date, min_rows=10):
    raise SystemExit(1)
prep = load_latest_us_prep_status(trade_date) or {}
locked_rows = load_locked_us_watchlist(trade_date=trade_date, min_count=10, allow_degraded=True) or []
provenance = validate_us_entry_provenance_contract(locked_rows)
prep_run_id = str(prep.get("run_id") or "").strip()
contract_run_id = str(contract.get("run_id") or "").strip()
run_id_ok = locked_rows_match_prep_run(prep, locked_rows)
artifact_run_id_ok = (not contract_run_id) or contract_run_id == prep_run_id
db_ok = (
    is_effective_prep_status(prep.get("status"))
    and len(locked_rows) >= 10
    and provenance.get("ok") is True
    and run_id_ok
    and artifact_run_id_ok
)
if not db_ok:
    locked_run_ids = sorted({str(row.get("run_id") or "").strip() for row in locked_rows if isinstance(row, dict)})
    print(
        "[US_PREP][POST_COMPLETE_CONTRACT_CHECK] status=DB_PROVENANCE_NOT_EFFECTIVE "
        f"prep_status={prep.get('status')} prep_run_id={prep_run_id or '-'} "
        f"contract_run_id={contract_run_id or '-'} locked_run_ids={locked_run_ids} "
        f"locked={len(locked_rows)} provenance_ok={provenance.get('ok')} run_id_ok={int(run_id_ok)} "
        f"artifact_run_id_ok={int(artifact_run_id_ok)}"
    )
    raise SystemExit(1)
marker = Path("runtime/health") / f"us-prep-missing-{trade_date}.json"
if marker.exists():
    marker.unlink()
    print(f"[US_PREP][CLEAR_STALE_EXIT_ONLY] trade_date={trade_date} marker={marker}")
raise SystemExit(0)
PY
  cleanup_rc=$?
  set -e
  if [[ "$cleanup_rc" != 0 ]]; then
    echo "[US_PREP][POST_COMPLETE_CONTRACT_CHECK] status=NOT_EFFECTIVE marker_preserved=1" >> "$LOG_FILE"
  fi
fi
exit "$prep_rc"
