#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/init-session-log.sh"
nullim_init_session_log US am-preflight "am-preflight" "${BASH_SOURCE[0]}"
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
export WSL_RUN_SESSION="am_preflight"
export PB1_SESSION="am_preflight"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi
mkdir -p runtime/health
if [[ -n "${US_TRADE_DATE:-}" ]]; then
  trade_date="${US_TRADE_DATE}"
elif [[ -n "${US_FORCE_NOW:-}" ]]; then
  export FORCE_NOW_INPUT="${US_FORCE_NOW}"
  trade_date="$(python - <<'PYDATE'
from datetime import datetime
from zoneinfo import ZoneInfo
import os
print(datetime.fromisoformat(os.environ["US_FORCE_NOW"]).astimezone(ZoneInfo("America/New_York")).date().isoformat())
PYDATE
)"
else
  trade_date="$(TZ=America/New_York date +%F)"
fi
check() {
python - "$trade_date" <<'PY'
import sys
trade_date=sys.argv[1]
try:
    # Canonical loader resolves runtime/us/watchlist/<date>/final30_scored.json.
    from trader.us.path_contract import load_us_final30_scored, load_us_prep_contract
    contract = load_us_prep_contract(trade_date) or {}
    rows = load_us_final30_scored(trade_date) or []
    status = str(contract.get('status') or '').upper()
    if status in {'OK', 'OK_WITH_WARNINGS'} and len(rows) >= 10:
        print('OK artifact final30_scored_count=%d prep_status=%s' % (len(rows), status))
        raise SystemExit(0)
except SystemExit:
    raise
except Exception as exc:
    print('ARTIFACT_CHECK_WARN %s' % exc)
try:
    from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
    prep=load_latest_us_prep_status(trade_date) or {}
    rows=load_locked_us_watchlist(trade_date=trade_date, min_count=10, allow_degraded=True) or []
    if prep.get('status') in {'OK','OK_WITH_WARNINGS'} and len(rows) >= 10:
        print('OK db locked_watchlist_count=%d prep_status=%s' % (len(rows), prep.get('status')))
        raise SystemExit(0)
except SystemExit:
    raise
except Exception as exc:
    print('DB_CHECK_WARN %s' % exc)
raise SystemExit(1)
PY
}
if check; then exit 0; fi
bash scripts/wsl/run-us-prep-recovery.sh || true
if check; then exit 0; fi
cat > "runtime/health/us-prep-missing-${trade_date}.json" <<JSON
{
  "trade_date": "${trade_date}",
  "status": "EXIT_ONLY",
  "reason": "prep_missing_after_preflight_recovery",
  "entry_can_proceed": 0,
  "exit_can_proceed": 1,
  "close_can_proceed": 1
}
JSON
echo "[US_PREP_PREFLIGHT][EXIT_ONLY] trade_date=${trade_date}"
exit 0
