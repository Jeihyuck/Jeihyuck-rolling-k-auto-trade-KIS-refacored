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
source "$SCRIPT_DIR/resolve-nullim-python.sh"
PYTHON_BIN="$(nullim_resolve_python "$APP_DIR")"
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
  trade_date="$("$PYTHON_BIN" - <<'PYDATE'
from datetime import datetime
from zoneinfo import ZoneInfo
import os
print(datetime.fromisoformat(os.environ["US_FORCE_NOW"]).astimezone(ZoneInfo("America/New_York")).date().isoformat())
PYDATE
)"
else
  trade_date="$(TZ=America/New_York date +%F)"
fi
marker="runtime/health/us-prep-missing-${trade_date}.json"
check() {
"$PYTHON_BIN" - "$trade_date" <<'PY'
import sys
trade_date=sys.argv[1]
from trader.us.prep_effective import check_effective_prep_sources
try:
    result = check_effective_prep_sources(trade_date)
    print('[US_PREP_PREFLIGHT][SOURCE_CHECK]', result)
    raise SystemExit(0 if result['ok'] else 1)
except Exception as exc:
    print('[US_PREP_PREFLIGHT][SOURCE_CHECK_ERROR] type=%s' % type(exc).__name__)
    raise SystemExit(1)
PY
}
if check; then rm -f "$marker"; exit 0; fi
bash scripts/wsl/run-us-prep-recovery.sh || true
if check; then rm -f "$marker"; exit 0; fi
marker_tmp="$(mktemp "${marker}.tmp.XXXXXX")"
cat > "$marker_tmp" <<JSON
{
  "trade_date": "${trade_date}",
  "status": "EXIT_ONLY",
  "reason": "prep_missing_after_preflight_recovery",
  "entry_can_proceed": 0,
  "exit_can_proceed": 1,
  "close_can_proceed": 1
}
JSON
mv -f "$marker_tmp" "$marker"
echo "[US_PREP_PREFLIGHT][EXIT_ONLY] trade_date=${trade_date}"
exit 0
