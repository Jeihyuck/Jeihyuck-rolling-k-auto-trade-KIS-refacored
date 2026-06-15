#!/usr/bin/env bash
set -euo pipefail
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"; mkdir -p runtime
if [[ -f .env ]]; then set -a; source .env; set +a; fi
if [[ -f .venv/bin/activate ]]; then source .venv/bin/activate; fi
export MARKET=KR REGION=KR TRADING_REGION=KR EXCHANGE=KRX PB1_MARKET_SCOPE=KRX
export STRATEGY_ENV="${STRATEGY_ENV:-practice}" KIS_ENV="${KIS_ENV:-practice}" TZ=Asia/Seoul PYTHONUNBUFFERED=1
export WSL_RUN_SOURCE="local-wsl" WSL_RUN_MARKET="KR"
export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}" DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}" DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"

export PB1_SESSION=am WSL_RUN_SESSION=am STRATEGY_MODE=LIVE DRY_RUN=0 DISABLE_LIVE_TRADING=0 LIVE_TRADING_ENABLED=1 KR_LIVE_TRADING_ENABLED=1 KR_ORDER_ARMED=1
LOG=runtime/wsl-kr-am.log
{
  echo "[KR_AM][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV dry_run=$DRY_RUN live_enabled=$LIVE_TRADING_ENABLED kr_order_armed=$KR_ORDER_ARMED"
  set +e
  python - <<'PYGUARD'
from pathlib import Path
from datetime import datetime
import json, sys
try:
    from zoneinfo import ZoneInfo; now=datetime.now(ZoneInfo('Asia/Seoul'))
except Exception: now=datetime.now()
if 'am' == 'am' and now.hour < 9:
    print('[KR_AM][SKIP] reason=PREOPEN_NO_ORDER'); print('[RUN_SUMMARY][RESULT] status=SKIP reason=PREOPEN_NO_ORDER'); sys.exit(10)
cands=[Path('signals/kr/final30_scored.json'), Path('runtime/kr/watchlist')/now.strftime('%Y-%m-%d')/'final30_scored.json']
for p in cands:
    if p.exists():
        try:
            data=json.loads(p.read_text(encoding='utf-8')); rows=data if isinstance(data,list) else data.get('rows') or data.get('data') or []; n=len(rows) if isinstance(rows,list) else 0
        except Exception: n=0
        if n>0:
            print('[KR_AM][PREP_GUARD][OK] final30=%s source=%s' % (n,p)); sys.exit(0)
print('[KR_AM][SKIP] reason=KR_PREP_ARTIFACT_MISSING'); print('[RUN_SUMMARY][RESULT] status=SKIP reason=KR_PREP_ARTIFACT_MISSING'); sys.exit(11)
PYGUARD
  guard=$?
  set -e
  if [[ $guard -eq 10 || $guard -eq 11 ]]; then echo "[KR_AM][EXIT] ts=$(date -Is) exit_code=0 guard=$guard"; exit 0; fi
  echo "[KR_AM][GATE] order_allowed=1"
  python -m trader.trader --window am --phase auto
  rc=$?
  echo "[KR_AM][DONE] status=$([[ $rc -eq 0 ]] && echo OK || echo FAIL)"
  echo "[RUN_SUMMARY][RESULT] session=am status=$([[ $rc -eq 0 ]] && echo OK || echo FAIL)"
  echo "[KR_AM][EXIT] ts=$(date -Is) exit_code=$rc"
  exit $rc
} >> "$LOG" 2>&1
