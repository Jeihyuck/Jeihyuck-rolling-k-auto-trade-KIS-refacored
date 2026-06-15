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

export PB1_SESSION=prep WSL_RUN_SESSION=prep STRATEGY_MODE=PREP DRY_RUN=1 DISABLE_LIVE_TRADING=1 LIVE_TRADING_ENABLED=0
LOG=runtime/wsl-kr-prep.log
{
  echo "[KR_PREP][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV dry_run=$DRY_RUN live_enabled=$LIVE_TRADING_ENABLED"
  python -m trader.prep_runner --env "$STRATEGY_ENV"
  rc=$?
  python - <<'PYVALID'
import json, sys
from pathlib import Path
final=Path('signals/kr/final30_scored.json')
if not final.exists():
    print('[KR_PREP][FAIL] reason=FINAL30_MISSING'); print('[RUN_SUMMARY][RESULT] status=FAIL reason=FINAL30_MISSING'); sys.exit(20)
data=json.loads(final.read_text(encoding='utf-8'))
rows=data if isinstance(data,list) else data.get('rows') or data.get('data') or []
count=len(rows) if isinstance(rows,list) else 0
print(f'[KR_PREP][ARTIFACT] path={final} rows={count}')
score_nonzero=sum(1 for r in rows if isinstance(r,dict) and float(r.get('score') or r.get('total_score') or 0)) if isinstance(rows,list) else 0
if count != 30:
    print(f'[KR_PREP][FAIL] reason=FINAL30_ROW_COUNT rows={count}'); print(f'[RUN_SUMMARY][RESULT] status=FAIL reason=FINAL30_ROW_COUNT rows={count}'); sys.exit(21)
if score_nonzero == 0: print('[KR_PREP][WARN] reason=FINAL30_SCORE_ALL_ZERO')
contract=Path('signals/kr/prep_contract.json'); contract_ok=1; trade_can=1
if contract.exists():
    c=json.loads(contract.read_text(encoding='utf-8'))
    contract_ok=int(bool(c.get('contract_ok', c.get('ok', False)))); trade_can=int(bool(c.get('trade_can_proceed', contract_ok)))
print(f'[KR_PREP][CONTRACT] contract_ok={contract_ok} trade_can_proceed={trade_can}')
if contract_ok != 1 or trade_can != 1:
    print('[KR_PREP][FAIL] reason=CONTRACT_NOT_OK'); print('[RUN_SUMMARY][RESULT] status=FAIL reason=CONTRACT_NOT_OK'); sys.exit(22)
print('[KR_PREP][DONE] status=OK'); print('[RUN_SUMMARY][RESULT] status=OK reason=KR_PREP_DONE')
PYVALID
  echo "[KR_PREP][EXIT] ts=$(date -Is) exit_code=$? runner_exit=$rc"
} >> "$LOG" 2>&1
