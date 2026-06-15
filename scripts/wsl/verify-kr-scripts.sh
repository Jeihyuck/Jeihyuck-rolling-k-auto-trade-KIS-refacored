#!/usr/bin/env bash
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
failed=0
for s in run-kr-prep.sh run-kr-am.sh run-kr-afternoon.sh run-kr-close.sh; do
  if [[ -x "scripts/wsl/$s" ]]; then
    echo "[VERIFY][OK] $s exists executable=1"
  else
    echo "[VERIFY][FAIL] $s exists/executable check failed"
    failed=1
  fi
done
session_scripts=(scripts/wsl/run-kr-prep.sh scripts/wsl/run-kr-am.sh scripts/wsl/run-kr-afternoon.sh scripts/wsl/run-kr-close.sh)
if rg -n "python -m trader\.trader" "${session_scripts[@]}" >/tmp/verify-kr-trader.$$ 2>/dev/null; then
  cat /tmp/verify-kr-trader.$$
  echo "[VERIFY][FAIL] KR session script still calls python -m trader.trader"
  failed=1
else
  echo "[VERIFY][OK] KR scripts do not call trader.trader"
fi
rm -f /tmp/verify-kr-trader.$$
if rg -n "localhost:8000|rebalance/run" "${session_scripts[@]}" trader/kr trader/pb1* trader/runner 2>/tmp/verify-kr-rgerr.$$ >/tmp/verify-kr-rebalance.$$; then
  cat /tmp/verify-kr-rebalance.$$
  echo "[VERIFY][FAIL] KR session path still references localhost rebalance API"
  failed=1
else
  echo "[VERIFY][OK] KR scripts do not call localhost rebalance API"
fi
rm -f /tmp/verify-kr-rebalance.$$ /tmp/verify-kr-rgerr.$$
if rg -n "portfolio_manager|KOSPI_CORE|KOSDAQ_ALPHA" trader/kr "${session_scripts[@]}" 2>/tmp/verify-kr-rgerr.$$ >/tmp/verify-kr-legacy.$$; then
  cat /tmp/verify-kr-legacy.$$
  echo "[VERIFY][FAIL] KR session path still references legacy portfolio manager path"
  failed=1
else
  echo "[VERIFY][OK] KR scripts do not reference legacy portfolio manager path"
fi
rm -f /tmp/verify-kr-legacy.$$ /tmp/verify-kr-rgerr.$$
if rg -n "PB1 KR .*run-kr-trader\.sh|run-kr-trader\.sh" scripts/windows 2>/dev/null | rg -v "verify-scheduler|LEGACY|DEPRECATED"; then
  echo "[VERIFY][FAIL] run-kr-trader.sh referenced by KR scheduler scripts"
  failed=1
fi
exit $failed
