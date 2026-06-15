#!/usr/bin/env bash
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
failed=0
for s in run-kr-prep.sh run-kr-am.sh run-kr-afternoon.sh run-kr-close.sh; do
  if [[ -x "scripts/wsl/$s" ]]; then echo "[VERIFY][OK] $s exists executable=1"; else echo "[VERIFY][FAIL] $s exists/executable check failed"; failed=1; fi
done
if rg -n "PB1 KR .*run-kr-trader\.sh|run-kr-trader\.sh" scripts/windows 2>/dev/null | rg -v "verify-scheduler|LEGACY|DEPRECATED"; then
  echo "[VERIFY][FAIL] run-kr-trader.sh referenced by KR scheduler scripts"; failed=1
fi
exit $failed
