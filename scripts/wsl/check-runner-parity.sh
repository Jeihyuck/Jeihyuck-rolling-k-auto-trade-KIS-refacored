#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fail=0
check(){ if eval "$2"; then echo "[RUNNER_PARITY][OK] $1"; else echo "[RUNNER_PARITY][FAIL] reason=$1"; fail=1; fi; }
check "KR_AM_WAIT_UNTIL_TARGET" "rg -q 'WAIT_UNTIL_TARGET|kr_am_policy' '$ROOT/trader/kr/runner' '$ROOT/scripts/wsl/run-kr-am.sh'"
check "KR_CANONICAL_PREP_ARTIFACT" "rg -q 'signals/kr/latest_final30_scored|runtime/kr/watchlist' '$ROOT/trader/kr/runner'"
check "NO_PREOPEN_IMMEDIATE_SKIP" "! rg -q 'PREOPEN_NO_ORDER.*SKIP|status.*SKIP.*PREOPEN_NO_ORDER' '$ROOT/trader/kr/runner/trade_session_runner.py'"
check "KR_FALLBACK_REPAIR" "rg -q 'REPAIRED|mirror_kr_prep_artifacts|legacy' '$ROOT/trader/kr/runner'"
check "US_PERMISSION_RESOLVER" "rg -q 'resolve_us_order_permissions' '$ROOT/trader/us' '$ROOT/scripts/wsl/run-us-am.sh' '$ROOT/scripts/wsl/run-us-afternoon.sh'"
check "KR_TRADER_BLOCKED" "rg -q 'LEGACY_SCRIPT_NOT_ALLOWED_FOR_SCHEDULER' '$ROOT/scripts/wsl/run-kr-trader.sh'"
check "DB_TIMEOUT_ENVS" "rg -q 'DB_LOCK_TIMEOUT_MS.*DB_STATEMENT_TIMEOUT_MS.*DB_IDLE_IN_TX_SESSION_TIMEOUT_MS|DB_IDLE_IN_TX_SESSION_TIMEOUT_MS' '$ROOT/trader/db/engine.py' '$ROOT/scripts/wsl'"
check "ORDER_LOOKUP_FAIL_OPEN" "rg -q 'PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT' '$ROOT/trader' '$ROOT/scripts/wsl'"
exit $([[ $fail -eq 0 ]] && echo 0 || echo 2)
