#!/usr/bin/env bash
# Controlled once-per-market-cycle synchronization. Runtime artifacts are never cleaned.
set -euo pipefail
market="${1^^}"; trade_date="${2:-$(date +%F)}"
[[ "$market" == KR || "$market" == US ]] || { echo "usage: $0 KR|US [trade-date]" >&2; exit 2; }
root="$(cd "$(dirname "$(readlink -f "$0")")/../.." && pwd -P)"; cd "$root"
[[ -d .git && -f scripts/wsl/deploy-preflight.sh ]] || { echo "[DEPLOY][SYNC][FAIL] reason=INVALID_REPOSITORY"; exit 1; }
mkdir -p runtime/locks runtime/code-pins
exec 8>runtime/locks/deploy-global.lock
flock -n 8 || { echo "[DEPLOY][SYNC][FAIL] reason=DEPLOY_LOCK_HELD"; exit 1; }
for lock in runtime/locks/kr-*.lock runtime/locks/us-*.lock; do
  [[ -e "$lock" ]] || continue
  exec {fd}>"$lock"
  if ! flock -n "$fd"; then echo "[DEPLOY][SYNC][FAIL] reason=ACTIVE_TRADING_PROCESS lock=$lock"; exit 1; fi
done
pin="runtime/code-pins/${market}-${trade_date}.sha"
if [[ -s "$pin" ]]; then
  sha="$(tr -d '[:space:]' < "$pin")"
  echo "[DEPLOY][SYNC][SKIP] market=$market trade_date=$trade_date commit=$sha reason=SESSION_ALREADY_PINNED"
  exit 0
fi
git fetch origin dual-agent
git checkout dual-agent
git reset --hard origin/dual-agent
sha="$(git rev-parse HEAD)"; printf '%s\n' "$sha" > "$pin"
echo "[SESSION][CODE_VERSION] market=$market trade_date=$trade_date branch=dual-agent commit=$sha"
