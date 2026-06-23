#!/usr/bin/env bash
set -euo pipefail
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"
echo "[COLLECT][START]"
ts="$(date +%Y%m%d-%H%M%S)"
out_dir="/mnt/c/Users/infin/Downloads"
[[ -d "$out_dir" ]] || out_dir="$REPO/runtime"
work="$(mktemp -d)"
mkdir -p "$work/payload"

mkdir -p runtime/diagnostics
{
echo "===== DATE ====="
date
echo

echo "===== PWD ====="
pwd
echo

echo "===== GIT BRANCH ====="
git branch --show-current || true
echo

echo "===== GIT LOG -1 ====="
git log -1 --oneline || true
echo

echo "===== GIT STATUS ====="
git status --short || true
echo

echo "===== SESSION GUARD CODE ====="
rg "RUNNING_LOCK|SKIP_DUPLICATE_RUNNING|US_SESSION_GUARD|US_FILE_GUARD" trader/us/utils trader/us/runner 2>/dev/null | head -300 || true
echo

echo "===== WSL LOCK CODE ====="
rg "flock|US_WSL_LOCK" scripts/wsl 2>/dev/null | head -300 || true
echo

echo "===== ADD_BUY / NEW_BUY / MAX POSITION CODE ====="
rg "ADD_TO_EXISTING_BUY|NEW_POSITION_BUY|position_action|US_MAX_POSITIONS|max_positions_reached|max_positions_reached_new_symbol|pyramid_allowed" trader/us scripts/wsl 2>/dev/null | head -500 || true
echo

echo "===== TOKEN CACHE CODE ====="
rg "token_cache|token_refresh|_TOKEN_CACHE|tokenP" trader/us/execution 2>/dev/null | head -300 || true
echo

echo "===== DIAGNOSTIC FILES ====="
find runtime/session_guard runtime/locks reports/us_daily reports/us_schedule_health -maxdepth 5 -type f -print 2>/dev/null || true
} > runtime/diagnostics/runtime_code_state.txt
{
  echo "timestamp=$(date -Is)"; uname -a; echo "pwd=$REPO"; git rev-parse --short HEAD 2>/dev/null || true
} > "$work/payload/system_context.txt"
while IFS= read -r -d '' f; do
  rel="${f#./}"; mkdir -p "$work/payload/$(dirname "$rel")"
  sed -E 's/(APP_KEY|APP_SECRET|KIS_APP_KEY|KIS_APP_SECRET|ACCESS_TOKEN|SLACK_WEBHOOK_URL|DATABASE_URL|DB_URL|CANO|ACNT_PRDT_CD)([=:][^[:space:]]+)/\1=***MASKED***/g; s/Bearer [A-Za-z0-9._~+\/-]+/Bearer ***MASKED***/g' "$f" > "$work/payload/$rel" || true
done < <(find runtime signals reports diagnostics scripts/wsl \( -path 'runtime/wsl-kr-*.log' -o -path 'runtime/wsl-us-*.log' -o -path 'runtime/diagnostics/runtime_code_state.txt' -o -path 'runtime/session_guard/*' -o -path 'runtime/locks/*' -o -path 'reports/us_daily/*' -o -path 'reports/us_schedule_health/*' -o -path 'signals/kr*' -o -path 'signals/us*' -o -path 'runtime/kr/watchlist*' -o -path 'runtime/us/watchlist*' -o -path 'reports/kr_prep*' -o -path 'reports/us_prep*' -o -path 'diagnostics*' -o -path 'scripts/wsl/run-kr*.sh' -o -path 'scripts/wsl/run-us*.sh' \) -type f -size -100M -print0 2>/dev/null)
echo "[COLLECT][MASK]"
out="$out_dir/trade-diagnostics-$ts.tar.gz"
tar -C "$work/payload" -czf "$out" .
rm -rf "$work"
echo "[COLLECT][DONE] path=$out"
