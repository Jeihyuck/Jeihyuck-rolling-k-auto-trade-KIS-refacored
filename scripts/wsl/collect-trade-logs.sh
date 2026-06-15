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
{
  echo "timestamp=$(date -Is)"; uname -a; echo "pwd=$REPO"; git rev-parse --short HEAD 2>/dev/null || true
} > "$work/payload/system_context.txt"
while IFS= read -r -d '' f; do
  rel="${f#./}"; mkdir -p "$work/payload/$(dirname "$rel")"
  sed -E 's/(APP_KEY|APP_SECRET|KIS_APP_KEY|KIS_APP_SECRET|ACCESS_TOKEN|SLACK_WEBHOOK_URL|DATABASE_URL|DB_URL|CANO|ACNT_PRDT_CD)([=:][^[:space:]]+)/\1=***MASKED***/g; s/Bearer [A-Za-z0-9._~+\/-]+/Bearer ***MASKED***/g' "$f" > "$work/payload/$rel" || true
done < <(find runtime signals reports diagnostics scripts/wsl \( -path 'runtime/wsl-kr-*.log' -o -path 'runtime/wsl-us-*.log' -o -path 'signals/kr*' -o -path 'signals/us*' -o -path 'runtime/kr/watchlist*' -o -path 'runtime/us/watchlist*' -o -path 'reports/kr_prep*' -o -path 'reports/us_prep*' -o -path 'diagnostics*' -o -path 'scripts/wsl/run-kr*.sh' -o -path 'scripts/wsl/run-us*.sh' \) -type f -size -100M -print0 2>/dev/null)
echo "[COLLECT][MASK]"
out="$out_dir/trade-diagnostics-$ts.tar.gz"
tar -C "$work/payload" -czf "$out" .
rm -rf "$work"
echo "[COLLECT][DONE] path=$out"
