#!/usr/bin/env bash
set -euo pipefail

APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$APP"

echo "[KR_LOCK_CLEANUP][START]"

echo "===== KR PROCESS ====="
KR_PROCESS_PATTERN="run-kr|kr-prep|kr-am|kr-afternoon|kr-close|pb1_runner|trader.kr|trader.pb1|python -m trader.*kr"
ps -ef | grep -E "$KR_PROCESS_PATTERN" | grep -v -E "grep|cleanup-stale-kr-locks|touch /tmp/nullim-kr|bash -c" || true

LIVE_KR="$(ps -ef | grep -E "$KR_PROCESS_PATTERN" | grep -v -E "grep|cleanup-stale-kr-locks|touch /tmp/nullim-kr|bash -c" || true)"

if [ -n "$LIVE_KR" ]; then
  echo "[KR_LOCK_CLEANUP][SKIP] live KR process exists"
  exit 0
fi

for f in \
  /tmp/nullim-kr-prep.lock \
  /tmp/nullim-kr-am.lock \
  /tmp/nullim-kr-afternoon.lock \
  /tmp/nullim-kr-close.lock \
  runtime/locks/kr-prep.lock \
  runtime/locks/kr-am.lock \
  runtime/locks/kr-afternoon.lock \
  runtime/locks/kr-close.lock
do
  if [ -e "$f" ]; then
    if fuser "$f" >/dev/null 2>&1; then
      echo "[KR_LOCK_CLEANUP][KEEP] lock has owner file=$f"
    else
      echo "[KR_LOCK_CLEANUP][REMOVE] stale lock file=$f"
      rm -f "$f"
    fi
  fi
done

echo "[KR_LOCK_CLEANUP][DONE]"
