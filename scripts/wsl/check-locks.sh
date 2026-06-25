#!/usr/bin/env bash
set -euo pipefail

echo "===== KR LOCK FILES ====="
for f in /tmp/nullim-kr-*.lock runtime/locks/kr*.lock; do
  if [ -e "$f" ]; then
    if fuser "$f" >/dev/null 2>&1; then
      echo "[LOCK][BUSY] $f"
    else
      echo "[LOCK][STALE_OR_FREE] $f"
    fi
  fi
done

echo "===== US LOCK FILES ====="
for f in /tmp/nullim-us-*.lock; do
  if [ -e "$f" ]; then
    if fuser "$f" >/dev/null 2>&1; then
      echo "[LOCK][BUSY] $f"
    else
      echo "[LOCK][STALE_OR_FREE] $f"
    fi
  fi
done
