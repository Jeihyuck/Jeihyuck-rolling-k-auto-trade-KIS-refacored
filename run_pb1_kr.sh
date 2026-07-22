#!/usr/bin/env bash
# Manual compatibility dispatcher. Automatic scheduling must use Windows Task Scheduler.
set -euo pipefail
phase="${1:-}"
case "$phase" in prep) target=scripts/wsl/run-kr-prep.sh;; am) target=scripts/wsl/run-kr-am.sh;; pm) target=scripts/wsl/run-kr-afternoon.sh;; close) target=scripts/wsl/run-kr-close.sh;; *) echo "usage: $0 {prep|am|pm|close}" >&2; exit 64;; esac
APP_DIR="${NULLIM_APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$APP_DIR"
echo "[MANUAL_DISPATCH][START] market=KR phase=$phase target=$target"
exec bash "$target"
