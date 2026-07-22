#!/usr/bin/env bash
# Manual compatibility dispatcher. Automatic scheduling must use Windows Task Scheduler.
set -euo pipefail
phase="${1:-}"
case "$phase" in prep) target=scripts/wsl/run-kr-prep.sh;; am) target=scripts/wsl/run-kr-am.sh;; pm) target=scripts/wsl/run-kr-afternoon.sh;; close) target=scripts/wsl/run-kr-close.sh;; *) echo "usage: $0 {prep|am|pm|close}" >&2; exit 64;; esac
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/scripts/wsl/nullim-repo-root.sh"
nullim_resolve_repo_root "$SCRIPT_DIR/scripts/wsl/nullim-repo-root.sh"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
export WSL_RUN_MARKET="KR"
export MARKET="KR"
export WSL_RUN_SESSION="$phase"
export PB1_SESSION="$phase"
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then
  NULLIM_WRAPPER="${BASH_SOURCE[0]}"
  source scripts/wsl/deploy-preflight.sh
  deploy_preflight
  exit 0
fi
echo "[MANUAL_DISPATCH][START] market=KR phase=$phase target=$target"
exec bash "$target"
