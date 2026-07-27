#!/usr/bin/env bash
set -euo pipefail
export NULLIM_RUN_PURPOSE="prep-recovery"

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/init-session-log.sh"
nullim_init_session_log US prep-recovery prep-recovery "${BASH_SOURCE[0]}"
set +e
nullim_require_trading_day us "$NULLIM_TRADE_DATE"
trading_day_rc=$?
set -e
[[ "$trading_day_rc" == 10 ]] && exit 0
[[ "$trading_day_rc" == 0 ]] || exit "$trading_day_rc"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
export WSL_RUN_MARKET="US"
export MARKET="US"
export WSL_RUN_SESSION="prep_recovery"
export PB1_SESSION="prep_recovery"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi
mkdir -p runtime runtime/locks runtime/health

export US_PREP_RECOVERY_RUN="${US_PREP_RECOVERY_RUN:-1}"
export US_ALLOW_DEGRADED_IN_TRADE="${US_ALLOW_DEGRADED_IN_TRADE:-1}"
export US_WSL_RECOVERY_SOURCE="scheduler-pre-am-recovery"

exec bash scripts/wsl/run-us-prep.sh
