#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi
mkdir -p runtime runtime/locks runtime/health

export US_PREP_RECOVERY_RUN="${US_PREP_RECOVERY_RUN:-1}"
export US_ALLOW_DEGRADED_IN_TRADE="${US_ALLOW_DEGRADED_IN_TRADE:-1}"
export US_WSL_RECOVERY_SOURCE="scheduler-pre-am-recovery"

exec bash scripts/wsl/run-us-prep.sh
