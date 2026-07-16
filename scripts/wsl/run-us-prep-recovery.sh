#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime runtime/locks runtime/health

export US_PREP_RECOVERY_RUN="${US_PREP_RECOVERY_RUN:-1}"
export US_ALLOW_DEGRADED_IN_TRADE="${US_ALLOW_DEGRADED_IN_TRADE:-1}"
export US_WSL_RECOVERY_SOURCE="scheduler-pre-am-recovery"

exec bash scripts/wsl/run-us-prep.sh
