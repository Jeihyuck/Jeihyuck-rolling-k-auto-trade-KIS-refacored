#!/usr/bin/env bash
# US practice-only GO evidence collection. NEVER starts a worker, submits
# orders, rewrites portfolio epochs, releases claims or modifies a DB.
# This deliberately cannot return an order-capable GO on read-only inputs.
# Usage:
#   bash scripts/wsl/us-go-readonly-acceptance.sh 2026-11-05 \
#       runtime/logs/us-stage4-worker-2026-11-05-am-2026-11-05-local.log
set -euo pipefail
set +x

if [[ "$#" -lt 1 || ! "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo 'US_GO_READONLY_INVALID_TRADE_DATE' >&2
    exit 42
fi
trade_date="$1"
shift
repo="$(git rev-parse --show-toplevel 2>/dev/null)" || {
    echo 'US_GO_REPO_NOT_VERIFIED' >&2
    exit 42
}
cd "$repo"

# Prefer the operator's actual isolated Python env. No pip installs and
# no scheduler or Git checkout modification during acceptance.
python_bin="${US_GO_PYTHON:-$repo/.venv/bin/python}"
if [[ ! -x "$python_bin" ]]; then
    echo 'US_GO_PYTHON_NOT_AVAILABLE' >&2
    exit 42
fi
branch="$(git branch --show-current)"
head="$(git rev-parse HEAD)"
dirty="$(git status --porcelain --untracked-files=normal -- trader scripts migrations)"
if [[ -n "$dirty" ]]; then
    echo 'US_GO_CODE_DIRTY_NO_APPROVAL' >&2
    exit 42
fi
echo "US_GO_CODE_SHA=$head"
if [[ "$branch" != "dual-agent" ]]; then
    echo "US_GO_CODE_PIN_REVIEW_REQUIRED branch=non-operational-checkout"
fi

# Enforce readonly even when .env still contains armed practice flags.
# Both native order and generic broker write/cancel paths must remain OFF.
export KIS_ENV=practice
export WSL_RUN_MARKET=US
export NO_TRADE=1 DRY_RUN=1 DISABLE_REAL_TRADING=1 US_ORDER_ARMED=0
export LIVE_TRADING_ENABLED=0 US_LIVE_TRADING_ENABLED=0

migration_status=NOT_PROVEN
if "$python_bin" -m scripts.us0062_readonly_preflight --confirm-practice-readonly; then
    migration_status=READONLY_SCHEMA_PASS
fi
original_status=NOT_PROVEN
if "$python_bin" -m scripts.us_legacy_nine_originals_readonly_certify \
    --trade-date "$trade_date" --confirm-practice-readonly; then
    original_status=READONLY_ORIGINALS_PASS
fi
stage4_status=NOT_PROVEN
if [[ "$#" -gt 0 ]]; then
    # Logs are independent evidence, not proof of KIS POST/ACK/fill.
    if "$python_bin" scripts/us_stage4_latency_acceptance.py \
        --stage4-only "$@"; then
        stage4_status=READONLY_STAGE4_INTERVAL_PASS
    fi
fi

echo "US_GO_MIGRATION=$migration_status"
echo "US_GO_ORIGINALS=$original_status"
echo "US_GO_STAGE4_LOG=$stage4_status"
echo "US_GO_ORDER_CAPABLE_STATUS=NO_GO"
echo "US_GO_REMAINING=LEGACY_SCOPE_POLICY_APPROVAL,KIS_ORIGINAL_ACCOUNT_CERTIFICATION,TMO_CLAIM_TERMINALITY,WSL_SHA_PIN,CONTROLLED_PRACTICE_POST_ACK_FILL,CLOSE"
# No source log, schema audit, or GET-only run can authorize broker POST.
exit 44
