#!/usr/bin/env bash
set -euo pipefail

LOG="${1:-prep.log}"
VERIFY_LOG="${2:-/tmp/prep_verify.out}"

python scripts/verify_prep_log.py "$LOG" | tee "$VERIFY_LOG"
