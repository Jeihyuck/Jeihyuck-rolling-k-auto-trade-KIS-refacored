#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

log() {
  echo "[PREP-FLOW-CHECK] $*"
}

die() {
  echo "[PREP-FLOW-CHECK][FAIL] $*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage:
  scripts/check_prep_flow_health.sh [LOG_FILE]

Behavior:
  - 수급 누락/실패 WARNING([FLOW][WARN])은 허용 (중단 사유 아님)
  - PREP_DEGRADED(metrics_mostly_zero, empty_final30) 발견 시 실패
  - PREP 완료 로그([PREP][DONE] 또는 PREP_DONE ledger event) 없으면 실패

Examples:
  scripts/check_prep_flow_health.sh /tmp/prep.log
  scripts/check_prep_flow_health.sh runtime/logs/prep.log
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

LOG_FILE="${1:-/tmp/prep.log}"
[[ -f "$LOG_FILE" ]] || die "log file not found: $LOG_FILE"

FLOW_WARN_COUNT="$(grep -Eci '\[FLOW\]\[WARN\]' "$LOG_FILE" || true)"
MISSING_FLOW_COUNT="$(grep -Eci 'missing_flow_rows=|flow missing -> non_blocking|missing flow symbol=' "$LOG_FILE" || true)"
DEGRADED_METRICS_COUNT="$(grep -Eci '\[PREP\]\[DEGRADED\].*metrics_mostly_zero' "$LOG_FILE" || true)"
DEGRADED_EMPTY_COUNT="$(grep -Eci '\[PREP\]\[DEGRADED\].*empty_final30' "$LOG_FILE" || true)"
PREP_DONE_COUNT="$(grep -Eci '\[PREP\]\[DONE\]|\[LEDGER_EVENT\].*event_type=PREP_DONE' "$LOG_FILE" || true)"

log "log_file=$LOG_FILE"
log "flow_warn_count=$FLOW_WARN_COUNT missing_flow_count=$MISSING_FLOW_COUNT"
log "degraded_metrics_count=$DEGRADED_METRICS_COUNT degraded_empty_count=$DEGRADED_EMPTY_COUNT"
log "prep_done_count=$PREP_DONE_COUNT"

if [[ "$DEGRADED_METRICS_COUNT" -gt 0 ]]; then
  log "Detected degraded reason=metrics_mostly_zero"
  grep -Ein '\[PREP\]\[DEGRADED\].*metrics_mostly_zero' "$LOG_FILE" | tail -n 5 || true
  die "PREP degraded by metrics_mostly_zero"
fi

if [[ "$DEGRADED_EMPTY_COUNT" -gt 0 ]]; then
  log "Detected degraded reason=empty_final30"
  grep -Ein '\[PREP\]\[DEGRADED\].*empty_final30' "$LOG_FILE" | tail -n 5 || true
  die "PREP degraded by empty_final30"
fi

if [[ "$PREP_DONE_COUNT" -eq 0 ]]; then
  die "PREP completion not found ([PREP][DONE] or PREP_DONE event)"
fi

if [[ "$FLOW_WARN_COUNT" -gt 0 ]]; then
  log "FLOW warnings found (non-blocking by policy):"
  grep -Ein '\[FLOW\]\[WARN\]' "$LOG_FILE" | tail -n 10 || true
fi

log "OK: policy check passed (flow warning is non-blocking, prep completed)"
