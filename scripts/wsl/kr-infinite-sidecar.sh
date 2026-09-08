#!/usr/bin/env bash
# Independent KR Infinite sidecar lifecycle.
#
# Source this file from the existing KR AM/afternoon/close wrappers.  The
# Infinite process has its own timeout and session loop and is never killed or
# skipped because PB1 returns a failure code.

nullim_start_kr_infinite_sidecar() {
  local session="${1:?session required}"
  local env="${2:-${KIS_ENV:-practice}}"
  local timeout_sec

  case "$session" in
    am) timeout_sec="${KR_INFINITE_AM_TIMEOUT_SEC:-15000}" ;;
    afternoon) timeout_sec="${KR_INFINITE_PM_TIMEOUT_SEC:-9000}" ;;
    close) timeout_sec="${KR_INFINITE_CLOSE_TIMEOUT_SEC:-900}" ;;
    *)
      echo "[KR_INF][SIDECAR][START_FAIL] unsupported_session=$session" >&2
      KR_INFINITE_SIDECAR_PID=""
      return 0
      ;;
  esac

  mkdir -p runtime/locks runtime/health
  echo "[KR_INF][SIDECAR][START] session=$session env=$env timeout_sec=$timeout_sec owner=KR_INFINITE_INDEPENDENT"

  set +e
  (
    exec timeout --kill-after=30s "$timeout_sec"       python -m trader.kr.infinite.session_runner       --session "$session"       --env "$env"
  ) &
  KR_INFINITE_SIDECAR_PID=$!
  set -e

  export KR_INFINITE_SIDECAR_PID
  echo "[KR_INF][SIDECAR][PID] session=$session pid=$KR_INFINITE_SIDECAR_PID"
  return 0
}


nullim_wait_kr_infinite_sidecar() {
  local pid="${KR_INFINITE_SIDECAR_PID:-}"
  if [[ -z "$pid" ]]; then
    echo "[KR_INF][SIDECAR][WAIT_SKIP] reason=no_pid"
    return 0
  fi

  local rc=0
  set +e
  wait "$pid"
  rc=$?
  set -e

  if [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
    echo "[KR_INF][SIDECAR][TIMEOUT] pid=$pid rc=$rc"
  elif [[ "$rc" -ne 0 ]]; then
    echo "[KR_INF][SIDECAR][FAIL] pid=$pid rc=$rc"
  else
    echo "[KR_INF][SIDECAR][DONE] pid=$pid rc=0"
  fi

  # PB1 and Infinite are siblings.  Infinite status is recorded separately and
  # must not rewrite the PB1 session exit code (and vice versa).
  return 0
}
