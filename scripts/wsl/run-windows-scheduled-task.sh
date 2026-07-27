#!/usr/bin/env bash
set -euo pipefail
SCRIPT="${1:?script required}"; shift
APP="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../.." && pwd -P)"
DAY="$(TZ=Asia/Seoul date +%F)"; NAME="${NULLIM_SCHEDULER_TASK_NAME:-unknown}"
SAFE="$(printf %s "$NAME" | sed 's/[^A-Za-z0-9._-]/-/g')"; LOG="$APP/runtime/scheduler/windows/$DAY/$SAFE.log"
mkdir -p "$(dirname "$LOG")"; exec >>"$LOG" 2>&1
printf '[WINDOWS_TASK][START] task=%s started_at=%s script=%s args_count=%s\n' "$NAME" "$(date -Is)" "$SCRIPT" "$#"
set +e; bash "$SCRIPT" "$@"; rc=$?; set -e
printf '[WINDOWS_TASK][END] task=%s wsl_exit_code=%s ended_at=%s\n' "$NAME" "$rc" "$(date -Is)"
exit "$rc"
