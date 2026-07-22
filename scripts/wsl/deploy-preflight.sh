#!/usr/bin/env bash
# Shared production deployment contract. Source this after changing to APP_DIR.
# It deliberately does not fetch: a trading job must never change code at runtime.

deploy_preflight() {
  local expected actual branch head origin_head status dirty_code dirty_generated severity result=OK reason=none
  local log_dir log_file wrapper market session
  expected="${NULLIM_RESOLVED_REPO_ROOT:-}"
  actual="$(pwd -P)"
  wrapper="${NULLIM_WRAPPER:-${BASH_SOURCE[1]:-unknown}}"
  market="${WSL_RUN_MARKET:-${MARKET:-unknown}}"
  session="${WSL_RUN_SESSION:-${PB1_SESSION:-unknown}}"
  log_dir="${expected:-$actual}/runtime/logs"
  mkdir -p "$log_dir" 2>/dev/null || true
  log_file="$log_dir/deploy-preflight.log"
  branch="$(git branch --show-current 2>/dev/null || echo detached)"
  head="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  origin_head="$(git rev-parse --short origin/dual-agent 2>/dev/null || echo unavailable)"
  _preflight_log() { printf '%s wrapper=%s market=%s session=%s inherited_NULLIM_APP_DIR=%s resolved_repo_root=%s actual_pwd=%s branch=%s HEAD=%s result=%s reason=%s\n' "$(date -Is)" "$wrapper" "$market" "$session" "${NULLIM_INHERITED_APP_DIR:-}" "$expected" "$actual" "$branch" "$head" "$result" "$reason" >> "$log_file"; }
  if [[ -z "$expected" ]]; then
    result=FAIL; reason=RESOLVED_REPO_ROOT_MISSING; echo "[DEPLOY][PATH][FAIL] reason=$reason"; _preflight_log; return 1
  fi
  echo "[DEPLOY][APP_DIR] expected=${expected} actual=${actual}"
  if [[ "$actual" != "$expected" ]]; then
    result=FAIL; reason=PATH_MISMATCH; echo "[DEPLOY][PATH_MISMATCH][FAIL] expected=${expected} actual=${actual}"; _preflight_log; return 1
  fi
  echo "[DEPLOY][GIT] branch=${branch} head=${head} origin_dual_agent=${origin_head}"
  if [[ "$branch" != "dual-agent" && "${ALLOW_STALE_CODE:-0}" != "1" ]]; then
    result=FAIL; reason=branch; echo "[DEPLOY][STALE_CODE][FAIL] reason=branch branch=${branch} expected=dual-agent"; _preflight_log; return 1
  fi
  if [[ "$origin_head" != unavailable ]] && ! git merge-base --is-ancestor HEAD origin/dual-agent; then
    severity=FAIL; [[ "${ALLOW_STALE_CODE:-0}" == "1" ]] && severity=WARN
    echo "[DEPLOY][STALE_CODE][${severity}] head=${head} origin_dual_agent=${origin_head}"
    if [[ "$severity" == FAIL ]]; then result=FAIL; reason=origin_ancestry; _preflight_log; return 1; fi
  fi
  status="$(git status --porcelain 2>/dev/null || true)"
  dirty_generated="$(printf '%s\n' "$status" | awk '$2 ~ /^(runtime|logs|reports|\.pytest_cache|__pycache__)/ {print $2}')"
  dirty_code="$(printf '%s\n' "$status" | awk '$2 !~ /^(runtime|logs|reports|\.pytest_cache|__pycache__)/ && NF {print $2}')"
  [[ -z "$dirty_generated" ]] || echo "[DEPLOY][DIRTY_GENERATED][WARN] files=$(tr '\n' ',' <<<"$dirty_generated")"
  if [[ -n "$dirty_code" ]]; then
    severity=FAIL; [[ "${ALLOW_DIRTY_CODE:-0}" == "1" ]] && severity=WARN
    echo "[DEPLOY][DIRTY_CODE][${severity}] files=$(tr '\n' ',' <<<"$dirty_code")"
    if [[ "$severity" == FAIL ]]; then result=FAIL; reason=dirty_code; _preflight_log; return 1; fi
  fi
  echo "[DEPLOY][OK]"; _preflight_log
}
