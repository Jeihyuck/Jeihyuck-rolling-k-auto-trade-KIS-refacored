#!/usr/bin/env bash
# Shared production deployment contract.  Source this after changing to APP_DIR.
# It deliberately does not fetch: a trading job must never change code at runtime.

NULLIM_CANONICAL_APP_DIR="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
deploy_preflight() {
  local expected="${NULLIM_APP_DIR:-$NULLIM_CANONICAL_APP_DIR}" actual branch head origin_head status dirty_code dirty_generated severity
  actual="$(pwd -P)"
  echo "[DEPLOY][APP_DIR] expected=${expected} actual=${actual}"
  if [[ "$actual" != "$expected" ]]; then
    severity=FAIL; [[ "${ALLOW_NON_CANONICAL_PATH:-0}" == "1" ]] && severity=WARN
    echo "[DEPLOY][PATH_MISMATCH][${severity}] expected=${expected} actual=${actual}"
    [[ "${ALLOW_NON_CANONICAL_PATH:-0}" == "1" ]] || return 1
  fi
  branch="$(git branch --show-current 2>/dev/null || echo detached)"
  head="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  origin_head="$(git rev-parse --short origin/dual-agent 2>/dev/null || echo unavailable)"
  echo "[DEPLOY][GIT] branch=${branch} head=${head} origin_dual_agent=${origin_head}"
  if [[ "$branch" != "dual-agent" && "${ALLOW_STALE_CODE:-0}" != "1" ]]; then
    echo "[DEPLOY][STALE_CODE][FAIL] reason=branch branch=${branch} expected=dual-agent"
    return 1
  fi
  if [[ "$origin_head" != unavailable ]] && ! git merge-base --is-ancestor HEAD origin/dual-agent; then
    severity=FAIL; [[ "${ALLOW_STALE_CODE:-0}" == "1" ]] && severity=WARN
    echo "[DEPLOY][STALE_CODE][${severity}] head=${head} origin_dual_agent=${origin_head}"
    [[ "${ALLOW_STALE_CODE:-0}" == "1" ]] || return 1
  fi
  status="$(git status --porcelain 2>/dev/null || true)"
  dirty_generated="$(printf '%s\n' "$status" | awk '$2 ~ /^(runtime|logs|reports|\.pytest_cache|__pycache__)/ {print $2}')"
  dirty_code="$(printf '%s\n' "$status" | awk '$2 !~ /^(runtime|logs|reports|\.pytest_cache|__pycache__)/ && NF {print $2}')"
  [[ -z "$dirty_generated" ]] || echo "[DEPLOY][DIRTY_GENERATED][WARN] files=$(tr '\n' ',' <<<"$dirty_generated")"
  if [[ -n "$dirty_code" ]]; then
    severity=FAIL; [[ "${ALLOW_DIRTY_CODE:-0}" == "1" ]] && severity=WARN
    echo "[DEPLOY][DIRTY_CODE][${severity}] files=$(tr '\n' ',' <<<"$dirty_code")"
    [[ "${ALLOW_DIRTY_CODE:-0}" == "1" ]] || return 1
  fi
  echo "[DEPLOY][OK]"
}
