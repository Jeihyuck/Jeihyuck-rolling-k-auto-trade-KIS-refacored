#!/usr/bin/env bash
# Shared production deployment contract. Source this after changing to APP_DIR.
# It deliberately does not fetch: code synchronization is an explicit, locked
# deployment operation (sync-market-code.sh), never a trading-job side effect.

deploy_preflight() {
  local expected actual branch head origin_head status dirty_code dirty_generated severity result=OK reason=none
  local log_dir log_file wrapper market session trade_date pin_dir pin_file pinned_sha diff_status
  expected="${NULLIM_RESOLVED_REPO_ROOT:-}"
  actual="$(pwd -P)"
  wrapper="${NULLIM_WRAPPER:-${BASH_SOURCE[1]:-unknown}}"
  market="${WSL_RUN_MARKET:-${MARKET:-unknown}}"
  session="${WSL_RUN_SESSION:-${PB1_SESSION:-unknown}}"
  trade_date="${NULLIM_TRADE_DATE:-${US_TRADE_DATE:-${KR_TRADE_DATE:-$(date +%F)}}}"
  log_dir="${expected:-$actual}/runtime/logs"
  mkdir -p "$log_dir" 2>/dev/null || true
  log_file="$log_dir/deploy-preflight.log"
  branch="$(git branch --show-current 2>/dev/null || echo detached)"
  head="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  origin_head="$(git rev-parse --short origin/dual-agent 2>/dev/null || echo unavailable)"
  _preflight_log() { printf '%s wrapper=%s market=%s session=%s trade_date=%s inherited_NULLIM_APP_DIR=%s resolved_repo_root=%s actual_pwd=%s branch=%s HEAD=%s origin_dual_agent=%s result=%s reason=%s\n' "$(date -Is)" "$wrapper" "$market" "$session" "$trade_date" "${NULLIM_INHERITED_APP_DIR:-}" "$expected" "$actual" "$branch" "$head" "$origin_head" "$result" "$reason" >> "$log_file"; }
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
  pin_dir="$actual/runtime/code-pins"
  pin_file="$pin_dir/${market^^}-${trade_date}.sha"
  mkdir -p "$pin_dir" || { result=FAIL; reason=pin_dir_unwritable; _preflight_log; return 1; }
  if [[ -s "$pin_file" ]]; then
    pinned_sha="$(tr -d '[:space:]' < "$pin_file")"
    if [[ "$pinned_sha" != "$(git rev-parse HEAD)" ]]; then
      result=FAIL; reason=pinned_sha_mismatch
      echo "[SESSION][CODE_VERSION][FAIL] market=${market^^} trade_date=$trade_date branch=dual-agent commit=$head pinned=$pinned_sha reason=PINNED_SHA_MISMATCH"
      _preflight_log; return 1
    fi
  else
    git rev-parse HEAD > "$pin_file" || { result=FAIL; reason=pin_write_failed; _preflight_log; return 1; }
    pinned_sha="$(git rev-parse HEAD)"
  fi
  export "${market^^}_CODE_SHA=$pinned_sha"
  echo "[SESSION][CODE_VERSION] market=${market^^} trade_date=$trade_date branch=dual-agent commit=$pinned_sha"
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
    diff_status="$(git diff --name-status 2>/dev/null | tr '\n' ',' || true)"
    echo "[DEPLOY][DIRTY_CODE][WARN] market=${market^^} session=$session trade_date=$trade_date branch=$branch HEAD=$head origin_dual_agent=$origin_head files=$(tr '\n' ',' <<<"$dirty_code") diff_name_status=${diff_status:-none} action=NON_BLOCKING"
  fi
  echo "[DEPLOY][OK]"; _preflight_log
}
