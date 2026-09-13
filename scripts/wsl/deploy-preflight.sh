#!/usr/bin/env bash
# Shared production deployment contract. Source this after changing to APP_DIR.
# The first real entry into each market/day invokes the locked synchronizer.
# Existing scheduler entries therefore remain unchanged; later sessions only
# verify the pin and never fetch/reset the source tree.

_deploy_sync_market_code() {
  bash scripts/wsl/sync-market-code.sh "$@"
}

deploy_preflight() {
  local expected actual branch head origin_head status dirty_code dirty_generated severity result=OK reason=none
  local log_dir log_file wrapper market session trade_date pin_dir pin_file pinned_sha diff_status pre_sync_sha post_sync_sha
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
  pin_dir="$actual/runtime/code-pins"
  pin_file="$pin_dir/${market^^}-${trade_date}.sha"
  mkdir -p "$pin_dir" || { result=FAIL; reason=pin_dir_unwritable; _preflight_log; return 1; }
  # PREP is normally the first entry, but recovery or AM can safely bootstrap
  # a missed cycle too. PREFLIGHT_ONLY is intentionally read-only for schedule
  # verification and CI; every real wrapper invocation takes this path.
  status="$(git status --porcelain 2>/dev/null || true)"
  dirty_generated="$(printf '%s\n' "$status" | awk '$2 ~ /^(runtime|logs|reports|\.pytest_cache|__pycache__)/ {print $2}')"
  dirty_code="$(printf '%s\n' "$status" | awk '$2 !~ /^(runtime|logs|reports|\.pytest_cache|__pycache__)/ && NF {print $2}')"
  [[ -z "$dirty_generated" ]] || echo "[DEPLOY][DIRTY_GENERATED][WARN] files=$(tr '\n' ',' <<<"$dirty_generated")"
  if [[ ! -s "$pin_file" && "${NULLIM_PREFLIGHT_ONLY:-0}" != "1" ]]; then
    if [[ -n "$dirty_code" ]]; then
      diff_status="$(git diff --name-status 2>/dev/null | tr '\n' ',' || true)"
      if [[ "${ALLOW_DIRTY_TRADING_CODE:-0}" == "1" ]]; then
        echo "[DEPLOY][DIRTY_CODE][WARN] market=${market^^} session=$session trade_date=$trade_date branch=$branch HEAD=$head origin_dual_agent=$origin_head files=$(tr '\n' ',' <<<"$dirty_code") diff_name_status=${diff_status:-none} action=EXPLICIT_EMERGENCY_OVERRIDE"
        export SYNC_MARKET_PRESERVE_DIRTY=1
      else
        result=FAIL; reason=dirty_trading_code
        echo "[DEPLOY][DIRTY_CODE][FAIL] market=${market^^} session=$session trade_date=$trade_date branch=$branch HEAD=$head pinned=${pinned_sha:-unknown} files=$(tr '\n' ',' <<<"$dirty_code") diff_name_status=${diff_status:-none} action=BLOCK_TRADING required=clean_worktree_or_ALLOW_DIRTY_TRADING_CODE_1"
        _preflight_log
        return 1
      fi
    fi
    pre_sync_sha="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "[DEPLOY][SYNC][AUTO] market=${market^^} session=$session trade_date=$trade_date source=existing_scheduler_chain"
    set +e
    _deploy_sync_market_code "${market^^}" "$trade_date"
    sync_rc=$?
    set -e
    case "$sync_rc" in
      0) ;;
      75)
        echo "[DEPLOY][SYNC][WARN] reason=code_sync_deferred action=CONTINUE_CURRENT_VERIFIED_CODE"
        git rev-parse HEAD > "$pin_file" || { result=FAIL; reason=pin_write_failed; _preflight_log; return 1; }
        ;;
      *)
        result=FAIL; reason=code_sync_failed
        echo "[DEPLOY][SYNC][FAIL] rc=$sync_rc"
        _preflight_log
        return "$sync_rc"
        ;;
    esac
    # reset --hard may have advanced the checkout; refresh every git fact.
    branch="$(git branch --show-current 2>/dev/null || echo detached)"
    head="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    origin_head="$(git rev-parse --short origin/dual-agent 2>/dev/null || echo unavailable)"
    post_sync_sha="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    if [[ "$pre_sync_sha" != "$post_sync_sha" && "${NULLIM_CODE_SYNC_REEXEC:-0}" != "1" && -f "$wrapper" ]]; then
      echo "[DEPLOY][SYNC][REEXEC] wrapper=$wrapper from=$pre_sync_sha to=$post_sync_sha"
      export NULLIM_CODE_SYNC_REEXEC=1
      exec bash "$wrapper"
    fi
  fi
  if [[ "$branch" != "dual-agent" && "${ALLOW_STALE_CODE:-0}" != "1" ]]; then
    result=FAIL; reason=branch; echo "[DEPLOY][STALE_CODE][FAIL] reason=branch branch=${branch} expected=dual-agent"; _preflight_log; return 1
  fi
  if [[ -s "$pin_file" ]]; then
    pinned_sha="$(tr -d '[:space:]' < "$pin_file")"
    if [[ "$pinned_sha" != "$(git rev-parse HEAD)" ]]; then
      result=FAIL; reason=pinned_sha_mismatch
      echo "[SESSION][CODE_VERSION][FAIL] market=${market^^} trade_date=$trade_date branch=dual-agent commit=$head pinned=$pinned_sha reason=PINNED_SHA_MISMATCH"
      _preflight_log; return 1
    fi
  elif [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then
    pinned_sha="$(git rev-parse HEAD)"
    echo "[SESSION][CODE_VERSION][CHECK_ONLY] market=${market^^} trade_date=$trade_date branch=dual-agent commit=$pinned_sha"
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
  if [[ -n "$dirty_code" ]]; then
    diff_status="$(git diff --name-status 2>/dev/null | tr '\n' ',' || true)"
    if [[ "${ALLOW_DIRTY_TRADING_CODE:-0}" == "1" ]]; then
      echo "[DEPLOY][DIRTY_CODE][WARN] market=${market^^} session=$session trade_date=$trade_date branch=$branch HEAD=$head origin_dual_agent=$origin_head files=$(tr '\n' ',' <<<"$dirty_code") diff_name_status=${diff_status:-none} action=EXPLICIT_EMERGENCY_OVERRIDE"
    else
      result=FAIL; reason=dirty_trading_code
      echo "[DEPLOY][DIRTY_CODE][FAIL] market=${market^^} session=$session trade_date=$trade_date branch=$branch HEAD=$head pinned=${pinned_sha:-unknown} files=$(tr '\n' ',' <<<"$dirty_code") diff_name_status=${diff_status:-none} action=BLOCK_TRADING required=clean_worktree_or_ALLOW_DIRTY_TRADING_CODE_1"
      _preflight_log
      return 1
    fi
  fi
  echo "[DEPLOY][OK]"; _preflight_log
}
