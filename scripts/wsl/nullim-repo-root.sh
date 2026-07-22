#!/usr/bin/env bash
# Resolve the repository from the canonical wrapper itself, never from login env.
nullim_resolve_repo_root() {
  local caller="${1:-${BASH_SOURCE[1]}}" caller_path caller_dir repo_root inherited inherited_real
  caller_path="$(readlink -f "$caller")" || { echo "[NULLIM_PATH][FAIL] reason=CALLER_UNRESOLVED caller=$caller" >&2; return 1; }
  caller_dir="$(cd "$(dirname "$caller_path")" && pwd -P)"
  repo_root="$(cd "$caller_dir/../.." && pwd -P)"
  inherited="${NULLIM_APP_DIR:-}"
  export NULLIM_INHERITED_APP_DIR="$inherited"
  if [[ -n "$inherited" ]]; then
    inherited_real="$(readlink -f "$inherited" 2>/dev/null || printf '%s' "$inherited")"
    if [[ "$inherited_real" != "$repo_root" ]]; then
      printf '%s\n' "[NULLIM_PATH][STALE_ENV_IGNORED] inherited=$inherited resolved=$repo_root" >&2
    fi
  fi
  if [[ ! -f "$repo_root/scripts/wsl/deploy-preflight.sh" ]]; then
    printf '%s\n' "[NULLIM_PATH][FAIL] reason=INVALID_REPO_ROOT resolved=$repo_root" >&2
    return 1
  fi
  if [[ ! -d "$repo_root/trader" ]]; then
    printf '%s\n' "[NULLIM_PATH][FAIL] reason=MISSING_TRADER_DIR resolved=$repo_root" >&2
    return 1
  fi
  export NULLIM_APP_DIR="$repo_root"
  export NULLIM_RESOLVED_REPO_ROOT="$repo_root"
}
