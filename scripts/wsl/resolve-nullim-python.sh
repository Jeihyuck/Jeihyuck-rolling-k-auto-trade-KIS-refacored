#!/usr/bin/env bash

nullim_resolve_python() {
  local root="${1:?repo root required}"
  if [[ -x "$root/.venv/bin/python" ]]; then
    printf '%s\n' "$root/.venv/bin/python"
    return 0
  fi
  if [[ -n "${NULLIM_PYTHON_BIN:-}" && -x "${NULLIM_PYTHON_BIN}" ]]; then
    printf '%s\n' "$NULLIM_PYTHON_BIN"
    return 0
  fi
  echo "[PYTHON_RUNTIME][FAIL] reason=VENV_PYTHON_MISSING root=$root" >&2
  return 1
}
