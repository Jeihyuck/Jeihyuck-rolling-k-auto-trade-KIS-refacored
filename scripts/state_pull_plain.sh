#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="bot_state"
JSON_PATH="${STATE_DIR}/state.json"
REMOTE_PATH="${STATE_DIR}/state.json"
DEFAULT_STATE='{"version": 1, "lots": [], "updated_at": null}'

mkdir -p "${STATE_DIR}"

if git ls-remote --exit-code --heads origin bot-state >/dev/null 2>&1; then
  git fetch --no-tags origin bot-state:refs/remotes/origin/bot-state >/dev/null 2>&1 || true
  if git cat-file -e "origin/bot-state:${REMOTE_PATH}" 2>/dev/null; then
    git show "origin/bot-state:${REMOTE_PATH}" > "${JSON_PATH}"
    echo "[STATE] Pulled ${REMOTE_PATH} from bot-state branch."
  else
    echo "[STATE] WARN: state.json not found in bot-state branch. Initializing."
    echo "${DEFAULT_STATE}" > "${JSON_PATH}"
  fi
else
  echo "[STATE] WARN: bot-state branch not found. Initializing."
  echo "${DEFAULT_STATE}" > "${JSON_PATH}"
fi
