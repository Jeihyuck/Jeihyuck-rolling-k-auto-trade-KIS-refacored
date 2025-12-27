#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="bot_state"
JSON_PATH="${STATE_DIR}/state.json"
REMOTE_PATH="${STATE_DIR}/state.json"
DEFAULT_STATE='{"version": 1, "lots": [], "updated_at": null}'
POS_STATE_DIR="trader/state"
POS_JSON_PATH="${POS_STATE_DIR}/state.json"
POS_REMOTE_PATH="${POS_STATE_DIR}/state.json"
DEFAULT_POS_STATE='{"schema_version": 2, "updated_at": null, "positions": {}, "memory": {"last_price": {}, "last_seen": {}}}'
INTENT_LOG_PATH="${POS_STATE_DIR}/strategy_intents.jsonl"
INTENT_REMOTE_PATH="${INTENT_LOG_PATH}"
DEFAULT_INTENT_LOG=""
INTENT_CURSOR_PATH="${POS_STATE_DIR}/strategy_intents_state.json"
INTENT_CURSOR_REMOTE_PATH="${INTENT_CURSOR_PATH}"
DEFAULT_INTENT_CURSOR='{"offset": 0, "last_intent_id": null, "last_ts": null}'

mkdir -p "${STATE_DIR}"
mkdir -p "${POS_STATE_DIR}"

if git ls-remote --exit-code --heads origin bot-state >/dev/null 2>&1; then
  git fetch --no-tags origin bot-state:refs/remotes/origin/bot-state >/dev/null 2>&1 || true
  if git cat-file -e "origin/bot-state:${REMOTE_PATH}" 2>/dev/null; then
    git show "origin/bot-state:${REMOTE_PATH}" > "${JSON_PATH}"
    echo "[STATE] Pulled ${REMOTE_PATH} from bot-state branch."
  else
    echo "[STATE] WARN: state.json not found in bot-state branch. Initializing."
    echo "${DEFAULT_STATE}" > "${JSON_PATH}"
  fi
  if git cat-file -e "origin/bot-state:${POS_REMOTE_PATH}" 2>/dev/null; then
    git show "origin/bot-state:${POS_REMOTE_PATH}" > "${POS_JSON_PATH}"
    echo "[STATE] Pulled ${POS_REMOTE_PATH} from bot-state branch."
  else
    echo "[STATE] WARN: position state not found in bot-state branch. Initializing."
    echo "${DEFAULT_POS_STATE}" > "${POS_JSON_PATH}"
  fi
  if git cat-file -e "origin/bot-state:${INTENT_REMOTE_PATH}" 2>/dev/null; then
    git show "origin/bot-state:${INTENT_REMOTE_PATH}" > "${INTENT_LOG_PATH}"
    echo "[STATE] Pulled ${INTENT_REMOTE_PATH} from bot-state branch."
  else
    echo "[STATE] WARN: intent log not found in bot-state branch. Initializing."
    echo -n "${DEFAULT_INTENT_LOG}" > "${INTENT_LOG_PATH}"
  fi
  if git cat-file -e "origin/bot-state:${INTENT_CURSOR_REMOTE_PATH}" 2>/dev/null; then
    git show "origin/bot-state:${INTENT_CURSOR_REMOTE_PATH}" > "${INTENT_CURSOR_PATH}"
    echo "[STATE] Pulled ${INTENT_CURSOR_REMOTE_PATH} from bot-state branch."
  else
    echo "[STATE] WARN: intent cursor not found in bot-state branch. Initializing."
    echo "${DEFAULT_INTENT_CURSOR}" > "${INTENT_CURSOR_PATH}"
  fi
else
  echo "[STATE] WARN: bot-state branch not found. Initializing."
  echo "${DEFAULT_STATE}" > "${JSON_PATH}"
  echo "${DEFAULT_POS_STATE}" > "${POS_JSON_PATH}"
  echo -n "${DEFAULT_INTENT_LOG}" > "${INTENT_LOG_PATH}"
  echo "${DEFAULT_INTENT_CURSOR}" > "${INTENT_CURSOR_PATH}"
fi
