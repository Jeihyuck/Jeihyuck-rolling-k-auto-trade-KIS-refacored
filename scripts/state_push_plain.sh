#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="bot_state"
JSON_PATH="${STATE_DIR}/state.json"
POS_STATE_DIR="trader/state"
POS_JSON_PATH="${POS_STATE_DIR}/state.json"
INTENT_LOG_PATH="${POS_STATE_DIR}/strategy_intents.jsonl"
INTENT_CURSOR_PATH="${POS_STATE_DIR}/strategy_intents_state.json"

if [[ ! -f "${JSON_PATH}" ]]; then
  echo "[STATE] WARN: ${JSON_PATH} not found. Skipping."
  exit 0
fi
if [[ ! -f "${POS_JSON_PATH}" ]]; then
  echo "[STATE] WARN: ${POS_JSON_PATH} not found. Skipping."
  exit 0
fi

tmp_state="$(mktemp)"
tmp_pos_state="$(mktemp)"
tmp_intent_log="$(mktemp)"
tmp_intent_cursor="$(mktemp)"
trap 'rm -f "${tmp_state}" "${tmp_pos_state}" "${tmp_intent_log}" "${tmp_intent_cursor}"' EXIT
cp -f "${JSON_PATH}" "${tmp_state}"
cp -f "${POS_JSON_PATH}" "${tmp_pos_state}"
cp -f "${INTENT_LOG_PATH}" "${tmp_intent_log}" 2>/dev/null || touch "${tmp_intent_log}"
cp -f "${INTENT_CURSOR_PATH}" "${tmp_intent_cursor}" 2>/dev/null || touch "${tmp_intent_cursor}"

# IMPORTANT: avoid "untracked would be overwritten by checkout"
rm -f "${JSON_PATH}" || true
rm -f "${POS_JSON_PATH}" || true
rm -f "${INTENT_LOG_PATH}" || true
rm -f "${INTENT_CURSOR_PATH}" || true

if git ls-remote --exit-code --heads origin bot-state >/dev/null 2>&1; then
  git fetch --no-tags origin bot-state:refs/remotes/origin/bot-state >/dev/null 2>&1 || true
  git checkout -B bot-state origin/bot-state
else
  git checkout --orphan bot-state
  git rm -r --cached . >/dev/null 2>&1 || true
fi

mkdir -p "${STATE_DIR}"
cp -f "${tmp_state}" "${JSON_PATH}"
mkdir -p "${POS_STATE_DIR}"
cp -f "${tmp_pos_state}" "${POS_JSON_PATH}"
cp -f "${tmp_intent_log}" "${INTENT_LOG_PATH}"
cp -f "${tmp_intent_cursor}" "${INTENT_CURSOR_PATH}"

git add -f "${JSON_PATH}"
git add -f "${POS_JSON_PATH}"
git add -f "${INTENT_LOG_PATH}"
git add -f "${INTENT_CURSOR_PATH}"
git status --porcelain
if git diff --cached --quiet; then
  echo "[STATE] No changes to commit."
  exit 0
fi

git commit -m "Update bot state (plain) [skip ci]"
git push --force-with-lease origin HEAD:bot-state
echo "[STATE] Pushed ${JSON_PATH} to bot-state branch."
