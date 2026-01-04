#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="bot_state"
JSON_PATH="${STATE_DIR}/state.json"
POS_STATE_DIR="trader/state"
POS_JSON_PATH="${POS_STATE_DIR}/state.json"
INTENT_LOG_PATH="${POS_STATE_DIR}/strategy_intents.jsonl"
INTENT_CURSOR_PATH="${POS_STATE_DIR}/strategy_intents_state.json"
DIAG_DIR="${POS_STATE_DIR}/diagnostics"
DIAG_LATEST="${DIAG_DIR}/diag_latest.json"
LEDGER_JSON="${POS_STATE_DIR}/../logs/ledger.jsonl"

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
tmp_diag_dir="$(mktemp -d)"
trap 'rm -f "${tmp_state}" "${tmp_pos_state}" "${tmp_intent_log}" "${tmp_intent_cursor}"; rm -rf "${tmp_diag_dir}"' EXIT
cp -f "${JSON_PATH}" "${tmp_state}"
cp -f "${POS_JSON_PATH}" "${tmp_pos_state}"
cp -f "${INTENT_LOG_PATH}" "${tmp_intent_log}" 2>/dev/null || touch "${tmp_intent_log}"
cp -f "${INTENT_CURSOR_PATH}" "${tmp_intent_cursor}" 2>/dev/null || touch "${tmp_intent_cursor}"
if [[ -f "${DIAG_LATEST}" ]]; then
  cp -f "${DIAG_LATEST}" "${tmp_diag_dir}/diag_latest.json"
fi
if [[ -d "${DIAG_DIR}" ]]; then
  find "${DIAG_DIR}" -maxdepth 1 -name "diag_*.json" -type f -exec cp -f {} "${tmp_diag_dir}/" \;
fi
if [[ -f "${LEDGER_JSON}" ]]; then
  cp -f "${LEDGER_JSON}" "${tmp_diag_dir}/ledger.jsonl" 2>/dev/null || true
fi

# IMPORTANT: avoid "untracked would be overwritten by checkout"
rm -f "${JSON_PATH}" || true
rm -f "${POS_JSON_PATH}" || true
rm -f "${INTENT_LOG_PATH}" || true
rm -f "${INTENT_CURSOR_PATH}" || true
rm -rf "${DIAG_DIR}" || true

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
mkdir -p "${DIAG_DIR}"
if [[ -d "${tmp_diag_dir}" ]]; then
  cp -f "${tmp_diag_dir}/diag_latest.json" "${DIAG_DIR}/diag_latest.json" 2>/dev/null || true
  find "${tmp_diag_dir}" -maxdepth 1 -name "diag_*.json" -type f -exec cp -f {} "${DIAG_DIR}/" \;
  if [[ -f "${tmp_diag_dir}/ledger.jsonl" ]]; then
    mkdir -p "$(dirname "${LEDGER_JSON}")"
    cp -f "${tmp_diag_dir}/ledger.jsonl" "${LEDGER_JSON}" 2>/dev/null || true
  fi
fi

git add -f "${JSON_PATH}"
git add -f "${POS_JSON_PATH}"
git add -f "${INTENT_LOG_PATH}"
git add -f "${INTENT_CURSOR_PATH}"
git add -f ${DIAG_DIR}/diag_*.json 2>/dev/null || true
git add -f "${DIAG_DIR}/diag_latest.json" 2>/dev/null || true
git add -f "${POS_STATE_DIR}/strategy_intents_state.json" 2>/dev/null || true
git add -f "${POS_STATE_DIR}/strategy_intents.jsonl" 2>/dev/null || true
git add -f "${LEDGER_JSON}" 2>/dev/null || true
echo "[STATE] Staged diagnostics + state.json"
git status --porcelain
if git diff --cached --quiet; then
  echo "[STATE] No changes to commit."
  exit 0
fi

git commit -m "Update bot state (plain) [skip ci]"
git push --force-with-lease origin HEAD:bot-state
echo "[STATE] Pushed ${JSON_PATH} to bot-state branch."
echo "[STATE] Pushed diagnostics dumps to bot-state branch."
