#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="bot_state"
JSON_PATH="${STATE_DIR}/state.json"

if [[ ! -f "${JSON_PATH}" ]]; then
  echo "[STATE] WARN: ${JSON_PATH} not found. Skipping."
  exit 0
fi

tmp_state="$(mktemp)"
trap 'rm -f "${tmp_state}"' EXIT
cp -f "${JSON_PATH}" "${tmp_state}"

# IMPORTANT: avoid "untracked would be overwritten by checkout"
rm -f "${JSON_PATH}" || true

if git ls-remote --exit-code --heads origin bot-state >/dev/null 2>&1; then
  git fetch --no-tags origin bot-state:refs/remotes/origin/bot-state >/dev/null 2>&1 || true
  git checkout -B bot-state origin/bot-state
else
  git checkout --orphan bot-state
  git rm -r --cached . >/dev/null 2>&1 || true
fi

mkdir -p "${STATE_DIR}"
cp -f "${tmp_state}" "${JSON_PATH}"

git add -f "${JSON_PATH}"
git status --porcelain
if git diff --cached --quiet; then
  echo "[STATE] No changes to commit."
  exit 0
fi

git commit -m "Update bot state (plain) [skip ci]"
git push --force-with-lease origin HEAD:bot-state
echo "[STATE] Pushed ${JSON_PATH} to bot-state branch."
