#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

TARGET_STATE_DIR="trader/state"
TARGET_LOG_DIR="trader/logs"
TARGET_FILLS_DIR="trader/fills"
TARGET_REBAL_DIR="rebalance_results"

mkdir -p "$TARGET_STATE_DIR" "$TARGET_LOG_DIR" "$TARGET_FILLS_DIR" "$TARGET_REBAL_DIR"

if ! git remote get-url origin >/dev/null 2>&1; then
  echo "[STATE_PULL] origin remote missing. Nothing to pull."
  touch "$TARGET_STATE_DIR/state.json" "$TARGET_STATE_DIR/orders_map.jsonl" "$TARGET_LOG_DIR/ledger.jsonl"
  exit 0
fi

git fetch --no-tags origin bot-state:refs/remotes/origin/bot-state >/dev/null 2>&1 || true

copy_path() {
  local remote_path="$1"
  local dest="$2"
  if git cat-file -e "origin/bot-state:${remote_path}" 2>/dev/null; then
    mkdir -p "$(dirname "$dest")"
    git show "origin/bot-state:${remote_path}" > "$dest"
    echo "[STATE_PULL] restored ${remote_path} -> ${dest}"
  fi
}

copy_tree() {
  local remote_dir="$1"
  local dest_dir="$2"
  mkdir -p "$dest_dir"
  git ls-tree -r "origin/bot-state" "$remote_dir" --name-only 2>/dev/null | while read -r file; do
    dest_path="$dest_dir/${file#${remote_dir}/}"
    copy_path "$file" "$dest_path"
  done
}

copy_path "bot_state/trader_state/trader/state/state.json" "$TARGET_STATE_DIR/state.json"
copy_path "bot_state/trader_state/trader/state/orders_map.jsonl" "$TARGET_STATE_DIR/orders_map.jsonl"
copy_tree "bot_state/trader_state/trader/logs" "$TARGET_LOG_DIR"
copy_tree "bot_state/trader_state/trader/fills" "$TARGET_FILLS_DIR"
copy_tree "bot_state/trader_state/rebalance_results" "$TARGET_REBAL_DIR"
# fallback for legacy layout
if [[ ! -s "$TARGET_STATE_DIR/state.json" ]]; then
  copy_path "bot_state/trader_state/state/state.json" "$TARGET_STATE_DIR/state.json"
fi
if [[ ! -s "$TARGET_STATE_DIR/orders_map.jsonl" ]]; then
  copy_path "bot_state/trader_state/state/orders_map.jsonl" "$TARGET_STATE_DIR/orders_map.jsonl"
fi

touch "$TARGET_STATE_DIR/state.json" "$TARGET_STATE_DIR/orders_map.jsonl" "$TARGET_LOG_DIR/ledger.jsonl"
echo "[STATE_PULL] done."
