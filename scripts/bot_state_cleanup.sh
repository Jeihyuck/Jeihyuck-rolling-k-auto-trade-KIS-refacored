#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

WORKTREE_DIR="$(mktemp -d)"
cleanup() {
  git worktree remove "$WORKTREE_DIR" --force 2>/dev/null || true
  rm -rf "$WORKTREE_DIR"
}
trap cleanup EXIT

git fetch origin bot-state --prune || true
if git show-ref --verify --quiet refs/remotes/origin/bot-state; then
  git worktree add -B bot-state "$WORKTREE_DIR" origin/bot-state
else
  git worktree add -B bot-state "$WORKTREE_DIR"
fi

TARGET="$WORKTREE_DIR/bot_state/trader_state"
mkdir -p "$TARGET"

find "$TARGET" -name "__pycache__" -type d -prune -exec rm -rf {} + || true
find "$TARGET" -name "*.pyc" -type f -delete || true
find "$TARGET" -name "*.py" -type f -delete || true
rm -rf "$TARGET/trader" || true

git config --worktree user.name "trade-bot"
git config --worktree user.email "trade-bot@users.noreply.github.com"

git add -f bot_state/trader_state || true

if git diff --cached --quiet; then
  echo "[CLEANUP] nothing to clean."
  exit 0
fi

git commit -m "Cleanup bot-state to data-only [skip ci]"
git push origin bot-state
echo "[CLEANUP] bot-state cleaned."
