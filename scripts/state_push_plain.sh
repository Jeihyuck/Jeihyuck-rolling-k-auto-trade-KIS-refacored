#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

STATE_SRC="trader/state"
LOG_SRC="trader/logs"
FILLS_SRC="trader/fills"
REBAL_SRC="rebalance_results"

for f in "$STATE_SRC/state.json" "$STATE_SRC/orders_map.jsonl"; do
  if [[ ! -f "$f" ]]; then
    echo "[STATE_PUSH] missing $f, aborting."
    exit 0
  fi
done

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
export TARGET
mkdir -p "$TARGET/state" "$TARGET/logs" "$TARGET/fills" "$TARGET/rebalance_results"

rsync -av --delete --exclude '__pycache__' --exclude '*.pyc' "$STATE_SRC/" "$TARGET/state/"
rsync -av --delete --exclude '__pycache__' --exclude '*.pyc' "$LOG_SRC/" "$TARGET/logs/" || true
rsync -av --delete --exclude '__pycache__' --exclude '*.pyc' "$FILLS_SRC/" "$TARGET/fills/" || true
rsync -av --delete --exclude '__pycache__' --exclude '*.pyc' "$REBAL_SRC/" "$TARGET/rebalance_results/" || true

find "$TARGET" -name "*.py" -o -name "*.pyc" -o -path "*/__pycache__*" -print -delete
rm -rf "$TARGET/trader" || true

MANIFEST="$TARGET/MANIFEST.json"
run_id="${GITHUB_RUN_ID:-local}"
commit_sha="$(git rev-parse --short HEAD)"
now_ts="$(date -Iseconds)"
export MANIFEST run_id commit_sha now_ts

python - <<'PY'
import json, os, pathlib, sys
target = pathlib.Path(os.environ["TARGET"])
manifest = pathlib.Path(os.environ["MANIFEST"])
state_path = target / "state" / "state.json"
orders_path = target / "state" / "orders_map.jsonl"
ledger_path = target / "logs" / "ledger.jsonl"
try:
    payload = json.loads(state_path.read_text(encoding="utf-8"))
except Exception:
    payload = {"lots": [], "meta": {}}
lots = payload.get("lots") or []
counts = {
    "n_lots": sum(1 for lot in lots if int(lot.get("remaining_qty") or lot.get("qty") or 0) > 0),
    "n_unknown": sum(1 for lot in lots if str(lot.get("sid") or lot.get("strategy_id")).upper() == "UNKNOWN"),
    "n_manual": sum(1 for lot in lots if str(lot.get("sid") or lot.get("strategy_id")).upper() == "MANUAL"),
}
files = []
for name, path in [
    ("state/state.json", state_path),
    ("state/orders_map.jsonl", orders_path),
    ("logs/ledger.jsonl", ledger_path),
]:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        size = 0
    files.append({"path": name, "size": size})

manifest.write_text(
    json.dumps(
        {
            "schema_version": "v3",
            "updated_at": os.environ.get("now_ts", ""),
            "github_run_id": os.environ.get("run_id", "local"),
            "commit_sha": os.environ.get("commit_sha", ""),
            "counts": counts,
            "files": files,
            "recovery_stats": payload.get("meta", {}).get("recovery_stats", {}),
        },
        ensure_ascii=False,
        indent=2,
    ),
    encoding="utf-8",
)
PY

pushd "$WORKTREE_DIR" >/dev/null
git config user.name "trade-bot"
git config user.email "trade-bot@users.noreply.github.com"

git add -f bot_state/trader_state

if git diff --cached --quiet; then
  echo "[STATE_PUSH] No changes to commit."
  exit 0
fi

if git diff --name-only --cached | grep -E "bot_state/trader_state/trader|\\.py$|\\.pyc$|__pycache__" >/dev/null; then
  echo "[STATE_PUSH] forbidden file staged. aborting."
  git reset --hard
  exit 1
fi

git commit -m "Update trader state [skip ci]"
git push origin bot-state
popd >/dev/null
echo "[STATE_PUSH] done."
