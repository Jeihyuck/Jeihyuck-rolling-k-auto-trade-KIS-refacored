from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from trader.botstate_paths import botstate_path, get_botstate_root
from trader.botstate_sync import (
    acquire_lock,
    BotStatePersistError,
    persist_run_files,
    release_lock,
    resolve_botstate_worktree_dir,
    setup_worktree,
)

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")


def _is_git_repo(path: str) -> bool:
    repo_path = Path(path)
    return (repo_path / ".git").is_file() or (repo_path / ".git").is_dir()


def _add_if_exists(paths: list[Path], candidate: Path) -> None:
    if candidate.is_file():
        paths.append(candidate)


def _add_glob(paths: list[Path], base_dir: Path, pattern: str) -> None:
    for path in base_dir.glob(pattern):
        if path.is_file():
            paths.append(path)


def _collect_paths(base_dir: Path) -> list[Path]:
    paths: list[Path] = []
    bot_root = get_botstate_root()
    _add_if_exists(paths, bot_root / "runtime/state.json")
    _add_if_exists(paths, bot_root / "runtime/lot_state.json")
    _add_glob(paths, bot_root, "runtime/universe/*.json")
    _add_glob(paths, bot_root, "runtime/diagnostics/*.flag")
    _add_glob(paths, bot_root, "universe_lkg/**/latest.json")
    return paths


def _resolve_owner() -> str:
    return os.getenv("GITHUB_ACTOR", "local")


def _resolve_run_id() -> str:
    return os.getenv("GITHUB_RUN_ID", datetime.now(tz=KST).strftime("%Y%m%d%H%M%S"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Persist bot_state files using the bot-state worktree")
    parser.add_argument("--message", required=True, help="Commit message for bot_state persist")
    parser.add_argument(
        "--soft-fail",
        action="store_true",
        help="Log errors and exit 0 instead of failing the job",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        worktree_env = os.environ.get("BOTSTATE_WORKTREE_DIR", "").strip()
        if not worktree_env:
            logger.info("[PERSIST] skip: BOTSTATE_WORKTREE_DIR is empty")
            return 0
        if not _is_git_repo(worktree_env):
            logger.info("[PERSIST] skip: worktree is not a git repo (PB1 exited early)")
            return 0

        base_dir = Path.cwd().resolve()
        worktree_dir = resolve_botstate_worktree_dir(base_dir)
        setup_worktree(base_dir, worktree_dir, target_branch="bot-state")

        owner = _resolve_owner()
        run_id = _resolve_run_id()

        if not acquire_lock(worktree_dir, owner=owner, run_id=run_id, ttl_sec=None):
            logger.error("[BOTSTATE][PERSIST] lock_unavailable owner=%s run_id=%s", owner, run_id)
            return 1

        try:
            files = _collect_paths(base_dir)
            if not files:
                logger.info("[BOTSTATE][PERSIST] no_files_found base_dir=%s", base_dir)
                return 0
            try:
                persist_run_files(worktree_dir, files, args.message)
            except BotStatePersistError as exc:
                if exc.require_persist and not exc.dirty_by_stat:
                    logger.warning(
                        "[BOTSTATE][PERSIST][EMPTY_STAGE][WARN] require_persist=1 dirty_by_stat=0 -> skip"
                    )
                    return 0
                raise
            return 0
        finally:
            release_lock(worktree_dir, owner=owner, run_id=run_id)
    except Exception as exc:
        if args.soft_fail:
            logger.warning("[BOTSTATE][PERSIST][SOFT-FAIL] %s", exc, exc_info=True)
            return 0
        raise


if __name__ == "__main__":
    sys.exit(main())
