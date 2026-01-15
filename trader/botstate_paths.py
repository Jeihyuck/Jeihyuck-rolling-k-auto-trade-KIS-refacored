from __future__ import annotations

import os
from pathlib import Path

BOTSTATE_ROOT_ENV = "BOTSTATE_ROOT"
BOTSTATE_WORKTREE_DIR_ENV = "BOTSTATE_WORKTREE_DIR"
TRADER_CACHE_ROOT_ENV = "TRADER_CACHE_ROOT"
DEFAULT_BOTSTATE_ROOT = "bot_state"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def get_botstate_root() -> Path:
    env_root = os.getenv(BOTSTATE_ROOT_ENV)
    if env_root:
        return Path(env_root).expanduser().resolve()
    worktree_dir = os.getenv(BOTSTATE_WORKTREE_DIR_ENV)
    if worktree_dir:
        return (Path(worktree_dir).expanduser() / "bot_state").resolve()
    return Path(DEFAULT_BOTSTATE_ROOT).resolve()


def ensure_not_repo_tracked_path(path: Path) -> None:
    repo_root = _repo_root()
    seeds_dir = (repo_root / "trader" / "universe" / "seeds").resolve()
    target = path.expanduser().resolve()
    if _is_relative_to(target, seeds_dir):
        raise RuntimeError(f"Refusing to write to repo-tracked seed path: {target}")


def botstate_path(*parts: str) -> Path:
    root = get_botstate_root()
    path = root.joinpath(*parts)
    resolved = path.expanduser().resolve()
    if not _is_relative_to(resolved, root):
        raise RuntimeError(f"Botstate path must stay under {root}: {resolved}")
    ensure_not_repo_tracked_path(resolved)
    return path


def get_cache_root() -> Path:
    env_root = os.getenv(TRADER_CACHE_ROOT_ENV)
    if env_root:
        root = Path(env_root).expanduser().resolve()
    else:
        botstate_runtime = get_botstate_root() / "runtime"
        root = botstate_runtime.resolve() if botstate_runtime.exists() else Path("runtime").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def get_ohlcv_cache_dir() -> Path:
    cache_dir = get_cache_root() / "ohlcv_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
