from __future__ import annotations

import os
from pathlib import Path

TRADER_RUNTIME_DIR_ENV = "TRADER_RUNTIME_DIR"
TRADER_CACHE_ROOT_ENV = "TRADER_CACHE_ROOT"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def runtime_root() -> Path:
    explicit = os.getenv(TRADER_RUNTIME_DIR_ENV)
    if explicit:
        return Path(explicit).expanduser().resolve()
    base = os.getenv("RUNNER_TEMP") or "/tmp"
    return (Path(base) / "trader_runtime").expanduser().resolve()


def get_botstate_root() -> Path:
    return runtime_root()


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
        root = get_botstate_root() / "runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root


def get_ohlcv_cache_dir() -> Path:
    cache_dir = get_cache_root() / "ohlcv_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def close_entry_orders_path(order_date: str) -> Path:
    """order_date: 'YYYY-MM-DD'"""
    path = botstate_path("runtime", "close_entry", f"orders_{order_date}.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
