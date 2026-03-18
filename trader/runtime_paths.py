from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from trader.path_contract import build_final30_paths, resolve_repo_root

TRADER_RUNTIME_DIR_ENV = "TRADER_RUNTIME_DIR"
TRADER_CACHE_ROOT_ENV = "TRADER_CACHE_ROOT"


def _repo_root() -> Path:
    return resolve_repo_root()


def repo_root() -> Path:
    return _repo_root()


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


def ensure_not_repo_tracked_path(path: Path) -> None:
    repo_root = _repo_root()
    seeds_dir = (repo_root / "trader" / "universe" / "seeds").resolve()
    target = path.expanduser().resolve()
    if _is_relative_to(target, seeds_dir):
        raise RuntimeError(f"Refusing to write to repo-tracked seed path: {target}")


def runtime_path(*parts: str) -> Path:
    root = runtime_root()
    path = root.joinpath(*parts)
    resolved = path.expanduser().resolve()
    if not _is_relative_to(resolved, root):
        raise RuntimeError(f"Runtime path must stay under {root}: {resolved}")
    ensure_not_repo_tracked_path(resolved)
    return path


def get_cache_root() -> Path:
    env_root = os.getenv(TRADER_CACHE_ROOT_ENV)
    if env_root:
        root = Path(env_root).expanduser().resolve()
    else:
        root = runtime_root() / "runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root


def get_ohlcv_cache_dir() -> Path:
    cache_dir = get_cache_root() / "ohlcv_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def close_entry_orders_path(order_date: str) -> Path:
    """order_date: 'YYYY-MM-DD'"""
    path = runtime_path("runtime", "close_entry", f"orders_{order_date}.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _as_of_str(as_of: str | date) -> str:
    if isinstance(as_of, date):
        return as_of.isoformat()
    return str(as_of).strip()


def build_final30_scored_paths(repo_root: Path, env: str, as_of: str | date) -> dict[str, Path]:
    return build_final30_paths(repo_root, env, as_of)


def get_final30_scored_paths(env: str, as_of: str | date) -> list[Path]:
    return list(build_final30_scored_paths(repo_root(), env, as_of).values())[:2]


def get_final30_artifact_paths(
    env: str,
    as_of: str | date,
    *,
    include_legacy: bool = True,
) -> list[tuple[str, Path]]:
    paths_by_label = build_final30_scored_paths(repo_root(), env, as_of)
    paths: list[tuple[str, Path]] = [
        ("runtime_final30_scored", paths_by_label["runtime"]),
        ("ledger_final30_scored", paths_by_label["ledger"]),
    ]
    if include_legacy:
        paths.append(("signals_final30", paths_by_label["signals"]))
    return paths
