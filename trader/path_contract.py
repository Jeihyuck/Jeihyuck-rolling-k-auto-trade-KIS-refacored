from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any


def _as_of_str(as_of: str | date) -> str:
    if isinstance(as_of, date):
        return as_of.isoformat()
    return str(as_of).strip()


def resolve_repo_root() -> Path:
    explicit = (os.getenv("TRADER_REPO_ROOT") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    return Path(__file__).resolve().parents[1]


def build_final30_paths(repo_root: Path | None, env: str, as_of: str | date) -> dict[str, Path]:
    root = (repo_root or resolve_repo_root()).expanduser().resolve()
    env_n = (env or "").strip().lower()
    as_of_s = _as_of_str(as_of)
    return {
        "runtime": root / "runtime" / "watchlist" / as_of_s / "final30_scored.json",
        "ledger": root / "bot_state" / "trader_ledger" / "final30" / env_n / as_of_s / "final30_scored.json",
        "signals": root / "signals" / "final30.json",
    }


def build_watchlist_paths(repo_root: Path | None, as_of: str | date) -> dict[str, Path]:
    root = (repo_root or resolve_repo_root()).expanduser().resolve()
    as_of_s = _as_of_str(as_of)
    return {
        "watchlist_dir": root / "runtime" / "watchlist" / as_of_s,
        "snapshot": root / "runtime" / "snapshots" / "final30.json",
    }


def serialize_path_map(path_map: dict[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in (path_map or {}).items()}