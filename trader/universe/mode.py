from __future__ import annotations

import os


def is_db_only_mode() -> bool:
    source = (os.getenv("UNIVERSE_SOURCE") or "").strip().upper()
    return source == "DB_ONLY" or os.getenv("DB_ONLY") == "1" or os.getenv("UNIVERSE_DB_ONLY") == "1"


def resolve_strategy_mode() -> str:
    return (os.getenv("EFFECTIVE_STRATEGY_MODE") or os.getenv("STRATEGY_MODE") or "").strip().upper()
