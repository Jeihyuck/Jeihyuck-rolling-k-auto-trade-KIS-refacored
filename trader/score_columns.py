from __future__ import annotations

from typing import Mapping

import pandas as pd

SCORE_ALIAS_CANDIDATES: dict[str, tuple[str, ...]] = {
    "tech": ("tech_score", "tech", "score_tech"),
    "final": ("score_final", "final_score", "score"),
    "breakout": ("breakout_score", "score_breakout", "breakout"),
    "pullback": ("pullback_score", "score_pullback", "pullback"),
    "momentum": ("momentum_score", "score_momentum", "momentum"),
    "rs": ("rs_score",),
    "vcp": ("vcp_score",),
    "trend": ("trend_score",),
}


def resolve_score_column(df: pd.DataFrame, logical_name: str) -> str | None:
    if df is None or df.empty:
        return None
    candidates = SCORE_ALIAS_CANDIDATES.get(logical_name, ())
    for col in candidates:
        if col in df.columns:
            return col
    return None


def count_nonzero_score(df: pd.DataFrame, logical_name: str) -> tuple[int, str | None]:
    col = resolve_score_column(df, logical_name)
    if col is None:
        return 0, None
    vals = pd.to_numeric(df[col], errors="coerce")
    return int((vals.fillna(0.0) != 0.0).sum()), col


def collect_nonzero_score_stats(
    df: pd.DataFrame,
    logical_names: tuple[str, ...] = ("tech", "final", "breakout", "pullback", "momentum", "rs", "vcp", "trend"),
) -> tuple[dict[str, int], dict[str, str | None]]:
    stats: dict[str, int] = {}
    alias_cols: dict[str, str | None] = {}
    for logical_name in logical_names:
        count, col = count_nonzero_score(df, logical_name)
        stats[f"{logical_name}_nonzero"] = count
        alias_cols[logical_name] = col
    if "final_nonzero" in stats:
        stats["score_final_nonzero"] = stats["final_nonzero"]
    return stats, alias_cols


def has_required_score_fields(df: pd.DataFrame, required_keys: tuple[str, ...]) -> bool:
    if df is None or df.empty:
        return False
    for key in required_keys:
        if resolve_score_column(df, key) is None:
            return False
    return True


def has_column_alias(df: pd.DataFrame, logical_name: str) -> int:
    return int(resolve_score_column(df, logical_name) is not None)


def resolve_alias_map(df: pd.DataFrame, logical_names: tuple[str, ...]) -> Mapping[str, str | None]:
    return {name: resolve_score_column(df, name) for name in logical_names}
