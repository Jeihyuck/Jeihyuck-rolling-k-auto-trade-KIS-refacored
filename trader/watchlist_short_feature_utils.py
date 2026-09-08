from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def short_feature_sample_rows(df: pd.DataFrame, limit: int = 5) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    sample_columns = [column for column in ("code", "ma20", "volume_avg20", "ma50", "close") if column in df.columns]
    if not sample_columns:
        return []
    return df.loc[:, sample_columns].head(limit).to_dict(orient="records")


def short_feature_null_count(df: pd.DataFrame, column: str) -> int:
    if df is None or column not in df.columns:
        return -1
    return int(pd.to_numeric(df[column], errors="coerce").isna().sum())


def should_backfill_short_horizon_features(df: pd.DataFrame, *, require_all_null: bool) -> bool:
    if df is None or df.empty:
        return False

    rows = len(df)
    if rows <= 0:
        return False

    ma20_missing = "ma20" not in df.columns
    vol20_missing = "volume_avg20" not in df.columns
    ma20_null = short_feature_null_count(df, "ma20")
    vol20_null = short_feature_null_count(df, "volume_avg20")

    if require_all_null:
        return bool(ma20_missing or vol20_missing or ma20_null == rows or vol20_null == rows)
    return bool(ma20_missing or vol20_missing or ma20_null > 0 or vol20_null > 0)
