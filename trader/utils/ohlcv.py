from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


def _match_column(columns_lower: List[str], candidates: List[str]) -> str | None:
    for cand in candidates:
        if cand.lower() in columns_lower:
            return cand.lower()
    return None


def _to_numeric_series(s: pd.Series) -> pd.Series:
    def _clean(val: object) -> object:
        if isinstance(val, str):
            stripped = val.strip()
            lower = stripped.lower()
            if lower in {"", "null", "none"}:
                return np.nan
            cleaned = (
                stripped.replace(",", "")
                .replace(" ", "")
                .replace("_", "")
                .replace("+", "")
            )
            return cleaned
        return val

    cleaned = s.apply(_clean)
    return pd.to_numeric(cleaned, errors="coerce")


def normalize_ohlcv(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Normalize OHLCV columns to a standard schema.

    Standard columns: date, open, high, low, close, volume
    - volume is kept as NaN when missing (never filled with 0)
    - numeric columns are coerced with errors='coerce'
    - rows are sorted by date and de-duplicated on date (keep last)

    Returns (df_norm, meta) where meta contains:
      - volume_missing: bool
      - source_cols: list of original columns
      - mapped: mapping of detected source -> target
    """

    if df is None:
        return pd.DataFrame(), {"volume_missing": True, "source_cols": [], "mapped": {}}

    df_copy = df.copy()
    original_columns = [str(c) for c in df_copy.columns]
    columns_lower = [c.strip().lower() for c in original_columns]
    col_map: Dict[str, str] = {}

    candidates: Dict[str, List[str]] = {
        "date": [
            "date",
            "stck_bsop_date",
            "tdd_clsp",
            "bas_dt",
            "날짜",
        ],
        "open": ["open", "stck_oprc", "oprc", "시가", "opn_prc"],
        "high": ["high", "stck_hgpr", "hgpr", "고가"],
        "low": ["low", "stck_lwpr", "lwpr", "저가"],
        "close": ["close", "stck_clpr", "clpr", "tp", "종가"],
        "volume": [
            "volume",
            "vol",
            "acml_vol",
            "acc_vol",
            "trade_volume",
            "거래량",
            "stck_vol",
            "acml_tr_pbmn",
            "stck_trqu",
            "volume(주)",
            "volume ",
        ],
    }

    for target, cand_list in candidates.items():
        matched = _match_column(columns_lower, cand_list)
        if matched:
            col_map[matched] = target

    df_copy.columns = columns_lower
    mapped: Dict[str, str] = {}
    for src, dst in col_map.items():
        if src in df_copy.columns:
            mapped[src] = dst
    df_norm = df_copy.rename(columns=mapped)

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df_norm.columns:
            df_norm[col] = np.nan
        df_norm[col] = _to_numeric_series(df_norm[col])

    if "date" in df_norm.columns:
        df_norm["date"] = pd.to_datetime(df_norm["date"], errors="coerce")
        df_norm = df_norm.dropna(subset=["date"])

    df_norm = df_norm.sort_values("date").drop_duplicates("date", keep="last")

    mapped_targets = set(mapped.values())
    volume_missing = bool("volume" not in mapped_targets or df_norm["volume"].isna().all())
    meta = {
        "volume_missing": volume_missing,
        "source_cols": original_columns,
        "mapped": {dst: src for src, dst in mapped.items()},
    }

    return df_norm, meta
