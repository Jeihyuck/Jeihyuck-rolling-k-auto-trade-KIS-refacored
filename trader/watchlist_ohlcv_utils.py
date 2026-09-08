from __future__ import annotations

from typing import List

import pandas as pd


def normalize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    normalized = df.copy()
    normalized.columns = [str(col).strip().lower().replace(" ", "_") for col in normalized.columns]

    def _map_if_missing(target: str, candidates: List[str]) -> None:
        if target in normalized.columns:
            return
        for candidate in candidates:
            if candidate in normalized.columns:
                normalized.rename(columns={candidate: target}, inplace=True)
                return

    _map_if_missing("close", ["adj_close", "adjusted_close", "close_price", "stck_clpr"])
    _map_if_missing("high", ["high_price", "stck_hgpr"])
    _map_if_missing("low", ["low_price", "stck_lwpr"])
    _map_if_missing("volume", ["vol", "trade_volume", "acml_vol", "acml_volm"])
    return normalized
