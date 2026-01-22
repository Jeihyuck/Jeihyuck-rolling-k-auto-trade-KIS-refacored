from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class VolContractRules:
    min_contractions: int = 2
    max_contractions: int = 4
    decline_min: float = 0.15


@dataclass
class PriceTightRules:
    tight_close_max_pct: float = 0.015


def score_vcp(
    ohlcv_df: pd.DataFrame,
    lookback: int,
    vol_contract_rules: VolContractRules,
    price_tight_rules: PriceTightRules,
) -> int:
    if ohlcv_df is None or len(ohlcv_df) < lookback:
        return 0
    window = ohlcv_df.tail(lookback)
    ranges = (window["high"] - window["low"]).to_numpy()
    closes = window["close"].to_numpy()
    range_pct = np.where(closes > 0, ranges / closes, 0.0)

    segments = np.array_split(range_pct, vol_contract_rules.max_contractions)
    contractions = [float(np.nanmax(seg)) for seg in segments if len(seg) > 0]
    contractions = [c for c in contractions if np.isfinite(c)]

    if len(contractions) < vol_contract_rules.min_contractions:
        return 0

    contraction_ok = True
    for prev, curr in zip(contractions, contractions[1:]):
        if prev <= 0:
            contraction_ok = False
            break
        decline = 1.0 - (curr / prev)
        if decline < vol_contract_rules.decline_min:
            contraction_ok = False
            break

    if not contraction_ok:
        return 0

    recent = window["close"].tail(5)
    mean_close = float(recent.mean())
    tight_ok = False
    if mean_close > 0:
        tight_ok = (float(recent.max()) - float(recent.min())) / mean_close <= price_tight_rules.tight_close_max_pct

    vol10 = float(window["volume"].tail(10).mean())
    vol50 = float(window["volume"].tail(50).mean()) if len(window) >= 50 else float("nan")
    vol_dryup = bool(np.isfinite(vol50) and vol50 > 0 and vol10 <= vol50 * 0.7)

    score = 0
    if contraction_ok:
        score += 40
    if tight_ok:
        score += 30
    if vol_dryup:
        score += 30
    return int(min(100, score))


def find_pivot(ohlcv_df: pd.DataFrame) -> dict:
    if ohlcv_df is None or ohlcv_df.empty:
        return {"pivot_price": float("nan"), "pivot_date": None, "tight_low": None, "base_high": None}
    window = ohlcv_df.tail(120)
    candidate = window.iloc[:-10] if len(window) > 10 else window
    idx = int(candidate["high"].values.argmax())
    pivot_price = float(candidate["high"].iloc[idx])
    pivot_date = candidate["date"].iloc[idx]
    tight_low = float(window["low"].tail(10).min()) if len(window) >= 10 else float(window["low"].min())
    base_high = float(window["high"].max())
    return {
        "pivot_price": pivot_price,
        "pivot_date": pivot_date,
        "tight_low": tight_low,
        "base_high": base_high,
    }


def is_vcp_ready(vcp_score: int, min_score: int) -> bool:
    return int(vcp_score) >= int(min_score)
