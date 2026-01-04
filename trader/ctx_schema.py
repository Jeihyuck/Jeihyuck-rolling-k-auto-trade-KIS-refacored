from __future__ import annotations

from typing import Any, Dict


def normalize_daily_ctx(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(payload or {})
    payload.setdefault("setup_flag", False)
    payload.setdefault("setup_ok", bool(payload.get("setup_flag")))
    for key in (
        "ma5",
        "ma10",
        "ma20",
        "atr",
        "recent_high_20",
        "pullback_depth_pct",
        "distance_to_peak",
        "ma20_ratio",
        "ma20_risk",
        "current_price",
        "peak_price",
        "prev_close",
    ):
        payload.setdefault(key, None)
    payload.setdefault("max_pullback_pct", None)
    payload.setdefault("strong_trend", False)
    return payload


def normalize_intraday_ctx(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(payload or {})
    payload.setdefault("vwap", None)
    payload.setdefault("below_vwap_ratio", None)
    payload.setdefault("prev_high_retest", False)
    payload.setdefault("range_break", False)
    payload.setdefault("vwap_reclaim", False)
    payload.setdefault("volume_spike", False)
    return payload
