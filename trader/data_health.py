from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from trader.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)


def _short_error(err: Exception) -> str:
    text = str(err)
    if len(text) > 40:
        return text[:37] + "..."
    return text or err.__class__.__name__


def check_data_health(code: str, kis: Optional[KisAPI]) -> Dict[str, Any]:
    reasons: list[str] = []
    daily_n = 0
    intraday_n = 0
    prev_close = None
    vwap = None
    last_price = None

    if kis is None:
        return {
            "ok": False,
            "daily_n": daily_n,
            "intraday_n": intraday_n,
            "prev_close": prev_close,
            "vwap": vwap,
            "last_price": last_price,
            "reasons": ["API_ERROR:NO_CLIENT"],
        }

    try:
        daily = kis.safe_get_daily_candles(code)
        daily_n = len(daily)
        if daily_n < 21:
            reasons.append("DAILY_LT_21")
    except Exception as e:
        reasons.append(f"API_ERROR:{_short_error(e)}")

    try:
        prev_close = kis.safe_get_prev_close(code)
        if prev_close is None:
            reasons.append("PREV_CLOSE_NONE")
    except Exception as e:
        reasons.append(f"API_ERROR:{_short_error(e)}")

    bars: list[dict[str, Any]] | None = None
    try:
        bars = kis.safe_get_intraday_bars(code)
        intraday_n = len(bars)
        if intraday_n <= 0:
            reasons.append("INTRADAY_EMPTY")
    except Exception as e:
        reasons.append(f"API_ERROR:{_short_error(e)}")

    try:
        vwap = kis.safe_compute_vwap(bars or [])
        if vwap is None:
            reasons.append("VWAP_NONE")
    except Exception as e:
        reasons.append(f"API_ERROR:{_short_error(e)}")

    try:
        last_price = kis.get_last_price(code)
    except Exception as e:
        reasons.append(f"API_ERROR:{_short_error(e)}")
    if last_price is None:
        reasons.append("LAST_PRICE_NONE")

    ok = (
        daily_n >= 21
        and prev_close is not None
        and last_price is not None
        and vwap is not None
        and not any(str(r or "").startswith("API_ERROR:") for r in reasons)
    )

    return {
        "ok": ok,
        "daily_n": int(daily_n),
        "intraday_n": int(intraday_n),
        "prev_close": prev_close,
        "vwap": vwap,
        "last_price": last_price,
        "reasons": reasons,
    }
