from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from trader.kis_wrapper import KisAPI
from trader.config import KST

logger = logging.getLogger(__name__)


def _short_error(err: Exception) -> str:
    text = str(err)
    if len(text) > 40:
        return text[:37] + "..."
    return text or err.__class__.__name__


def check_data_health(code: str, kis: Optional[KisAPI]) -> Dict[str, Any]:
    ts = datetime.now(KST).isoformat()
    reasons: list[str] = []
    daily_len: int | None = None
    intraday_len: int | None = None
    prev_close = None
    vwap = None
    last_price = None

    if kis is None:
        reasons.append("API_ERROR:NO_CLIENT")
    else:
        try:
            daily = kis.safe_get_daily_candles(code)
            daily_len = len(daily)
            if daily_len < 21:
                reasons.append("DAILY_LT_21")
        except Exception as e:
            reasons.append(f"DAILY_FETCH_FAIL:{_short_error(e)}")

        try:
            prev_close = kis.safe_get_prev_close(code)
            if prev_close is None:
                reasons.append("PREV_CLOSE_MISSING")
        except Exception as e:
            reasons.append(f"PREV_CLOSE_FAIL:{_short_error(e)}")

        bars: list[dict[str, Any]] | None = None
        try:
            bars = kis.safe_get_intraday_bars(code)
            intraday_len = len(bars)
            if intraday_len <= 0:
                reasons.append("INTRADAY_FETCH_EMPTY")
        except Exception as e:
            reasons.append(f"INTRADAY_FETCH_FAIL:{_short_error(e)}")

        try:
            vwap = kis.safe_compute_vwap(bars or [])
            if vwap is None:
                reasons.append("VWAP_UNAVAILABLE")
        except Exception as e:
            reasons.append(f"VWAP_COMPUTE_FAIL:{_short_error(e)}")

        try:
            last_price = kis.get_last_price(code)
        except Exception as e:
            reasons.append(f"LAST_PRICE_FAIL:{_short_error(e)}")
        if last_price is None:
            reasons.append("LAST_PRICE_NONE")

    ok = (
        kis is not None
        and daily_len is not None
        and daily_len >= 21
        and prev_close is not None
        and vwap is not None
        and intraday_len is not None
        and intraday_len > 0
        and not any(str(r or "").startswith("API_ERROR:") for r in reasons)
    )
    if ok and not reasons:
        reasons.append("OK")
    if (not ok) and not reasons:
        reasons.append("UNKNOWN_DATA_HEALTH_FAIL")

    return {
        "ts": ts,
        "ok": ok,
        "daily_len": daily_len,
        "intraday_len": intraday_len,
        "prev_close": prev_close,
        "vwap": vwap,
        "last_price": last_price,
        "reasons": reasons,
        # legacy keys for compatibility
        "daily_n": daily_len if daily_len is not None else 0,
        "intraday_n": intraday_len if intraday_len is not None else 0,
    }
