from __future__ import annotations

import logging
from typing import Any

from trader.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)

_TRADE_BLOCK_KEYS = (
    "trht_yn",
    "trht",
    "trading_halt",
    "trade_halt",
    "trade_stop_yn",
    "halted",
)


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"y", "yes", "true", "1"}


def _extract_price(quote: dict) -> float:
    raw = quote.get("raw") if isinstance(quote, dict) else None
    price = (
        quote.get("prpr")
        or quote.get("stck_prpr")
        or quote.get("last")
        or (raw.get("stck_prpr") if isinstance(raw, dict) else None)
    )
    try:
        return float(price)
    except Exception:
        return 0.0


def validate_tradeable(kis: KisAPI, code: str) -> tuple[bool, str]:
    if not code:
        return False, "missing_code"
    code = str(code).zfill(6)
    try:
        quote = kis.get_quote_safe(code, diag_mode=True)
    except Exception as exc:  # pragma: no cover - external failure
        return False, f"quote_fail:{exc}"
    if not isinstance(quote, dict):
        return False, "quote_missing"
    rt_cd = quote.get("rt_cd")
    if rt_cd is not None and str(rt_cd) != "0":
        return False, f"rt_cd_{rt_cd}"
    raw = quote.get("raw") if isinstance(quote, dict) else None
    for key in _TRADE_BLOCK_KEYS:
        if _is_truthy(quote.get(key)) or (isinstance(raw, dict) and _is_truthy(raw.get(key))):
            return False, f"halted:{key}"
    if _extract_price(quote) <= 0:
        return False, "price_unavailable"
    return True, "ok"


def validate_listed_and_tradeable(kis: KisAPI, code: str) -> tuple[bool, str]:
    return validate_tradeable(kis, code)
