# -*- coding: utf-8 -*-
"""KR products that must not be newly bought by the overlay."""
from __future__ import annotations

FORBIDDEN_KR_BUY_SYMBOLS = {"252670", "114800", "251340", "122630"}


def is_forbidden_kr_buy_product(symbol: str | None) -> bool:
    return str(symbol or "").strip().upper() in FORBIDDEN_KR_BUY_SYMBOLS
