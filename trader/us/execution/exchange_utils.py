# -*- coding: utf-8 -*-
"""Pure exchange-routing helpers for US order handling."""
from __future__ import annotations

from typing import Any


def normalize_exchange_code(exchange: str) -> str:
    ex = str(exchange or "").upper().strip()
    aliases = {
        "NAS": "NASDAQ",
        "NASD": "NASDAQ",
        "NASDAQ": "NASDAQ",
        "NYS": "NYSE",
        "NYSE": "NYSE",
        "AMS": "AMEX",
        "AMEX": "AMEX",
        "ASE": "AMEX",
    }
    return aliases.get(ex, ex)


def lookup_nested_exchange(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    for key in ("exchange", "exch", "market", "ovrs_excg_cd", "tr_mket_name"):
        val = normalize_exchange_code(obj.get(key))
        if val:
            return val
    return ""
