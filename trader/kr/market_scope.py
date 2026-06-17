# -*- coding: utf-8 -*-
from __future__ import annotations

import os

KR_MARKET_VALUES = {"KR", "KRX", "KOREA"}


def is_kr_market() -> bool:
    vals = {
        str(os.getenv("MARKET") or "").upper(),
        str(os.getenv("REGION") or "").upper(),
        str(os.getenv("TRADING_REGION") or "").upper(),
        str(os.getenv("EXCHANGE") or "").upper(),
        str(os.getenv("PB1_MARKET_SCOPE") or "").upper(),
        str(os.getenv("WSL_RUN_MARKET") or "").upper(),
    }
    return bool(vals & KR_MARKET_VALUES)
