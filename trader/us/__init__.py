# -*- coding: utf-8 -*-
"""trader.us — 미국주식 모의투자 Multi-Agent 패키지.

브랜치: us-agent (nullim 수정 금지)
목표: KIS 해외주식 모의투자 API 기반 미국주식 paper trading.
"""
from __future__ import annotations

import os

# KIS practice REST is account-wide and limited far more tightly than live.
# Leave enough shared tick budget for the final broker POST after any balance
# validation / rate-governor wait.  Operators may still override explicitly.
os.environ.setdefault("US_SELL_ROUTING_RESERVE_SEC", "3")
os.environ.setdefault("US_BALANCE_FETCH_RESERVE_SEC", "3")

from .kis_http_governor import install_kis_http_governor

install_kis_http_governor()
