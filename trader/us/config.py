# -*- coding: utf-8 -*-
"""미국주식 환경변수 설정 모듈.

모든 US agent 관련 설정은 여기서 읽는다.
국내 trader.config와 섞지 않는다.
"""
from __future__ import annotations

import os
from typing import Any

from trader.utils.env import env_bool, env_str


# ---------------------------------------------------------------------------
# Region / Safety
# ---------------------------------------------------------------------------
TRADING_REGION: str = os.getenv("TRADING_REGION", "US")
US_AGENT_ENABLED: bool = env_bool("US_AGENT_ENABLED", default=False)
KIS_ENV: str = os.getenv("KIS_ENV", "practice").lower()

# Paper trading switches
US_PAPER_TRADING_ENABLED: bool = env_bool("US_PAPER_TRADING_ENABLED", default=False)
US_LIVE_TRADING_ENABLED: bool = env_bool("US_LIVE_TRADING_ENABLED", default=False)
DISABLE_REAL_TRADING: bool = env_bool("DISABLE_REAL_TRADING", default=True)
DRY_RUN: bool = env_bool("DRY_RUN", default=True)

# ---------------------------------------------------------------------------
# KIS Auth (공용 또는 US 전용)
# ---------------------------------------------------------------------------
KIS_APP_KEY: str = os.getenv("KIS_US_APP_KEY") or os.getenv("KIS_APP_KEY", "")
KIS_APP_SECRET: str = os.getenv("KIS_US_APP_SECRET") or os.getenv("KIS_APP_SECRET", "")
KIS_REST_URL: str = (
    os.getenv("KIS_US_REST_URL")
    or os.getenv("KIS_REST_URL", "https://openapivts.koreainvestment.com:29443")
)
CANO: str = os.getenv("CANO_US") or os.getenv("CANO", "")
ACNT_PRDT_CD: str = os.getenv("ACNT_PRDT_CD_US") or os.getenv("ACNT_PRDT_CD", "01")

# ---------------------------------------------------------------------------
# Risk limits
# ---------------------------------------------------------------------------
US_MAX_ORDER_USD: float = float(os.getenv("US_MAX_ORDER_USD", "100"))
US_MAX_DAILY_NOTIONAL_USD: float = float(os.getenv("US_MAX_DAILY_NOTIONAL_USD", "500"))
# 한국장 PB1 기준 반영: 기본값 30 (10에서 변경), 0이면 무제한
US_MAX_POSITIONS: int = int(os.getenv("US_MAX_POSITIONS", "30"))
US_MAX_POSITION_WEIGHT: float = float(os.getenv("US_MAX_POSITION_WEIGHT", "0.10"))
US_MIN_CASH_BUFFER_USD: float = float(os.getenv("US_MIN_CASH_BUFFER_USD", "50"))
US_ALLOW_FRACTIONAL_SHARES: bool = env_bool("US_ALLOW_FRACTIONAL_SHARES", default=False)
US_ORDER_TYPE_DEFAULT: str = os.getenv("US_ORDER_TYPE_DEFAULT", "LIMIT")
US_LIMIT_PRICE_BAND_PCT: float = float(os.getenv("US_LIMIT_PRICE_BAND_PCT", "0.30"))

# ---------------------------------------------------------------------------
# HTTP policy
# ---------------------------------------------------------------------------
KIS_HTTP_ENABLED: str = os.getenv("KIS_HTTP_ENABLED", "AUTO")
US_KIS_HTTP_ENABLED: str = os.getenv("US_KIS_HTTP_ENABLED", "AUTO")
ALLOW_US_KIS_DATA_HTTP_IN_DIAG: bool = env_bool("ALLOW_US_KIS_DATA_HTTP_IN_DIAG", default=False)


def is_paper_order_allowed() -> bool:
    """True if all paper-trading guard conditions are met."""
    return (
        TRADING_REGION == "US"
        and KIS_ENV == "practice"
        and US_PAPER_TRADING_ENABLED
        and US_LIVE_TRADING_ENABLED
        and not DISABLE_REAL_TRADING
        and not DRY_RUN
    )


def assert_us_paper_order_allowed() -> None:
    """모든 US 주문 함수 앞에 반드시 호출해야 하는 guard."""
    if TRADING_REGION != "US":
        raise RuntimeError("[US_ORDER][BLOCKED] reason=trading_region_not_us")
    if KIS_ENV != "practice":
        raise RuntimeError("[US_ORDER][BLOCKED] reason=kis_env_not_practice")
    if not US_PAPER_TRADING_ENABLED:
        raise RuntimeError("[US_ORDER][BLOCKED] reason=paper_trading_not_enabled")
    if not US_LIVE_TRADING_ENABLED:
        raise RuntimeError("[US_ORDER][BLOCKED] reason=us_live_trading_disabled required=US_LIVE_TRADING_ENABLED=1")
    if DISABLE_REAL_TRADING:
        raise RuntimeError("[US_ORDER][BLOCKED] reason=disable_real_trading_enabled required=DISABLE_REAL_TRADING=0")
    if DRY_RUN:
        raise RuntimeError("[US_ORDER][BLOCKED] reason=dry_run_enabled required=DRY_RUN=0")


def get_summary() -> dict[str, Any]:
    """현재 US 설정 요약 (디버깅/로깅용)."""
    return {
        "TRADING_REGION": TRADING_REGION,
        "US_AGENT_ENABLED": US_AGENT_ENABLED,
        "KIS_ENV": KIS_ENV,
        "US_PAPER_TRADING_ENABLED": US_PAPER_TRADING_ENABLED,
        "US_LIVE_TRADING_ENABLED": US_LIVE_TRADING_ENABLED,
        "DISABLE_REAL_TRADING": DISABLE_REAL_TRADING,
        "DRY_RUN": DRY_RUN,
        "US_MAX_ORDER_USD": US_MAX_ORDER_USD,
        "US_MAX_DAILY_NOTIONAL_USD": US_MAX_DAILY_NOTIONAL_USD,
        "US_MAX_POSITIONS": US_MAX_POSITIONS,
        "is_paper_order_allowed": is_paper_order_allowed(),
    }
