# -*- coding: utf-8 -*-
"""미국장 예산 관리 모듈.

한국장과 미국장은 각각 독립 예산 5천만원을 운용한다.

환경변수:
  US_PAPER_MAX_CAPITAL_KRW     : 미국장 최대 운용 한도 (원화)   기본 50,000,000
  US_EXPECTED_PRACTICE_CAPITAL_KRW: 연습 계좌 기준 자본  기본 50,000,000
  US_BUDGET_FX_KRW_PER_USD     : KRW/USD 환율          기본 1450
  US_MAX_CAPITAL_USD_AUTO      : 1이면 USD cap 자동 계산  기본 1

사용:
  from trader.us.budget import resolve_us_order_budget
  budget = resolve_us_order_budget(available_cash_usd=12000.0)
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 개별 getter
# ---------------------------------------------------------------------------

def get_us_capital_krw() -> float:
    """미국장 최대 운용 한도(원화)."""
    return float(os.getenv("US_PAPER_MAX_CAPITAL_KRW", "50000000"))


def get_us_budget_fx() -> float:
    """KRW/USD 환율."""
    return float(os.getenv("US_BUDGET_FX_KRW_PER_USD", "1450"))


def get_us_capital_usd_cap() -> float:
    """미국장 USD 예산 한도.

    US_PAPER_MAX_CAPITAL_KRW / US_BUDGET_FX_KRW_PER_USD
    """
    krw = get_us_capital_krw()
    fx = get_us_budget_fx()
    if fx <= 0:
        logger.warning("[US_BUDGET][WARN] fx_krw_per_usd=%s is invalid, using 1450", fx)
        fx = 1450.0
    return round(krw / fx, 2)


# ---------------------------------------------------------------------------
# 주문 예산 결정
# ---------------------------------------------------------------------------

def resolve_us_order_budget(available_cash_usd: float) -> dict:
    """미국장 실제 주문 가능 예산 계산.

    available_cash_usd 와 capital_usd_cap 중 작은 값을 effective_order_budget_usd로 반환.

    Args:
        available_cash_usd: KIS 해외주식 계좌에서 실제 조회한 주문 가능 USD

    Returns:
        {
            "capital_krw": 50000000,
            "fx_krw_per_usd": 1450,
            "capital_usd_cap": 34482.75,
            "available_cash_usd": <input>,
            "effective_order_budget_usd": min(available_cash_usd, capital_usd_cap),
        }
    """
    capital_krw = get_us_capital_krw()
    fx = get_us_budget_fx()
    capital_usd_cap = get_us_capital_usd_cap()

    effective = min(available_cash_usd, capital_usd_cap)

    result = {
        "capital_krw": capital_krw,
        "fx_krw_per_usd": fx,
        "capital_usd_cap": capital_usd_cap,
        "available_cash_usd": available_cash_usd,
        "effective_order_budget_usd": round(effective, 2),
    }

    logger.info(
        "[US_BUDGET][SUMMARY] capital_krw=%.0f fx=%.0f cap_usd=%.2f "
        "available=%.2f effective=%.2f",
        capital_krw,
        fx,
        capital_usd_cap,
        available_cash_usd,
        effective,
    )
    return result
