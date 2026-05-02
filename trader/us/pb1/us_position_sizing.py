# -*- coding: utf-8 -*-
"""US PB1 Position Sizing.

한국장 PB1의 position sizing 개념을 USD 기반으로 이식.

원칙:
- 최대 포지션 수 제한
- 종목당 최대 포지션 비중 제한
- 현금 버퍼 유지
- 예산 cap 초과 금지
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def calc_position_size(
    price: float,
    available_cash_usd: float,
    capital_usd_cap: float,
    position_count: int,
    score: float = 0.5,
) -> dict:
    """포지션 크기 계산.

    Args:
        price: 현재 주가 (USD)
        available_cash_usd: 실제 주문 가능 잔고
        capital_usd_cap: 미국장 예산 cap (원화 한도를 USD로 변환한 값)
        position_count: 현재 보유 포지션 수
        score: 전략 점수 (0.0~1.0) — 점수 높을수록 더 많이 배분

    Returns:
        {
            "qty": int,
            "notional_usd": float,
            "position_weight": float,
            "blocked": bool,
            "reason": str,
        }
    """
    max_positions = int(os.getenv("US_MAX_POSITIONS", "10"))
    max_weight = float(os.getenv("US_MAX_POSITION_WEIGHT", "0.10"))
    cash_buffer_usd = float(os.getenv("US_MIN_CASH_BUFFER_USD", "200"))

    if position_count >= max_positions:
        return {"qty": 0, "notional_usd": 0.0, "position_weight": 0.0,
                "blocked": True, "reason": "max_positions_reached"}

    if price <= 0:
        return {"qty": 0, "notional_usd": 0.0, "position_weight": 0.0,
                "blocked": True, "reason": "invalid_price"}

    # 사용 가능한 실질 자금: cap과 잔고 중 작은 값 - 버퍼
    effective_cash = min(available_cash_usd, capital_usd_cap) - cash_buffer_usd
    if effective_cash <= 0:
        return {"qty": 0, "notional_usd": 0.0, "position_weight": 0.0,
                "blocked": True, "reason": "insufficient_cash"}

    # 점수 기반 배분: (점수 + 0.5) / 1.5 로 스케일링 → 0.33~1.0 범위
    score_factor = max(0.1, min(1.0, (score + 0.5) / 1.5))

    # 기준 배분: effective_cash * max_weight * score_factor
    reference_budget = capital_usd_cap if capital_usd_cap > 0 else effective_cash
    alloc_usd = reference_budget * max_weight * score_factor

    # 실제 잔고 초과 방지
    alloc_usd = min(alloc_usd, effective_cash)

    if alloc_usd < price:
        return {"qty": 0, "notional_usd": 0.0, "position_weight": 0.0,
                "blocked": True, "reason": "alloc_below_one_share"}

    qty = int(alloc_usd // price)
    if qty <= 0:
        return {"qty": 0, "notional_usd": 0.0, "position_weight": 0.0,
                "blocked": True, "reason": "qty_zero"}

    notional = qty * price
    position_weight = notional / reference_budget if reference_budget > 0 else 0.0

    logger.debug(
        "[US_SIZING] symbol=? price=%.2f qty=%d notional=%.2f weight=%.3f",
        price, qty, notional, position_weight,
    )

    return {
        "qty": qty,
        "notional_usd": round(notional, 4),
        "position_weight": round(position_weight, 4),
        "blocked": False,
        "reason": "ok",
    }
