# -*- coding: utf-8 -*-
"""US PB1 Signal Schema.

한국장 PB1의 신호 스키마를 미국장용으로 이식.
KRW → USD, KST → America/New_York, code 6자리 → ticker 문자열.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class USPb1Signal:
    """미국장 PB1 진입 신호."""

    # 기본 식별
    symbol: str
    exchange: str  # NASDAQ | NYSE | AMEX

    # 가격
    price: float
    close: float = 0.0

    # 점수
    score: float = 0.0
    rank: int = 0

    # 진입 스타일
    entry_style: str = "momentum"  # momentum | pullback | breakout

    # 추세 지표
    ma20: float = 0.0
    ma50: float = 0.0
    above_ma20: bool = False
    above_ma50: bool = False

    # 모멘텀
    momentum_20d: float = 0.0
    momentum_50d: float = 0.0

    # 거래량
    volume_ratio: float = 1.0  # 오늘 거래량 / 평균 거래량

    # 눌림목
    pullback_pct: float = 0.0  # 최근 고점 대비 조정 비율

    # 기타
    strategy_tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "price": self.price,
            "close": self.close,
            "score": self.score,
            "rank": self.rank,
            "entry_style": self.entry_style,
            "ma20": self.ma20,
            "ma50": self.ma50,
            "above_ma20": self.above_ma20,
            "above_ma50": self.above_ma50,
            "momentum_20d": self.momentum_20d,
            "momentum_50d": self.momentum_50d,
            "volume_ratio": self.volume_ratio,
            "pullback_pct": self.pullback_pct,
            "strategy_tags": self.strategy_tags,
            "meta": self.meta,
        }


@dataclass
class USPb1ExitSignal:
    """미국장 PB1 청산 신호."""

    symbol: str
    exchange: str
    exit_type: str  # hard_stop | trailing_stop | profit_protect | giveback | time_stop | risk_off

    current_price: float
    entry_price: float
    qty: int

    unrealized_pnl_usd: float = 0.0
    unrealized_pnl_pct: float = 0.0

    reason: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "exit_type": self.exit_type,
            "current_price": self.current_price,
            "entry_price": self.entry_price,
            "qty": self.qty,
            "unrealized_pnl_usd": self.unrealized_pnl_usd,
            "unrealized_pnl_pct": self.unrealized_pnl_pct,
            "reason": self.reason,
            "meta": self.meta,
        }
