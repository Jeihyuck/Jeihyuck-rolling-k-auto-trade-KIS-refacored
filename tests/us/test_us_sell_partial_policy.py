# -*- coding: utf-8 -*-
"""Phase 8: partial exit 차단 정책 검증.

- partial_exit_allowed=False(기본): 전략적 partial sell 금지
- orderable_qty < holding_qty: broker constraint → PARTIAL_DUE_TO_ORDERABLE_QTY_CLAMP
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import patch


def _intent(symbol: str, qty: int, partial_exit_allowed: bool | None = None) -> dict:
    i = {"symbol": symbol, "qty": qty, "side": "SELL"}
    if partial_exit_allowed is not None:
        i["partial_exit_allowed"] = partial_exit_allowed
    return i


def _position(holding: int, orderable: int) -> dict:
    return {
        "symbol": "TEST",
        "qty": holding,
        "holding_qty": holding,
        "orderable_qty": orderable,
    }


def test_partial_allowed_false_forces_full_exit():
    """partial_exit_allowed=False이면 intent_qty 관계없이 orderable_qty 전량 매도."""
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    intent = _intent("CRDO", qty=5, partial_exit_allowed=False)  # partial sell 시도
    position = _position(holding=10, orderable=10)

    with patch.dict(os.environ, {"US_SELL_PARTIAL_ALLOWED": "0"}):
        sell_qty, meta = resolve_sell_qty(intent, position)

    # full exit = 10 (holding_qty)
    assert sell_qty == 10, f"full exit 기대: sell_qty={sell_qty}"
    assert meta["strategic_partial"] is False


def test_partial_allowed_false_broker_clamp():
    """orderable_qty < holding_qty → broker constraint, PARTIAL_DUE_TO_ORDERABLE_QTY_CLAMP."""
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    intent = _intent("CRDO", qty=10, partial_exit_allowed=False)
    position = _position(holding=10, orderable=7)  # broker 제약: 7만 매도 가능

    with patch.dict(os.environ, {"US_SELL_PARTIAL_ALLOWED": "0"}):
        sell_qty, meta = resolve_sell_qty(intent, position)

    assert sell_qty == 7, f"broker constraint clamp: sell_qty={sell_qty}"
    assert meta["broker_constrained"] is True
    assert meta["strategic_partial"] is False
    assert "clamp" in meta["reason"].lower() or "broker" in meta["reason"].lower()


def test_partial_allowed_true_intent_qty_honored():
    """partial_exit_allowed=True이면 intent_qty 그대로 처리."""
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    intent = _intent("CRDO", qty=3, partial_exit_allowed=True)  # 3주만 매도
    position = _position(holding=10, orderable=10)

    sell_qty, meta = resolve_sell_qty(intent, position)

    assert sell_qty == 3
    assert meta["strategic_partial"] is True


def test_partial_allowed_env_false():
    """US_SELL_PARTIAL_ALLOWED=0 환경변수 → intent에 partial_exit_allowed 없으면 전량 매도."""
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    intent = _intent("CRDO", qty=3)  # partial_exit_allowed 명시 없음
    position = _position(holding=10, orderable=10)

    with patch.dict(os.environ, {"US_SELL_PARTIAL_ALLOWED": "0"}):
        sell_qty, meta = resolve_sell_qty(intent, position)

    # 환경변수 기본값 False → full exit
    assert sell_qty == 10


def test_no_orderable_qty_returns_zero():
    """orderable_qty=0이면 sell_qty=0."""
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    intent = _intent("CRDO", qty=5)
    position = _position(holding=5, orderable=0)

    sell_qty, meta = resolve_sell_qty(intent, position)

    assert sell_qty == 0
    assert meta["reason"] == "no_orderable_qty"


def test_no_position_returns_intent_qty():
    """position=None이면 intent_qty 그대로 반환."""
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    intent = _intent("CRDO", qty=5)
    sell_qty, meta = resolve_sell_qty(intent, None)

    assert sell_qty == 5
