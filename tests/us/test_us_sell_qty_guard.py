"""Tests: resolve_sell_qty() in us_sell_qty_guard.

세 가지 시나리오:
1. 정상 (sell_qty == intent_qty, no clamp)
2. 클램프 (intent_qty > orderable_qty → sell_qty = orderable_qty)
3. 차단 (orderable_qty == 0 → sell_qty = 0)
"""

from __future__ import annotations

import pytest

from trader.us.execution.us_sell_qty_guard import resolve_sell_qty


class TestResolveSellQty:
    def test_ok_no_clamp(self):
        """intent_qty ≤ orderable_qty → sell_qty == intent_qty, clamped=False."""
        intent = {"symbol": "CRDO", "qty": 5}
        position = {"holding_qty": 12, "orderable_qty": 12}

        sell_qty, meta = resolve_sell_qty(intent, position)

        assert sell_qty == 5
        assert meta["clamped"] is False
        assert meta["reason"] == "ok"

    def test_clamped_to_orderable(self):
        """intent_qty > orderable_qty → sell_qty = orderable_qty, clamped=True."""
        intent = {"symbol": "CRDO", "qty": 24}  # Bot 오류로 2배
        position = {"holding_qty": 12, "orderable_qty": 12}

        sell_qty, meta = resolve_sell_qty(intent, position)

        assert sell_qty == 12
        assert meta["clamped"] is True
        assert meta["reason"] == "sell_qty_clamped_to_orderable"

    def test_blocked_no_orderable(self):
        """orderable_qty == 0 → sell_qty = 0, reason = no_orderable_qty."""
        intent = {"symbol": "VRT", "qty": 6}
        position = {"holding_qty": 6, "orderable_qty": 0}

        sell_qty, meta = resolve_sell_qty(intent, position)

        assert sell_qty == 0
        assert meta["reason"] == "no_orderable_qty"

    def test_no_position_fallback(self):
        """Position이 None이어도 크래시 없이 intent_qty 그대로 반환."""
        intent = {"symbol": "SOXX", "qty": 4}

        sell_qty, meta = resolve_sell_qty(intent, position=None)

        assert sell_qty == 4
        assert meta["clamped"] is False

    def test_sellable_qty_used_when_orderable_missing(self):
        """orderable_qty 없으면 sellable_qty 사용."""
        intent = {"symbol": "LITE", "qty": 5}
        position = {"holding_qty": 2, "sellable_qty": 2}  # orderable_qty 없음

        sell_qty, meta = resolve_sell_qty(intent, position)

        assert sell_qty == 2
        assert meta["clamped"] is True

    def test_holding_qty_fallback(self):
        """orderable_qty, sellable_qty 모두 없으면 holding_qty 사용."""
        intent = {"symbol": "CRDO", "qty": 10}
        position = {"holding_qty": 12}  # orderable/sellable 없음

        sell_qty, meta = resolve_sell_qty(intent, position)

        assert sell_qty == 10  # intent_qty ≤ holding_qty → no clamp
        assert meta["clamped"] is False
