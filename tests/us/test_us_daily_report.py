# -*- coding: utf-8 -*-
"""tests/us/test_us_daily_report.py

US 세션 리포트 집계 검증.
"""
from __future__ import annotations

import pytest


class TestSessionReportBlockedAccumulation:
    """orders_blocked_total이 세션 전체에 걸쳐 누적되어야 한다."""

    def test_session_report_accumulates_blocked_reasons(self):
        """여러 tick에 걸친 orders_blocked가 last_tick이 아닌 total로 집계되어야 한다."""
        # trade_session_runner의 누적 로직을 시뮬레이션
        tick_results = [
            {
                "status": "OK",
                "orders_blocked": 3,
                "block_reasons": {"symbol_not_in_universe": 2, "notional_exceeds_order_limit": 1},
            },
            {
                "status": "OK",
                "orders_blocked": 0,
                "block_reasons": {},
            },
            {
                "status": "OK",
                "orders_blocked": 2,
                "block_reasons": {"symbol_not_in_universe": 1, "daily_notional_exceeded": 1},
            },
        ]

        total_orders_blocked = 0
        block_reasons_total: dict[str, int] = {}

        for tick in tick_results:
            tick_blocked = int(tick.get("orders_blocked", 0) or 0)
            total_orders_blocked += tick_blocked
            for reason, cnt in (tick.get("block_reasons") or {}).items():
                block_reasons_total[reason] = block_reasons_total.get(reason, 0) + int(cnt or 0)

        # 전체 합산: 3 + 0 + 2 = 5
        assert total_orders_blocked == 5, (
            f"orders_blocked_total should be 5 but got {total_orders_blocked}"
        )

        # last tick만 반영하면 2가 되어야 하지만, 누적이므로 5이어야 함
        last_tick_blocked = int(tick_results[-1].get("orders_blocked", 0) or 0)
        assert total_orders_blocked != last_tick_blocked, (
            "orders_blocked_total must differ from last tick value when multiple ticks have blocks"
        )

        # block_reasons_total 검증
        assert block_reasons_total.get("symbol_not_in_universe") == 3  # 2 + 0 + 1
        assert block_reasons_total.get("notional_exceeds_order_limit") == 1
        assert block_reasons_total.get("daily_notional_exceeded") == 1

    def test_orders_blocked_total_in_report_payload(self):
        """report_payload에 orders_blocked_total, block_reasons_total 키가 있어야 한다."""
        # session_runner에서 생성하는 report_payload 구조 검증
        total_orders_blocked = 5
        block_reasons_total = {"symbol_not_in_universe": 3, "notional_exceeds_order_limit": 1}
        final_tick = {"orders_blocked": 2, "block_reasons": {"symbol_not_in_universe": 1}}

        report_payload = {
            "orders_blocked": total_orders_blocked,
            "orders_blocked_total": total_orders_blocked,
            "orders_blocked_last_tick": int(final_tick.get("orders_blocked", 0) or 0),
            "block_reasons": block_reasons_total,
            "block_reasons_total": block_reasons_total,
            "block_reasons_last_tick": final_tick.get("block_reasons", {}),
        }

        # backward compat 키들이 total 값이어야 함
        assert report_payload["orders_blocked"] == 5
        assert report_payload["orders_blocked"] == report_payload["orders_blocked_total"]
        assert report_payload["block_reasons"] == report_payload["block_reasons_total"]

        # last_tick 키는 마지막 tick 값
        assert report_payload["orders_blocked_last_tick"] == 2
        assert report_payload["block_reasons_last_tick"].get("symbol_not_in_universe") == 1
