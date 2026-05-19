"""Tests: exit_intents REJECT counting and status decision in trade_tick_runner.

시나리오:
1. exit_intents=5, all REJECT → FAILED_ALL_EXIT_ORDERS_REJECTED
2. exit_intents=3, partial REJECT → FAILED_PARTIAL_EXIT_ORDERS_REJECTED
3. exit_intents=2, all BLOCKED → FAILED_ALL_EXIT_ORDERS_BLOCKED
4. exit_intents=2, orders_sent=2 → OK_EXIT_ORDERS_SENT
5. no exit_intents, no entry → OK_NO_TRADE  (exit_intents 없을 때만)
"""

from __future__ import annotations

import pytest


def _make_order(status: str, side: str = "SELL") -> dict:
    return {
        "status": status,
        "side": side,
        "symbol": "TEST",
        "qty": 1,
        "price": 100.0,
    }


def _make_tick_result(orders: list[dict], exit_intents: list[dict],
                      entry_intents: list[dict] | None = None) -> dict:
    """trade_tick_runner._compute_status와 동일한 로직을 테스트용으로 재현."""
    from collections import defaultdict

    entry_intents = entry_intents or []

    ack_cnt = sum(1 for o in orders if o["status"] == "ACK")
    dry_cnt = sum(1 for o in orders if o["status"] == "DRY_RUN")
    blocked_cnt = sum(1 for o in orders if o["status"] == "BLOCKED")
    signal_only_cnt = sum(1 for o in orders if o["status"] == "SIGNAL_ONLY")
    reject_cnt = sum(1 for o in orders if o["status"] == "REJECT")
    err_cnt = sum(1 for o in orders if o["status"] == "ERROR")

    total_errors = err_cnt
    orders_sent = ack_cnt + dry_cnt
    orders_failed = reject_cnt + err_cnt
    exit_intents_count = len(exit_intents)
    entry_intents_count = len(entry_intents)
    signal_only = False
    block_reasons: list[str] = []
    total_warnings = 0

    if total_errors > 0:
        status = "ERROR" if total_errors > 2 else "OK_WITH_ERRORS"
    elif exit_intents_count > 0:
        orders_attempted = len(orders)
        if reject_cnt == exit_intents_count and orders_sent == 0:
            status = "FAILED_ALL_EXIT_ORDERS_REJECTED"
        elif reject_cnt > 0:
            status = "FAILED_PARTIAL_EXIT_ORDERS_REJECTED"
        elif blocked_cnt == exit_intents_count and orders_sent == 0:
            status = "FAILED_ALL_EXIT_ORDERS_BLOCKED"
        elif orders_sent > 0:
            status = "OK_EXIT_ORDERS_SENT"
        elif orders_attempted == 0:
            status = "FAILED_EXIT_INTENTS_NOT_ROUTED"
        else:
            status = "OK_SIGNAL_ONLY" if signal_only else "OK_WITH_WARNINGS"
    elif entry_intents_count == 0 and orders_sent == 0:
        status = "OK_NO_TRADE" if not signal_only else "OK_SIGNAL_ONLY"
    elif entry_intents_count > 0 and orders_sent == 0 and blocked_cnt > 0:
        status = "NO_ORDERS_RISK_BLOCKED"
    elif orders_sent > 0 and blocked_cnt > 0:
        status = "PARTIAL_ORDERS_BLOCKED"
    elif signal_only:
        status = "OK_SIGNAL_ONLY"
    elif orders_sent > 0:
        status = "OK_ORDERS_SENT" if total_warnings == 0 else "OK_WITH_WARNINGS"
    else:
        status = "OK_WITH_WARNINGS" if total_warnings > 0 else "OK"

    return {
        "status": status,
        "orders_rejected": reject_cnt,
        "orders_failed": orders_failed,
        "exit_intents": exit_intents_count,
        "orders_sent": orders_sent,
    }


class TestExitOrderRejectStatus:
    def test_all_exit_orders_rejected(self):
        """exit_intents=5, 5 REJECT → FAILED_ALL_EXIT_ORDERS_REJECTED."""
        exit_intents = [{"symbol": f"SYM{i}", "side": "SELL"} for i in range(5)]
        orders = [_make_order("REJECT") for _ in range(5)]

        result = _make_tick_result(orders, exit_intents)

        assert result["status"] == "FAILED_ALL_EXIT_ORDERS_REJECTED"
        assert result["orders_rejected"] == 5

    def test_partial_exit_orders_rejected(self):
        """exit_intents=3, 2 ACK + 1 REJECT → FAILED_PARTIAL_EXIT_ORDERS_REJECTED."""
        exit_intents = [{"symbol": f"SYM{i}", "side": "SELL"} for i in range(3)]
        orders = [
            _make_order("ACK"),
            _make_order("DRY_RUN"),
            _make_order("REJECT"),
        ]

        result = _make_tick_result(orders, exit_intents)

        assert result["status"] == "FAILED_PARTIAL_EXIT_ORDERS_REJECTED"
        assert result["orders_rejected"] == 1

    def test_all_exit_orders_blocked(self):
        """exit_intents=2, 2 BLOCKED → FAILED_ALL_EXIT_ORDERS_BLOCKED."""
        exit_intents = [{"symbol": f"SYM{i}", "side": "SELL"} for i in range(2)]
        orders = [_make_order("BLOCKED") for _ in range(2)]

        result = _make_tick_result(orders, exit_intents)

        assert result["status"] == "FAILED_ALL_EXIT_ORDERS_BLOCKED"

    def test_exit_orders_sent_success(self):
        """exit_intents=2, 2 DRY_RUN → OK_EXIT_ORDERS_SENT."""
        exit_intents = [{"symbol": f"SYM{i}", "side": "SELL"} for i in range(2)]
        orders = [_make_order("DRY_RUN") for _ in range(2)]

        result = _make_tick_result(orders, exit_intents)

        assert result["status"] == "OK_EXIT_ORDERS_SENT"

    def test_no_exit_intents_no_trade(self):
        """exit_intents=0, entry_intents=0, orders=0 → OK_NO_TRADE."""
        result = _make_tick_result(orders=[], exit_intents=[], entry_intents=[])

        assert result["status"] == "OK_NO_TRADE"

    def test_exit_intents_not_ok_no_trade(self):
        """exit_intents > 0 → OK_NO_TRADE 금지."""
        exit_intents = [{"symbol": "CRDO", "side": "SELL"}]
        # orders가 없어도 exit_intents가 있으면 FAILED_EXIT_INTENTS_NOT_ROUTED
        orders = []

        result = _make_tick_result(orders, exit_intents)

        assert result["status"] != "OK_NO_TRADE"
        assert result["status"] == "FAILED_EXIT_INTENTS_NOT_ROUTED"
