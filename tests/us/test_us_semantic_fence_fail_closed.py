from __future__ import annotations

import pytest

from trader.us.execution.order_router import same_day_semantic_sell_exists


def test_semantic_sell_ledger_lookup_failure_is_not_treated_as_no_prior_action(monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("ledger unavailable")

    monkeypatch.setattr("trader.us.db.repos.load_us_daily_orders_for_report", unavailable)
    intent = {
        "trade_date": "2026-10-02",
        "symbol": "AN_EXAMPLE_SYMBOL",
        "side": "SELL",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "cycle-1",
        "reason": "PROFIT_CAPTURE_TP2",
        "meta": {"reason": "PROFIT_CAPTURE_TP2"},
    }
    with pytest.raises(RuntimeError, match="ledger unavailable"):
        same_day_semantic_sell_exists(intent)
