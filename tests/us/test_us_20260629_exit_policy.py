from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.us.pb1.us_exit_engine import evaluate_exit

NY = ZoneInfo("America/New_York")


def test_arm_open_whipsaw_soft_stop_not_full_sell(monkeypatch):
    monkeypatch.setenv("US_OPEN_VOL_GUARD_ENABLED", "1")
    pos = {
        "symbol": "ARM",
        "exchange": "NASDAQ",
        "qty": 10,
        "entry_price": 339.626,
        "max_price": 339.626,
        "soft_stop_breach_count": 1,
    }
    intent = evaluate_exit(pos, current_price=322.63, now=datetime(2026, 6, 29, 9, 55, tzinfo=NY))

    assert intent is not None
    assert intent["exit_type"] != "profit_trailing_stop"
    assert intent["side"] == "HOLD"
    assert intent["meta"]["full_sell"] is False
    assert "open_vol_guard" in intent["reason"] or "soft_stop_wait_confirm" in intent["reason"]


def test_hard_stop_not_blocked_by_open_guard(monkeypatch):
    monkeypatch.setenv("US_OPEN_VOL_GUARD_ENABLED", "1")
    pos = {"symbol": "ARM", "qty": 10, "entry_price": 339.626, "max_price": 339.626}
    intent = evaluate_exit(pos, current_price=339.626 * (1 - 0.085), now=datetime(2026, 6, 29, 9, 50, tzinfo=NY))

    assert intent is not None
    assert intent["exit_type"] == "hard_stop_loss"
    assert intent["qty"] == 10


def test_profit_trailing_stop_after_real_profit_experience(monkeypatch):
    monkeypatch.setenv("US_OPEN_VOL_GUARD_ENABLED", "0")
    pos = {"symbol": "NVDA", "qty": 10, "entry_price": 100.0, "max_price": 110.0}
    intent = evaluate_exit(pos, current_price=104.5, now=datetime(2026, 6, 29, 11, 0, tzinfo=NY))

    assert intent is not None
    assert intent["exit_type"] == "profit_trailing_stop"
    assert intent["qty"] <= 5


def test_loss_without_profit_experience_is_soft_stop_not_trailing(monkeypatch):
    monkeypatch.setenv("US_OPEN_VOL_GUARD_ENABLED", "0")
    pos = {"symbol": "QCOM", "qty": 10, "entry_price": 100.0, "max_price": 100.0, "soft_stop_breach_count": 3}
    intent = evaluate_exit(pos, current_price=95.0, now=datetime(2026, 6, 29, 11, 0, tzinfo=NY))

    assert intent is not None
    assert intent["exit_type"] == "soft_stop_loss"
    assert intent["exit_type"] != "profit_trailing_stop"
    assert intent["qty"] <= 5


def test_repeated_kis_fills_are_idempotent(monkeypatch):
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_FILLS.clear()
    fills = [
        {"symbol": "ARM", "side": "SELL", "qty": 10, "price_usd": 322.63, "order_no": "O1", "client_order_key": "C1"},
        {"symbol": "QCOM", "side": "SELL", "qty": 12, "price_usd": 188.38, "order_no": "O2", "client_order_key": "C2"},
        {"symbol": "INTC", "side": "BUY", "qty": 3, "price_usd": 127.53, "order_no": "O3", "client_order_key": "C3"},
    ]

    assert repos.save_fills(fills, trade_date="2026-06-29") == 3
    for _ in range(10):
        assert repos.save_fills(fills, trade_date="2026-06-29") == 0
    assert len(repos._MEM_FILLS) == 3
