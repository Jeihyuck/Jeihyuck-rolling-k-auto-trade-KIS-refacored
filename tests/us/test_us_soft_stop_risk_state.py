from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")


def test_soft_stop_breach_count_increments_across_ticks(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()

    s1 = repos.update_us_soft_stop_risk_state(
        symbol="GENERIC", trade_date="2026-06-29", pnl_pct=-0.051,
        current_price=94.9, now=datetime(2026, 6, 29, 10, 5, tzinfo=NY), soft_stop_pct=0.05,
    )
    s2 = repos.update_us_soft_stop_risk_state(
        symbol="GENERIC", trade_date="2026-06-29", pnl_pct=-0.053,
        current_price=94.7, now=datetime(2026, 6, 29, 10, 10, tzinfo=NY), soft_stop_pct=0.05,
    )

    assert s1["soft_stop_breach_count"] == 1
    assert s2["soft_stop_breach_count"] == 2
    assert s2["lowest_price_since_breach"] == 94.7


def test_soft_stop_breach_count_resets_after_recovery(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()

    repos.update_us_soft_stop_risk_state(
        symbol="GENERIC", trade_date="2026-06-29", pnl_pct=-0.051,
        current_price=94.9, now=datetime(2026, 6, 29, 10, 5, tzinfo=NY), soft_stop_pct=0.05,
    )
    recovered = repos.update_us_soft_stop_risk_state(
        symbol="GENERIC", trade_date="2026-06-29", pnl_pct=-0.040,
        current_price=96.0, now=datetime(2026, 6, 29, 10, 10, tzinfo=NY), soft_stop_pct=0.05,
    )

    assert recovered["soft_stop_breach_count"] == 0
    assert recovered["first_soft_stop_seen_at"] is None
    assert recovered["lowest_price_since_breach"] is None


class _PriceProvider:
    def __init__(self, price: float):
        self.price = price

    def get_current_price(self, symbol, exchange):
        return {"last": self.price}


def test_open_vol_guard_blocks_sell_but_keeps_breach_count(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_exit_engine import generate_exit_intents

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    monkeypatch.setenv("US_OPEN_VOL_GUARD_ENABLED", "1")

    positions = [{"symbol": "GENERIC", "exchange": "NASDAQ", "qty": 10, "entry_price": 100.0, "max_price": 100.0}]
    intents = generate_exit_intents(positions, _PriceProvider(94.8), now=datetime(2026, 6, 29, 9, 55, tzinfo=NY))

    assert intents == []
    assert positions[0]["soft_stop_breach_count"] == 1
    state = repos.load_us_position_risk_state("GENERIC", "2026-06-29")
    assert state["soft_stop_breach_count"] == 1


def test_soft_stop_confirmed_triggers_partial_sell_after_required_ticks(monkeypatch):
    from trader.us.pb1.us_exit_engine import evaluate_exit

    monkeypatch.setenv("US_OPEN_VOL_GUARD_ENABLED", "0")
    monkeypatch.setenv("US_SOFT_STOP_CONFIRM_TICKS", "2")
    pos = {
        "symbol": "GENERIC",
        "exchange": "NASDAQ",
        "qty": 10,
        "entry_price": 100.0,
        "max_price": 100.0,
        "risk_state": {"soft_stop_breach_count": 2},
    }
    intent = evaluate_exit(pos, current_price=94.8, now=datetime(2026, 6, 29, 10, 5, tzinfo=NY))

    assert intent is not None
    assert intent["side"] == "SELL"
    assert intent["exit_type"] == "soft_stop_loss"
    assert intent["qty"] <= 5
