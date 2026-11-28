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


def test_atomic_soft_stop_requires_durable_postgres(monkeypatch):
    from trader.us.db import repos
    import pytest
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    with pytest.raises(RuntimeError, match="US_SOFT_STOP_DURABLE_STATE_WRITE_FAILED"):
        repos.update_us_soft_stop_risk_state_atomic(
            symbol="GENERIC", trade_date="2026-06-29", pnl_pct=-.04,
            current_price=96, now=datetime(2026,6,29,10,5,tzinfo=NY), soft_stop_pct=.03)


def test_atomic_soft_stop_single_upsert_returns_committed_state(monkeypatch):
    from trader.us.db import repos
    from contextlib import contextmanager
    from types import SimpleNamespace
    queries = []
    class Connection:
        def execute(self, statement, params):
            queries.append((str(statement), params))
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: {
                "soft_stop_breach_count":2, "state":{"lifecycle":{"lifecycle_id":"original"},
                "soft_stop_execution":{"soft_stop_partial_done":True}},
                "lowest_price_since_breach":95.0,
            }))
    class Engine:
        dialect=SimpleNamespace(name="postgresql")
        @contextmanager
        def begin(self):
            yield Connection()
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:Engine())
    monkeypatch.setattr(repos,"_ensure_us_position_risk_state_table",lambda conn:None)
    monkeypatch.setattr(repos,"_active_us_epoch",lambda conn,required: "epoch")
    result=repos.update_us_soft_stop_risk_state_atomic(
        symbol="GENERIC",trade_date="2026-06-29",pnl_pct=-.04,current_price=96,
        now=datetime(2026,6,29,10,5,tzinfo=NY),soft_stop_pct=.03)
    assert len(queries)==1
    assert "ON CONFLICT" in queries[0][0] and "RETURNING *" in queries[0][0]
    assert "old.soft_stop_breach_count + 1" in queries[0][0]
    assert queries[0][1]["breached"] is True
    assert result["state"]["lifecycle"]["lifecycle_id"]=="original"
    assert result["state"]["soft_stop_execution"]["soft_stop_partial_done"] is True
    assert result["soft_stop_breach_count"]==2
