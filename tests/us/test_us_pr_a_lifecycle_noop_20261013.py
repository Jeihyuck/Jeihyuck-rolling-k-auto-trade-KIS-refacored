"""PR-A real lifecycle reconciliation: unchanged same-day rows never rewrite DB."""
from datetime import datetime, timedelta, timezone

from trader.us.db import repos
from trader.us import position_lifecycle_state as lifecycle


IDENTITY = {
    "env": "practice",
    "account_id": "practice:test-noop-account",
    "trading_epoch_id": "epoch-noop",
    "strategy_owner": "US_STANDARD",
}
DATE = "2026-10-12"


def _position(qty=3):
    return {
        "symbol": "SAMPLE",
        "qty": qty,
        "entry_price": 100.0,
        "current_price_usd": 105.0,
        **IDENTITY,
    }


def _setup(monkeypatch):
    repos.reset_memory_stores()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(lifecycle, "_identity_is_current", lambda identity: True)
    actual = repos.save_us_position_lifecycle_atomic
    recorded = []

    def persist(symbol, trade_date, value):
        recorded.append((symbol, trade_date, dict(value)))
        return actual(symbol, trade_date, value)

    monkeypatch.setattr(repos, "save_us_position_lifecycle_atomic", persist)
    return recorded


def _reconcile(positions, *, trade_date=DATE, at=None):
    return lifecycle.reconcile_us_position_lifecycles(
        positions=positions,
        trade_date=trade_date,
        now=at or datetime(2026, 10, 12, 14, 0, tzinfo=timezone.utc),
        authoritative=False, env="practice", managed_owners=None,
    )


def test_repeated_same_day_unchanged_broker_holding_does_not_write_again(monkeypatch):
    writes = _setup(monkeypatch)
    first = _reconcile([_position()])["SAMPLE"]
    assert len(writes) == 1
    second = _reconcile(
        [_position()], at=datetime(2026, 10, 12, 14, 5, tzinfo=timezone.utc)
    )["SAMPLE"]
    assert first == second
    assert len(writes) == 1


def test_actual_broker_qty_change_still_goes_through_atomic_persistence(monkeypatch):
    writes = _setup(monkeypatch)
    first = _reconcile([_position()])["SAMPLE"]
    second = _reconcile([_position(qty=2)])["SAMPLE"]
    assert len(writes) == 2
    assert second["last_seen_qty"] == 2
    assert second["lifecycle_id"] == first["lifecycle_id"]


def test_previous_day_lifecycle_is_carried_forward_and_counted_once(monkeypatch):
    writes = _setup(monkeypatch)
    first = _reconcile([_position()])["SAMPLE"]
    assert len(writes) == 1
    next_date = "2026-10-13"
    second = _reconcile(
        [_position()], trade_date=next_date,
        at=datetime(2026, 10, 13, 14, 0, tzinfo=timezone.utc)
    )["SAMPLE"]
    assert len(writes) == 2
    assert second["lifecycle_id"] == first["lifecycle_id"]
    assert second["holding_trade_days"] == first["holding_trade_days"] + 1
    third = _reconcile(
        [_position()], trade_date=next_date,
        at=datetime(2026, 10, 13, 14, 10, tzinfo=timezone.utc)
    )["SAMPLE"]
    assert len(writes) == 2
    assert third["holding_trade_days"] == second["holding_trade_days"]


def test_actual_scale_twenty_one_and_stress_forty_two_holdings_do_not_rewrite(monkeypatch):
    # The same owner/epoch contract is evaluated across broker-sized holdings.
    for count in (21, 42):
        writes = _setup(monkeypatch)
        positions = [
            {"symbol": f"SYM{i:02d}", "qty": 1, "entry_price": 100.0,
             "current_price_usd": 110.0, **IDENTITY}
            for i in range(count)
        ]
        first = _reconcile([dict(pos) for pos in positions])
        assert len(first) == count
        assert len(writes) == count
        second = _reconcile([dict(pos) for pos in positions])
        assert len(second) == count
        assert len(writes) == count
        assert first == second
