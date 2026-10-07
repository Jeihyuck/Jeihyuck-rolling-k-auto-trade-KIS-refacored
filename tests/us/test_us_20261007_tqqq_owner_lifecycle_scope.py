"""TQQQ ownership: filtered PB1 universes must not close Infinite holdings."""
from datetime import date, datetime, timezone

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import InfiniteState, PositionSnapshot, Status, Action
from trader.us.infinite.strategy import evaluate
from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles

DATE = "2026-10-06"
NOW = datetime(2026, 10, 6, 20, 0, tzinfo=timezone.utc)


def _lc(owner, lifecycle):
    return {
        "strategy_owner": owner, "lifecycle_id": lifecycle,
        "is_open": True, "last_seen_qty": 5, "opened_trade_date": "2026-10-01",
    }


def test_pb1_owner_scoped_zero_close_does_not_touch_tqqq(monkeypatch):
    from trader.us import position_lifecycle_state as state
    changed = []
    monkeypatch.setattr(state, "load_latest_open_us_position_lifecycles",
                        lambda day: {
                            "TQQQ": _lc("TQQQ_INFINITE", "cycle-owned"),
                            "MSFT": _lc("US_STANDARD", "msft-open"),
                        })
    monkeypatch.setattr(state, "_save_lifecycle",
                        lambda symbol, td, lifecycle, **_kw:
                        (changed.append((symbol, dict(lifecycle))) or lifecycle))
    result = reconcile_us_position_lifecycles(
        positions=[], trade_date=DATE, now=NOW, authoritative=True,
        managed_owners={"US_STANDARD"},
    )
    assert [symbol for symbol, _lc_state in changed] == ["MSFT"]
    assert result["MSFT"]["is_open"] is False
    assert "TQQQ" not in result


def test_owner_scope_unknown_lifecycle_does_not_close_without_identity(monkeypatch):
    from trader.us import position_lifecycle_state as state
    changed = []
    monkeypatch.setattr(state, "load_latest_open_us_position_lifecycles",
                        lambda day: {"AAPL": {"is_open": True, "lifecycle_id": "legacy-no-owner"}})
    monkeypatch.setattr(state, "_save_lifecycle",
                        lambda symbol, td, lifecycle, **_kw: changed.append(symbol))
    result = reconcile_us_position_lifecycles(
        positions=[], trade_date=DATE, now=NOW, authoritative=True,
        managed_owners={"US_STANDARD"},
    )
    assert result == {}
    assert changed == []


def test_infinite_real_zero_still_transitions_to_complete():
    decision = evaluate(
        config=InfiniteConfig(),
        state=InfiniteState(cycle_id="real-broker-cycle", status=Status.EXIT_PENDING),
        position=PositionSnapshot(qty=0, orderable_qty=0, price=55),
        trading_date=date(2026, 10, 6),
        overlay={}, pending_sell=False,
    )
    assert decision.action == Action.WAIT
    assert decision.next_status == Status.COMPLETE


def test_infinite_partial_or_unknown_holdings_never_complete():
    from trader.us.infinite.models import Status
    decision = evaluate(
        config=InfiniteConfig(),
        state=InfiniteState(cycle_id="still-holding", status=Status.EXIT_PENDING),
        position=PositionSnapshot(qty=4, orderable_qty=0, average_price=50, price=55),
        trading_date=date(2026, 10, 6),
        overlay={}, pending_sell=True,
    )
    assert decision.next_status == Status.EXIT_PENDING


def test_owner_first_scope_excludes_dedicated_and_explicit_foreign_owner():
    from trader.us.strategy_ownership import is_standard_owned_position
    assert is_standard_owned_position({"symbol": "MSFT"})
    assert is_standard_owned_position({"symbol": "MSFT", "strategy_owner": "US_STANDARD"})
    assert not is_standard_owned_position({"symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE"})
    assert not is_standard_owned_position({"symbol": "TQQQ"})
    assert not is_standard_owned_position({"symbol": "MSFT", "strategy_owner": "UNKNOWN"})
    assert not is_standard_owned_position({"symbol": "AMD", "meta": {"strategy_owner": "OTHER"}})


def test_cluster_trim_uses_original_holding_owner_not_intent_missing_owner():
    from trader.us.strategy_ownership import filter_standard_owned_exit_intents
    positions = [
        {"symbol": "MSFT", "strategy_owner": "UNKNOWN"},
        {"symbol": "AMD", "strategy_owner": "US_STANDARD"},
        {"symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE"},
    ]
    # Intents have lost their originating owner's metadata in the generic
    # cluster guard. Never infer missing intent owner as US_STANDARD.
    intents = [
        {"symbol": "MSFT", "side": "SELL", "reason": "CLUSTER_EXPOSURE_TRIM"},
        {"symbol": "TQQQ", "side": "SELL", "reason": "CLUSTER_EXPOSURE_TRIM"},
        {"symbol": "AMD", "side": "SELL", "reason": "CLUSTER_EXPOSURE_TRIM"},
    ]
    assert [x["symbol"] for x in filter_standard_owned_exit_intents(intents, positions)] == ["AMD"]
