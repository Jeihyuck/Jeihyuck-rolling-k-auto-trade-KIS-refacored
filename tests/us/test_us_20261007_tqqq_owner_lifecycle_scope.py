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


def test_infinite_dedicated_runner_persists_complete_when_broker_zero(monkeypatch):
    """A genuine fully-liquidated Infinite cycle still terminates in its own owner."""
    from trader.us.infinite.integration import run_sleeve
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    class Repo:
        def __init__(self):
            self.state = InfiniteState(
                cycle_id="completed-cycle", status=Status.EXIT_PENDING,
                core_filled_notional=500,
            )
            self.saved_statuses = []
        def ensure_schema(self): pass
        def reconcile_metadata(self, state, **kwargs): return state
        def load_state(self, **kwargs): return self.state
        def save_state(self, state):
            self.state = state
            self.saved_statuses.append(state.status)
        def pending_sides(self, *_args): return False, False
        def pending_buy_notional(self, *_args): return 0
        def fill_accounting(self, *_args): return 500, 0, 0, None, 50
    repo = Repo()
    broker_routes = []
    result = run_sleeve(
        positions=[], price=55, trading_date=date(2026, 10, 6),
        overlay={"market_state": "NORMAL", "market_regime": "NEUTRAL"},
        repository=repo, route=lambda intent: broker_routes.append(intent),
    )
    assert repo.state.status == Status.COMPLETE, result
    assert repo.saved_statuses[-1] == Status.COMPLETE
    assert broker_routes == []


def test_scope_does_not_close_nonzero_unknown_owner_while_pb1_owner_differs(monkeypatch):
    from trader.us import position_lifecycle_state as state
    saves = []
    monkeypatch.setattr(state, "load_latest_open_us_position_lifecycles",
                        lambda _day: {"MSFT": _lc("US_STANDARD", "msft-live")})
    monkeypatch.setattr(state, "_save_lifecycle",
                        lambda symbol, trade_date, lifecycle, **kw: saves.append(symbol))
    held_but_unattributed = {
        "symbol": "MSFT", "qty": 3, "strategy_owner": "UNKNOWN",
        "meta": {"strategy_owner": "UNKNOWN"},
    }
    result = reconcile_us_position_lifecycles(
        positions=[held_but_unattributed], trade_date=DATE,
        now=NOW, authoritative=True, managed_owners={"US_STANDARD"},
    )
    assert result == {}
    assert saves == []
    assert held_but_unattributed["qty"] == 3
