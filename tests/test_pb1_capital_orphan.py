import pytest

from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision
from trader.ledger.store import LedgerStore


class DummyLedgerRepo:
    def __init__(self):
        self.events = []

    def append_event(self, **kwargs):
        self.events.append(kwargs)
        return "evt"


class DummyKis:
    def __init__(self, snapshot, refreshed_snapshot=None, orderable=0):
        self.snapshot = snapshot
        self.refreshed_snapshot = refreshed_snapshot or snapshot
        self.orderable = orderable
        self.calls = []

    def get_balance_cached(self, force=False, return_source=False):
        self.calls.append(force)
        data = self.refreshed_snapshot if force else self.snapshot
        if return_source:
            return data, "api"
        return data

    def get_orderable_cash(self, code_hint=None, price_hint=None):
        return self.orderable, {}


def _make_engine(*, kis=None, ledger_repo=None, env="practice"):
    return PB1Engine(
        universe_repo=object(),
        orders_repo=object(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=ledger_repo or DummyLedgerRepo(),
        kis=kis,
        window=WindowDecision(name="morning", phase="trade"),
        window_label="morning",
        phase="trade",
        dry_run=True,
        env=env,
        run_id="test",
    )


def test_entry_capital_ignores_zero_override():
    engine = _make_engine()
    entry_capital, usable, meta = engine._resolve_entry_capital(
        available_cash_krw=10_000_000,
        override_capital=0.0,
        reserve_pct=0.1,
    )

    assert entry_capital == 10_000_000
    assert usable == 9_000_000
    assert meta["use_override"] is False


def test_balance_parse_refreshes_once_on_failure():
    snapshot = {"output2": [{}], "output1": []}
    refreshed = {"output2": [{"dnca_tot_amt": "10000000"}], "output1": []}
    kis = DummyKis(snapshot, refreshed_snapshot=refreshed, orderable=0)
    engine = _make_engine(kis=kis)
    initial = {"output2": [{}], "output1": []}

    refreshed, cash, meta = engine._resolve_holdings_snapshot_with_cash(initial)

    assert cash == 10_000_000
    assert meta["source"] == "balance_dnca_tot_amt"
    assert refreshed == initial
    assert kis.calls == [False, True]


def test_balance_parse_raises_after_failed_refresh():
    snapshot = {"output2": [{}], "output1": []}
    kis = DummyKis(snapshot, refreshed_snapshot=snapshot, orderable=0)
    engine = _make_engine(kis=kis)

    with pytest.raises(RuntimeError, match="Balance parse failed"):
        engine._resolve_holdings_snapshot_with_cash(snapshot)

    assert kis.calls == [False, True]


def test_ledger_only_positions_are_marked_orphan(monkeypatch):
    ledger_repo = DummyLedgerRepo()
    engine = _make_engine(ledger_repo=ledger_repo)

    def fake_rebuild(self, lookback_days=120):
        return {("000001", 1, 1): {"total_qty": 5, "market": "KOSPI"}}

    monkeypatch.setattr(LedgerStore, "rebuild_positions_average_cost", fake_rebuild)

    positions = engine._build_positions_from_kis([])

    assert positions == []
    assert len(ledger_repo.events) == 1
    event = ledger_repo.events[0]
    assert event["event_type"] == "POSITION_ORPHANED"
    assert event["code"] == "000001"
    assert event["qty"] == 5
