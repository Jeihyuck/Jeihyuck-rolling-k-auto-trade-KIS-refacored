from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


class DummyLedgerRepo:
    def append_event(self, **kwargs):
        return "evt"


class DummyKis:
    def __init__(self, orderable=0):
        self.orderable = orderable

    def get_orderable_cash_krw(self, force=False):
        return self.orderable

    def get_balance_cached(self, force=False, return_source=False):
        if return_source:
            return {}, "api"
        return {}


def _make_engine(kis=None):
    return PB1Engine(
        universe_repo=object(),
        orders_repo=object(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=DummyLedgerRepo(),
        kis=kis,
        window=WindowDecision(name="morning", phase="trade"),
        window_label="morning",
        phase="trade",
        dry_run=True,
        env="practice",
        run_id="test",
    )


def test_available_cash_from_dnca_list():
    engine = _make_engine(kis=DummyKis(orderable=0))
    snapshot = {"output2": [{"dnca_tot_amt": "10000000"}], "output1": []}

    _, cash, meta = engine._resolve_holdings_snapshot_with_cash(snapshot)

    assert cash == 10_000_000
    assert meta["source"] == "balance_snapshot"
    assert meta["selected_key"] == "dnca_tot_amt"


def test_available_cash_from_output2_dict():
    engine = _make_engine(kis=DummyKis(orderable=0))
    snapshot = {"output2": {"nxdy_excc_amt": "5000000", "dnca_tot_amt": "9000000"}, "output1": []}

    _, cash, meta = engine._resolve_holdings_snapshot_with_cash(snapshot)

    assert cash == 5_000_000
    assert meta["selected_key"] == "nxdy_excc_amt"


def test_available_cash_prefers_orderable_endpoint():
    engine = _make_engine(kis=DummyKis(orderable=8_000_000))
    snapshot = {"output2": [], "output1": []}

    _, cash, meta = engine._resolve_holdings_snapshot_with_cash(snapshot)

    assert cash == 8_000_000
    assert meta["source"] == "orderable_cash"
    assert meta["selected_key"] == "ord_psbl_cash"
