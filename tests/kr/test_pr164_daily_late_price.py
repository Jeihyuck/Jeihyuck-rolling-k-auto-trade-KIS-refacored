"""Production reconcile_today uses late-priced daily-ccld without reapplying holdings qty."""
from datetime import datetime

import pytest

import trader.reconcile_kis as rk
from trader.run_context import RunContext


class OrderRepo:
    def __init__(self, *, already_confirmed=37):
        self.already_confirmed = already_confirmed
        self.observations = []
        self.rows = []

    def get_order_by_kis_odno(self, env, no):
        assert env == "practice" and no == "0000012522"
        return {
            "order_id": "exact-sell-id", "client_order_key": "exact-sell-key",
            "strategy": "pb1_pullback_close", "sid": 1, "mode": 1,
            "side": "SELL", "qty": 37, "market": "J", "code": "293490",
            "position_cycle_id": "exact-cycle", "portfolio_epoch_id": "exact-portfolio",
            "request_json": {"pre_order_holding_qty": 115},
            "response_json": {
                "promotion_source": "kis_holdings", "confirmed_fill_qty": self.already_confirmed,
                "pre_order_holding_qty": 115, "holding_qty": 78,
            },
        }

    def upsert_reconciled_order(self, **kwargs):
        self.rows.append(kwargs)

    def record_execution_claim_for_order(self, key, **kwargs):
        self.observations.append((key, kwargs))

    def execution_claim_health(self, *, market=None):
        return {"available": True, "in_flight": 0, "uncertain": 0, "unresolved": 0}


class NoOpRepo:
    def __init__(self, *_args, **_kwargs):
        pass

    def __getattr__(self, _name):
        return lambda **kwargs: None


class PositionRepo(NoOpRepo):
    def __init__(self):
        self.sells = []

    def reconcile_sell_execution(self, **kwargs):
        self.sells.append(kwargs)
        return {"qty_applied": 0, "pnl_qty_applied": kwargs["confirmed_cumulative_qty"]}


class FakeKIS:
    def __init__(self, *, trade_fill=37, total_fill=37, px=10050.0):
        self.trade_fill, self.total_fill, self.px = trade_fill, total_fill, px

    def inquire_daily_ccld(self, **kwargs):
        return {"output1": [{
            "pdno": "293490", "side": "SELL", "ord_qty": "37",
            "odno": "0000012522", "ord_stat_cd": "FILLED",
            "tot_ccld_qty": str(self.total_fill),
            "ccld_qty": str(self.trade_fill),
            "ccld_prc": str(self.px),
            "ccld_no": "trade-1",
        }]}


@pytest.fixture
def fake_reconcile(monkeypatch):
    orders, positions = OrderRepo(), PositionRepo()
    monkeypatch.setattr(rk, "OrdersRepo", lambda _: orders)
    monkeypatch.setattr(rk, "PositionsRepo", lambda _: positions)
    monkeypatch.setattr(rk, "FillsRepo", NoOpRepo)
    monkeypatch.setattr(rk, "LedgerEventsRepo", NoOpRepo)
    monkeypatch.setattr(rk, "ReconcileLogRepo", NoOpRepo)
    monkeypatch.setattr(rk, "_resolve_reconcile_run_id", lambda *_: None)
    monkeypatch.setattr(rk, "_execution_claim_health", lambda *_args, **_kwargs: {"available": True})
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(rk, "now_kst", lambda: datetime(2026, 10, 6, 15, 0))
    return orders, positions


def _run(kis):
    ctx = RunContext(
        run_id="test-reconcile", env="practice", strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 6, 15, 0), dry_run=True,
    )
    return rk.reconcile_today(engine=None, kis=kis, ctx=ctx)


def test_late_daily_price_converges_already_confirmed_sell_without_second_qty(fake_reconcile):
    orders, positions = fake_reconcile
    result = _run(FakeKIS())
    assert result["orders"] == 1
    assert result["fills"] == 0  # already proved by holdings
    assert len(positions.sells) == 1
    assert positions.sells[0]["confirmed_cumulative_qty"] == 37
    assert positions.sells[0]["fill_price"] == 10050.0
    assert positions.sells[0]["broker_holding_qty"] is None
    assert orders.observations[0][1]["cumulative_filled_qty"] == 37


def test_partial_daily_price_is_not_mistaken_for_cumulative_average(fake_reconcile):
    orders, positions = fake_reconcile
    orders.already_confirmed = 3
    result = _run(FakeKIS(trade_fill=2, total_fill=5, px=10100.0))
    assert result["orders"] == 1
    assert len(positions.sells) == 1
    assert positions.sells[0]["confirmed_cumulative_qty"] == 5
    assert positions.sells[0]["fill_price"] is None


def test_late_per_fill_row_without_broker_cumulative_cannot_increase_confirmed_qty(fake_reconcile):
    orders, positions = fake_reconcile
    class NoTotalKIS(FakeKIS):
        def inquire_daily_ccld(self, **kwargs):
            data = super().inquire_daily_ccld(**kwargs)
            data["output1"][0].pop("tot_ccld_qty")
            return data
    result = _run(NoTotalKIS())
    assert result["orders"] == 1
    assert positions.sells == []  # quantity already held; price evidence incomplete
    assert orders.observations[0][1]["cumulative_filled_qty"] == 37
