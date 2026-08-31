from trader.execution_state import (BrokerBalanceSnapshot, OrderBaseline, BalanceFreshness,
                                    balance_freshness_for_source)
import pandas as pd
import sqlalchemy as sa
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from tests.kr.test_kr_sell_no_sellable_session_block import FakeKis, _make_engine


BALANCE = {
    "output1": [{"pdno": "010060", "hldg_qty": "14", "ord_psbl_qty": "14", "pchs_avg_pric": "271660"}],
    "output2": [{"ord_psbl_cash": "1000000"}],
}


def test_existing_broker_holding_is_authoritative_entry_fence():
    snapshot = BrokerBalanceSnapshot.from_kis(BALANCE)
    assert snapshot.holding_qty("010060") == 14
    # A stale DB entry view cannot change the immutable tick snapshot.
    stale_db_qty = 0
    assert snapshot.holding_qty("010060") != stale_db_qty


def test_order_baseline_contains_immutable_balance_evidence():
    snapshot = BrokerBalanceSnapshot.from_kis(BALANCE)
    baseline = OrderBaseline.capture(snapshot, "010060", 7)
    assert baseline.balance_snapshot_id == snapshot.snapshot_id
    assert baseline.pre_order_holding_qty == 14
    assert baseline.pre_order_orderable_qty == 14
    assert baseline.pre_order_avg_price == 271660
    assert baseline.requested_qty == baseline.submitted_qty == 7


def test_forced_refresh_replaces_engine_authoritative_balance():
    initial = {"output1": [], "output2": None}
    refreshed = BALANCE

    class RefreshKis(FakeKis):
        def get_balance_cached(self, force=False, return_source=False):
            assert force
            return (refreshed, "api") if return_source else refreshed

        def get_orderable_cash_krw(self, force=False):
            return 1_000_000

    engine, kis = _make_engine(kis=RefreshKis(), balance_snapshot=initial)
    resolved, cash, _ = engine._resolve_holdings_snapshot_with_cash(initial)
    assert resolved is refreshed
    assert cash == 1_000_000
    assert engine._balance_snapshot is refreshed
    assert engine._authoritative_balance.holding_qty("010060") == 14
    assert engine._authoritative_balance.freshness is BalanceFreshness.FRESH
    assert kis.sell_calls == 0


def test_balance_freshness_classification():
    expected = {
        "api": BalanceFreshness.FRESH,
        "forced_refresh_output2_none": BalanceFreshness.FRESH,
        "forced_refresh_sanitized": BalanceFreshness.FRESH,
        "forced_refresh_cash": BalanceFreshness.FRESH,
        "cached": BalanceFreshness.CACHED,
        "cached_kis_balance": BalanceFreshness.CACHED,
        "stale": BalanceFreshness.STALE,
        "persisted_cache": BalanceFreshness.STALE,
        "engine_fail_soft_empty": BalanceFreshness.INVALID,
    }
    assert {source: balance_freshness_for_source(source) for source in expected} == expected


def _engine_with_filled_tp1(*, source: str, initial: dict, kis):
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    OrdersRepo(db).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=None, stage="TP1", client_order_key=f"freshness-{source}",
        request_json={"exit_stage": "TP1", "trade_session": "day",
                      "pre_order_holding_qty": 14, "submitted_qty": 7},
        status="FILLED", position_cycle_id="cycle-freshness",
    )
    engine, _ = _make_engine(db, kis, initial)
    return engine


def _hard_stop_position():
    return {"code": "010060", "name": "OCI", "mode": 1, "sid": 1, "qty": 7,
            "kis_qty": 7, "orderable_qty": 7, "avg_buy_price": 271660.0,
            "last_price": 260000.0, "stop_price": 265000.0, "market": "J",
            "holding_days": 1, "entry_date": "2026-08-30", "holding_source": "kis_balance",
            "position_cycle_id": "cycle-freshness",
            "position_meta": {"position_cycle_id": "cycle-freshness"}}


def test_forced_refresh_balance_allows_full_exit_after_filled_tp1(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    refreshed = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7",
                               "pchs_avg_pric": "271660"}], "output2": [{"ord_psbl_cash": "1000000"}]}

    class RefreshKis(FakeKis):
        def get_balance_cached(self, force=False, return_source=False):
            assert force
            return (refreshed, "api") if return_source else refreshed
        def get_orderable_cash_krw(self, force=False): return 1_000_000

    kis = RefreshKis()
    kis.sell_calls, kis.sell_quantities = 1, [7]
    initial = {"output1": [], "output2": None}
    engine = _engine_with_filled_tp1(source="api", initial=initial, kis=kis)
    engine._resolve_holdings_snapshot_with_cash(initial)
    result = engine._plan_exit_event(_hard_stop_position(), {"close": 260000.0}, pd.DataFrame(), "day")
    assert engine._authoritative_balance.source == "forced_refresh_output2_none"
    assert engine._authoritative_balance.freshness is BalanceFreshness.FRESH
    assert result["submitted"] == 1
    assert kis.sell_calls == 2 and kis.sell_quantities == [7, 7]


def test_stale_balance_does_not_prove_partial_to_full_progression():
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7"}], "output2": [{}]}
    engine = _engine_with_filled_tp1(source="stale", initial=balance, kis=FakeKis())
    engine._set_authoritative_balance(balance, source="stale", freshness=BalanceFreshness.STALE)
    blocked, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-freshness", exit_stage="FULL_EXIT")
    assert blocked


def test_invalid_balance_does_not_prove_partial_to_full_progression():
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7"}], "output2": [{}]}
    engine = _engine_with_filled_tp1(source="invalid", initial=balance, kis=FakeKis())
    engine._set_authoritative_balance(balance, source="engine_fail_soft_empty", freshness=BalanceFreshness.INVALID)
    blocked, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-freshness", exit_stage="FULL_EXIT")
    assert blocked
