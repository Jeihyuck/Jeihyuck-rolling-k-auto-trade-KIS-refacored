from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kis_wrapper import (
    KisAPI,
    KisTemporaryError,
    _kr_bounded_timeout,
    _kr_sleep_with_budget,
    clear_kr_tick_deadline,
    kr_tick_remaining_sec,
    set_kr_tick_deadline,
)
from trader.pb1_runner import _resolve_kr_shared_tick_budget
from trader.reconcile_kis import (
    _promote_open_buy_orders_from_holdings,
    reconcile_today,
)
from trader.trade_plan import build_entry_exit_plan
from trader.time_utils import now_kst


def _engine():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _new_buy_order(engine, *, key: str, odno: str):
    orders = OrdersRepo(engine)
    plan = build_entry_exit_plan(
        code="293490",
        market="KOSDAQ",
        entry_style_selected="ENTRY_PULLBACK",
        entry_reason="ENTRY_PULLBACK",
        entry_price=9550.0,
        features={"stop_price": 8026.79},
    ).to_dict()
    order_id, created = orders.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        market="KOSDAQ",
        side="BUY",
        ord_type="LIMIT",
        qty=207,
        limit_price=9600.0,
        stage="PB1-ENTRY",
        client_order_key=key,
        request_json={
            "enforce_entry_contract": True,
            "entry_plan": {"entry_price": 9550.0},
            "entry_meta": {
                "entry_reason": "ENTRY_PULLBACK",
                "entry_style_selected": "ENTRY_PULLBACK",
                "entry_thesis": "PULLBACK_CONTINUATION",
                "trade_horizon": "SWING",
                "exit_policy_family": "SWING_STAGED_EXIT",
            },
            "entry_exit_plan": plan,
            "pre_order_holding_qty": 0,
            "pre_order_orderable_qty": 0,
            "pre_order_avg_price": 0.0,
            "requested_qty": 207,
            "submitted_qty": 207,
            "balance_snapshot_id": "kr-balance-before-293490",
            "order_intent_ts": now_kst().isoformat(),
        },
        entry_meta_json={
            "entry_reason": "ENTRY_PULLBACK",
            "entry_style_selected": "ENTRY_PULLBACK",
            "entry_thesis": "PULLBACK_CONTINUATION",
            "trade_horizon": "SWING",
            "exit_policy_family": "SWING_STAGED_EXIT",
        },
        status="CREATED",
        account_id="acct-pr133",
    )
    assert created
    orders.mark_submitted(
        "practice",
        key,
        odno,
        {"rt_cd": "0", "msg1": "accepted"},
        submitted_qty=207,
    )
    orders.mark_acked("practice", odno, {"rt_cd": "0", "msg1": "accepted"})
    row = orders.get_order_by_client_order_key("practice", key)
    assert row
    return orders, row, plan, str(order_id)


def _kakao_holding():
    return {
        "pdno": "293490",
        "hldg_qty": "207",
        "ord_psbl_qty": "207",
        "pchs_avg_pric": "9555.507",
        "pchs_amt": str(9555.507 * 207),
        "prpr": "9560",
        "evlu_amt": str(9560 * 207),
        "prdt_type_cd": "KOSDAQ",
        "prdt_name": "카카오게임즈",
    }


def test_293490_ack_balance_delta_promotes_fill_and_binds_exact_buy_contract():
    engine = _engine()
    orders, source, plan, _ = _new_buy_order(
        engine, key="pr133-293490-holdings", odno="KR293490-1"
    )
    fills = FillsRepo(engine)
    positions = PositionsRepo(engine)

    result = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=now_kst(),
        holdings_rows=[_kakao_holding()],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )

    assert result["orders"] == 1
    assert result["fills"] == 1
    persisted_order = orders.get_order_by_client_order_key(
        "practice", "pr133-293490-holdings"
    )
    assert persisted_order["status"] == "FILLED"
    assert persisted_order["request_json"]["pre_order_holding_qty"] == 0
    assert persisted_order["request_json"]["requested_qty"] == 207
    assert persisted_order["request_json"]["submitted_qty"] == 207
    assert persisted_order["request_json"]["entry_contract_sha256"] == source["request_json"]["entry_contract_sha256"]

    position = positions.get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        position_cycle_id=str(source["position_cycle_id"]),
        portfolio_epoch_id=str(source["portfolio_epoch_id"]),
    )
    assert position
    assert position["position_origin"] == "SYSTEM"
    assert int(position["qty"]) == 207
    assert position["entry_exit_plan_json"] == plan
    assert position["entry_meta_json"]["entry_contract_sha256"] == source["request_json"]["entry_contract_sha256"]
    assert position["entry_meta_json"]["source_buy_order_id"] == str(source["order_id"])

    # Re-running the same holdings reconciliation must never double-apply 207 shares.
    second = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=now_kst(),
        holdings_rows=[_kakao_holding()],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )
    assert second["fills"] == 0
    reloaded = positions.get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        position_cycle_id=str(source["position_cycle_id"]),
        portfolio_epoch_id=str(source["portfolio_epoch_id"]),
    )
    assert int(reloaded["qty"]) == 207


def test_daily_ccld_preserves_original_buy_request_and_applies_position_once():
    engine = _engine()
    orders, source, plan, _ = _new_buy_order(
        engine, key="pr133-293490-ccld", odno="KR293490-2"
    )

    class FakeKis:
        def inquire_daily_ccld(self, **kwargs):
            return {
                "rt_cd": "0",
                "output1": [{
                    "pdno": "293490",
                    "sll_buy_dvsn_cd": "02",
                    "odno": "KR293490-2",
                    "ord_qty": "207",
                    "tot_ccld_qty": "207",
                    "ccld_qty": "207",
                    "ccld_prc": "9555.507",
                    "ccld_no": "EXEC-293490-1",
                    "ord_stat_cd": "FILLED",
                    "ord_dt": "20260918",
                    "ord_tmd": "093200",
                }],
                "output2": [],
            }

    ctx = SimpleNamespace(
        env="practice",
        strategy="pb1_pullback_close",
        run_id=None,
    )
    first = reconcile_today(engine=engine, kis=FakeKis(), ctx=ctx)
    assert first["fills"] == 1

    persisted = orders.get_order_by_client_order_key("practice", "pr133-293490-ccld")
    assert persisted["request_json"]["pre_order_holding_qty"] == 0
    assert persisted["request_json"]["balance_snapshot_id"] == "kr-balance-before-293490"
    assert persisted["request_json"]["entry_exit_plan"] == plan
    assert persisted["request_json"]["entry_contract_sha256"] == source["request_json"]["entry_contract_sha256"]

    position = PositionsRepo(engine).get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        position_cycle_id=str(source["position_cycle_id"]),
        portfolio_epoch_id=str(source["portfolio_epoch_id"]),
    )
    assert position
    assert position["position_origin"] == "SYSTEM"
    assert int(position["qty"]) == 207
    assert position["entry_exit_plan_json"] == plan

    # Same broker execution observed again is diagnostic only; position qty is unchanged.
    second = reconcile_today(engine=engine, kis=FakeKis(), ctx=ctx)
    assert second["fills"] == 0
    position2 = PositionsRepo(engine).get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        position_cycle_id=str(source["position_cycle_id"]),
        portfolio_epoch_id=str(source["portfolio_epoch_id"]),
    )
    assert int(position2["qty"]) == 207


@pytest.mark.parametrize(
    "remaining,base,grace,expected",
    [
        (120.0, 90.0, 15.0, 90.0),
        (20.0, 90.0, 15.0, 5.0),
        (10.0, 90.0, 15.0, 0.0),
    ],
)
def test_kr_shared_tick_budget_reserves_grace_before_session_end(
    remaining, base, grace, expected
):
    assert _resolve_kr_shared_tick_budget(
        remaining_to_session_end=remaining,
        base_tick_timeout=base,
        grace_sec=grace,
    ) == pytest.approx(expected)


def test_kr_kis_request_and_retry_sleep_cannot_outlive_shared_tick_deadline():
    try:
        set_kr_tick_deadline(time.monotonic() + 0.20)
        bounded = _kr_bounded_timeout((3.0, 7.0))
        assert isinstance(bounded, tuple)
        assert bounded[0] > 0
        assert bounded[1] > 0
        assert sum(bounded) < 0.20
        assert _kr_sleep_with_budget(0.50) is False

        set_kr_tick_deadline(time.monotonic() + 0.03)
        with pytest.raises(KisTemporaryError, match="KR_TICK_DEADLINE_EXHAUSTED"):
            _kr_bounded_timeout((3.0, 7.0))
    finally:
        clear_kr_tick_deadline()


def test_kr_balance_stage_preserves_sell_routing_reserve(monkeypatch):
    client = object.__new__(KisAPI)
    client._balance_cache = None
    client._balance_cache_at = None
    client._kr_stage_deadline = None
    observed = {}

    def fake_balance():
        observed["remaining"] = kr_tick_remaining_sec(client._kr_stage_deadline)
        return {
            "rt_cd": "0",
            "output1": [],
            "output2": [{
                "dnca_tot_amt": "1000000",
                "ord_psbl_cash": "1000000",
                "tot_evlu_amt": "1000000",
            }],
            "ctx_area_fk100": "",
            "ctx_area_nk100": "",
        }

    client.inquire_balance_all = fake_balance
    monkeypatch.setenv("KR_BALANCE_FETCH_BUDGET_SEC", "30")
    monkeypatch.setenv("KR_SELL_ROUTING_RESERVE_SEC", "10")
    try:
        set_kr_tick_deadline(time.monotonic() + 12.0)
        snapshot = client.get_balance_cached(force=True)
        assert isinstance(snapshot, dict)
        # 12 sec tick - 10 sec SELL reserve leaves at most ~2 sec for balance.
        assert 0 < observed["remaining"] <= 2.1
        # The shared tick still retains the routing reserve after stage binding.
        assert kr_tick_remaining_sec() > 9.0
    finally:
        clear_kr_tick_deadline()
