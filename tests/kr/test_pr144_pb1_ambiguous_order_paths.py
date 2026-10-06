from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kis_wrapper import KisAuthError, KisOrderOutcomeUnknown
from trader.pb1_engine import PB1Engine
from trader.reconcile_kis import (
    _project_pb1_exit_stage_truth,
    _promote_open_buy_orders_from_holdings,
)
from trader.trade_plan import build_entry_exit_plan
from trader.window_router import WindowDecision
from tests.kr.test_kr_entry_authoritative_gate_state import _build_candidate
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


class AmbiguousBuyKis:
    def __init__(self) -> None:
        self.buy_calls = 0

    def buy_stock_limit(self, code: str, qty: int, price: float):
        self.buy_calls += 1
        raise KisOrderOutcomeUnknown("accepted boundary crossed; ACK body unavailable")

    def get_quote_snapshot(self, code: str):
        return {"ap": 10000.0, "tp": 10000.0, "close": 10000.0}




class AuthRejectThenAcceptBuyKis:
    def __init__(self) -> None:
        self.buy_calls = 0

    def buy_stock_limit(self, code: str, qty: int, price: float):
        self.buy_calls += 1
        if self.buy_calls == 1:
            raise KisAuthError("HTTP 401 order auth rejection")
        return {
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {"ODNO": f"RETRY-{code}-{qty}"},
        }

class ExplicitRejectThenAcceptBuyKis:
    def __init__(self) -> None:
        self.buy_calls = 0

    def buy_stock_limit(self, code: str, qty: int, price: float):
        self.buy_calls += 1
        if self.buy_calls == 1:
            return {
                "rt_cd": "1",
                "msg_cd": "EGW00201",
                "msg1": "초당 거래건수 초과",
                "output": {},
            }
        return {
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {"ODNO": f"RETRY-BIZ-{code}-{qty}"},
        }


class AckOnlyBuyKis:
    def __init__(self) -> None:
        self.buy_calls = 0

    def buy_stock_limit(self, code: str, qty: int, price: float):
        self.buy_calls += 1
        return {
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {"ODNO": f"ACK-{code}-{qty}"},
        }


class AmbiguousSellKis:
    def __init__(self) -> None:
        self.sell_calls = 0
        self.sell_quantities: list[int] = []

    def sell_stock_market(self, code: str, qty: int):
        self.sell_calls += 1
        self.sell_quantities.append(qty)
        raise KisOrderOutcomeUnknown("accepted boundary crossed; ACK body unavailable")


class UnresolvedThenAcceptedSellKis:
    def __init__(self) -> None:
        self.sell_calls = 0
        self.sell_quantities: list[int] = []

    def sell_stock_market(self, code: str, qty: int):
        self.sell_calls += 1
        self.sell_quantities.append(qty)
        if self.sell_calls == 1:
            raise KisOrderOutcomeUnknown("accepted boundary crossed; ACK body unavailable")
        return {
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {"ODNO": f"SELL-{self.sell_calls}"},
        }


class RetryableSellKis:
    def __init__(self, *, reject_first: bool = False) -> None:
        self.sell_calls = 0
        self.sell_quantities: list[int] = []
        self.reject_first = reject_first

    def sell_stock_market(self, code: str, qty: int):
        self.sell_calls += 1
        self.sell_quantities.append(qty)
        if self.reject_first and self.sell_calls == 1:
            return {
                "rt_cd": "1",
                "msg_cd": "REJECTED",
                "msg1": "broker rejected order",
                "output": {},
            }
        return {
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {"ODNO": f"SELL-{self.sell_calls}"},
        }


def _engine(db, kis, *, phase: str = "entry", window_name: str = "day", balance_snapshot=None):
    return PB1Engine(
        universe_repo=object(),
        orders_repo=OrdersRepo(db),
        fills_repo=FillsRepo(db),
        positions_repo=PositionsRepo(db),
        ledger_repo=LedgerEventsRepo(db),
        kis=kis,
        window=WindowDecision(name=window_name, phase=phase),
        window_label=window_name,
        phase=phase,
        dry_run=False,
        intended_live=True,
        env="practice",
        run_id="pr144-path-test",
        order_allowed=True,
        trading_day=True,
        now_kst_value=datetime(2026, 9, 23, 13, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        balance_snapshot=balance_snapshot,
        balance_source="api" if balance_snapshot else None,
    )


def _new_db():
    db = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(db)
    return db


def _sell_case(db, kis, *, holding_qty: int = 20):
    code = "123450"
    positions = PositionsRepo(db)
    persisted, created = positions.get_or_create_imported_cycle_for_kis_holding(
        env="practice",
        strategy="pb1_pullback_close",
        account_id=get_account_key(env="practice"),
        sid=1,
        mode=1,
        code=code,
        market="J",
        qty=holding_qty,
        avg_price=100.0,
    )
    assert persisted is not None
    balance = {
        "output1": [{
            "pdno": code,
            "hldg_qty": str(holding_qty),
            "ord_psbl_qty": str(holding_qty),
            "pchs_avg_pric": "100",
        }],
        "output2": [{}],
    }
    engine = _engine(db, kis, phase="manage", balance_snapshot=balance)
    engine._resolve_price_with_fallback = lambda *_args, **_kwargs: (109.0, "test")
    position = dict(persisted)
    position.update(
        qty=holding_qty,
        kis_qty=holding_qty,
        orderable_qty=holding_qty,
        avg_buy_price=100.0,
        last_price=109.0,
        holding_source="kis_balance",
    )
    return engine, position


def _run_sell(engine, position):
    return engine._plan_exit_event(
        position,
        {"close": 109.0, "ma20": 101.0, "ma50": 99.0},
        pd.DataFrame(),
        "day",
    )


def test_regular_buy_unresolved_ack_survives_restart_and_blocks_resubmit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    kis = AmbiguousBuyKis()
    candidate = _build_candidate("018260")
    candidate.client_order_key = "pr144-regular-unresolved"

    first = _engine(db, kis)._place_entry(candidate)
    row = OrdersRepo(db).get_order_by_client_order_key("practice", candidate.client_order_key)

    assert first["submit_terminal_status"] == "UNRESOLVED_ACK"
    assert row["status"] == "UNRESOLVED_ACK"
    assert kis.buy_calls == 1

    restarted_candidate = _build_candidate("018260")
    restarted_candidate.client_order_key = candidate.client_order_key
    second = _engine(db, kis)._place_entry(restarted_candidate)

    assert kis.buy_calls == 1
    assert second["terminal_event"] == "FINAL_SKIP"


def test_kr_claim_acquisition_failure_blocks_before_broker_submit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))

    def unavailable_claim(*_args, **_kwargs):
        raise OSError("execution claim store unavailable")

    monkeypatch.setattr(OrdersRepo, "claim_execution_action", unavailable_claim)
    kis = AckOnlyBuyKis()
    candidate = _build_candidate("018260")
    candidate.client_order_key = "kr-claim-acquisition-failure"

    result = _engine(_new_db(), kis)._place_entry(candidate)

    assert result["submit_terminal_status"] == "EXECUTION_CLAIM_UNAVAILABLE"
    assert result["skipped_reason"] == "EXECUTION_CLAIM_UNAVAILABLE"
    assert kis.buy_calls == 0


def test_kr_claim_observation_failure_is_visible_and_keeps_submit_fence(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    kis = AckOnlyBuyKis()
    record_observation = OrdersRepo.record_execution_claim_for_order

    def fail_ack_observation(self, client_order_key, **kwargs):
        if kwargs.get("state") == "ACKED":
            raise OSError("execution claim store unavailable after broker ACK")
        return record_observation(self, client_order_key, **kwargs)

    monkeypatch.setattr(OrdersRepo, "record_execution_claim_for_order", fail_ack_observation)
    candidate = _build_candidate("018260")
    candidate.client_order_key = "kr-claim-observation-failure"

    first = _engine(db, kis)._place_entry(candidate)
    health = OrdersRepo(db).execution_claim_health()

    assert kis.buy_calls == 1
    assert first["reconcile_required"] == 1
    assert first["execution_integrity_error"]
    assert first["submit_terminal_status"] == (
        "EXECUTION_CLAIM_OBSERVATION_FAILED_RECONCILE_REQUIRED"
    )
    assert health["unresolved_execution_actions"] == 1

    retry = _build_candidate("018260")
    retry.client_order_key = "kr-claim-observation-failure-retry"
    second = _engine(db, kis)._place_entry(retry)

    assert second["submit_terminal_status"] == "EXECUTION_ACTION_ALREADY_CLAIMED"
    blocked_order = OrdersRepo(db).get_order_by_client_order_key(
        "practice", retry.client_order_key
    )
    assert blocked_order["status"] == "ERROR"
    assert blocked_order["response_json"]["broker_submit"] is False
    assert kis.buy_calls == 1
    assert OrdersRepo(db).execution_claim_health()["unresolved_execution_actions"] == 1


def test_close_buy_unresolved_ack_survives_restart_and_blocks_resubmit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    kis = AmbiguousBuyKis()
    candidate = _build_candidate("018260")
    candidate.client_order_key = "pr144-close-unresolved"

    first = _engine(db, kis, window_name="close")._place_entry_close(candidate)
    row = OrdersRepo(db).get_order_by_client_order_key("practice", candidate.client_order_key)

    assert first["submit_terminal_status"] == "UNRESOLVED_ACK"
    assert row["status"] == "UNRESOLVED_ACK"
    assert kis.buy_calls == 1

    restarted_candidate = _build_candidate("018260")
    restarted_candidate.client_order_key = candidate.client_order_key
    second = _engine(db, kis, window_name="close")._place_entry_close(restarted_candidate)

    assert kis.buy_calls == 1
    assert second["terminal_event"] == "FINAL_SKIP"


def _prepare_parent_position(db):
    plan = build_entry_exit_plan(
        code="005930",
        market="KOSPI",
        entry_style_selected="ENTRY_PULLBACK",
        entry_reason="ENTRY_PULLBACK",
        entry_price=100.0,
        features={"stop_price": 95.0},
    ).to_dict()
    orders = OrdersRepo(db)
    positions = PositionsRepo(db)
    root_order_id, created = orders.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        ord_type="LIMIT",
        qty=10,
        limit_price=100.0,
        stage="PB1-ENTRY",
        client_order_key="pr144-root-buy",
        request_json={
            "enforce_entry_contract": True,
            "entry_exit_plan": plan,
            "pre_order_holding_qty": 0,
            "requested_qty": 10,
            "submitted_qty": 10,
            "balance_snapshot_id": "pr144-root-balance",
        },
        entry_meta_json={
            "entry_reason": "ENTRY_PULLBACK",
            "entry_style_selected": "ENTRY_PULLBACK",
        },
        status="ACKED",
        account_id=get_account_key(env="practice"),
    )
    assert created
    positions.apply_fill(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=10,
        price=100.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime(2026, 9, 23, 12, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        order_id=root_order_id,
        account_id=get_account_key(env="practice"),
    )
    positions.update_position_fields(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        fields={
            "pyramid_level": 0,
            "stop_price": 95.0,
            "initial_stop": 95.0,
        },
    )
    with db.connect() as conn:
        return dict(
            conn.execute(
                sa.select(schema_for_engine(db).positions).where(
                    schema_for_engine(db).positions.c.code == "005930"
                )
            ).mappings().one()
        )


def test_add_on_unresolved_ack_survives_restart_and_blocks_resubmit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    parent = _prepare_parent_position(db)
    kis = AmbiguousBuyKis()

    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)

    with db.connect() as conn:
        add_rows = list(
            conn.execute(
                sa.select(schema_for_engine(db).orders).where(
                    schema_for_engine(db).orders.c.stage == "PB1-ADD"
                )
            ).mappings()
        )
    assert len(add_rows) == 1
    assert add_rows[0]["status"] == "UNRESOLVED_ACK"
    assert kis.buy_calls == 1

    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)
    assert kis.buy_calls == 1


def test_tp_sell_unresolved_ack_keeps_pending_and_blocks_restart_resubmit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")

    db = _new_db()
    positions = PositionsRepo(db)
    persisted, created = positions.get_or_create_imported_cycle_for_kis_holding(
        env="practice",
        strategy="pb1_pullback_close",
        account_id=get_account_key(env="practice"),
        sid=1,
        mode=1,
        code="067290",
        market="J",
        qty=20,
        avg_price=100.0,
    )
    assert created
    balance = {
        "output1": [{
            "pdno": "067290",
            "hldg_qty": "20",
            "ord_psbl_qty": "20",
            "pchs_avg_pric": "100",
        }],
        "output2": [{}],
    }
    kis = AmbiguousSellKis()
    engine = _engine(db, kis, phase="manage", balance_snapshot=balance)
    monkeypatch.setattr(engine, "_resolve_price_with_fallback", lambda *_args, **_kwargs: (109.0, "test"))
    pos = dict(persisted)
    pos.update(
        qty=20,
        kis_qty=20,
        orderable_qty=20,
        avg_buy_price=100.0,
        last_price=109.0,
        holding_source="kis_balance",
    )

    first = engine._plan_exit_event(
        pos,
        {"close": 109.0, "ma20": 101.0, "ma50": 99.0},
        pd.DataFrame(),
        "day",
    )
    order = OrdersRepo(db).list_today_orders(
        "practice", side="SELL", code="067290", status_exclude=()
    )[0]
    stored = positions.get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="067290",
        position_cycle_id=str(persisted["position_cycle_id"]),
        portfolio_epoch_id=str(persisted["portfolio_epoch_id"]),
    )

    assert first["decision_reason"] == "KR_TAKE_PROFIT_TP1"
    assert first["reconcile_required"] == 1
    assert order["status"] == "UNRESOLVED_ACK"
    assert stored["position_meta"]["kr_tp1_pending"] is True
    assert kis.sell_calls == 1

    restarted = _engine(db, kis, phase="manage", balance_snapshot=balance)
    monkeypatch.setattr(restarted, "_resolve_price_with_fallback", lambda *_args, **_kwargs: (109.0, "test"))
    second = restarted._plan_exit_event(
        pos,
        {"close": 109.0, "ma20": 101.0, "ma50": 99.0},
        pd.DataFrame(),
        "day",
    )

    assert kis.sell_calls == 1
    assert second["order_result"] == "ORDER_SKIPPED_DURABLE_SESSION_BLOCK"


def test_explicit_sell_reject_allows_fresh_retry_through_pb1_claim(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis(reject_first=True)
    engine, position = _sell_case(db, kis)

    _run_sell(engine, position)
    assert kis.sell_calls == 1
    assert OrdersRepo(db).list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]["status"] == "ERROR"

    second_engine, second_position = _sell_case(db, kis)
    second = _run_sell(second_engine, second_position)

    assert second["submitted"] == 1
    assert kis.sell_calls == 2
    sell_orders = OrdersRepo(db).list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )
    assert len(sell_orders) == 2
    assert sorted(row["status"] for row in sell_orders) == ["ACKED", "ERROR"]


def test_giveback_completion_waits_for_authoritative_sell_fill(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setattr("trader.pb1_engine._position_policy_missing_contract", lambda *_args: False)
    monkeypatch.setattr("trader.pb1_engine._resolve_position_horizon", lambda *_args, **_kwargs: "SWING")
    monkeypatch.setattr("trader.pb1_engine._resolve_position_book", lambda *_args, **_kwargs: "SWING_BOOK")
    monkeypatch.setattr(
        "trader.pb1_engine._horizon_to_exit_family",
        lambda *_args, **_kwargs: "SWING_STAGED_EXIT",
    )
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis(reject_first=True)
    engine, position = _sell_case(db, kis)
    position.update(
        entry_style_selected="ENTRY_PULLBACK",
        exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING",
    )
    position["position_meta"] = {
        **(position.get("position_meta") or {}),
        "max_pnl_pct_since_entry": 13.0,
    }
    first = _run_sell(engine, position)

    orders = OrdersRepo(db)
    first_order = orders.list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]
    assert first_order["status"] == "ERROR"
    assert first_order["request_json"]["exit_reason"] == "SWING_PROFIT_PROTECT_GIVEBACK"
    assert first.get("submitted") != 1
    rejected_position = PositionsRepo(db).get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        position_cycle_id=str(position["position_cycle_id"]),
        portfolio_epoch_id=str(position["portfolio_epoch_id"]),
    )
    assert rejected_position["position_meta"].get("giveback_protect_done") is not True

    retry_engine, retry_position = _sell_case(db, kis)
    retry_position.update(
        entry_style_selected="ENTRY_PULLBACK",
        exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING",
    )
    retry_position["position_meta"] = {
        **(retry_position.get("position_meta") or {}),
        "max_pnl_pct_since_entry": 13.0,
    }
    retry = _run_sell(retry_engine, retry_position)
    sell_orders = orders.list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )
    assert retry["submitted"] == 1
    assert len(sell_orders) == 2
    assert kis.sell_calls == 2

    retried_order = next(row for row in sell_orders if row["status"] == "ACKED")
    assert retried_order["request_json"]["exit_reason"] == "SWING_PROFIT_PROTECT_GIVEBACK"
    assert (
        retried_order["request_json"]["semantic_action"]
        == first_order["request_json"]["semantic_action"]
    )
    order = orders.get_order_by_client_order_key(
        "practice", retried_order["client_order_key"],
    )
    requested_qty = int(order["qty"])
    request_json = order["request_json"]
    assert request_json["execution_meta_update"]["giveback_protect_done"] is True
    orders.record_execution_claim_for_order(
        order["client_order_key"],
        state="FILLED",
        cumulative_filled_qty=requested_qty,
        authoritative=True,
    )
    snapshot = orders.get_execution_action_snapshot(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=request_json["position_lifecycle_id"],
        action=request_json["semantic_action"],
    )
    assert snapshot.action_state == "SATISFIED"
    assert snapshot.active_attempt_id is None
    _project_pb1_exit_stage_truth(
        orders_repo=orders,
        positions_repo=PositionsRepo(db),
        env="practice",
        code="123450",
        strategy="pb1_pullback_close",
        source_order=order,
        request_json=request_json,
    )
    completed = PositionsRepo(db).get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        position_cycle_id=str(position["position_cycle_id"]),
        portfolio_epoch_id=str(position["portfolio_epoch_id"]),
    )
    assert completed["position_meta"]["giveback_protect_done"] is True


def test_authoritative_zero_fill_cancel_allows_fresh_pb1_sell_retry(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis()
    engine, position = _sell_case(db, kis)
    _run_sell(engine, position)

    orders = OrdersRepo(db)
    order = orders.list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]
    orders.mark_cancelled(
        "practice", order["client_order_key"], {"tot_ccld_qty": "0"},
    )
    orders.record_execution_claim_for_order(
        order["client_order_key"],
        state="CANCELLED",
        cumulative_filled_qty=0,
        authoritative=True,
    )
    _project_pb1_exit_stage_truth(
        orders_repo=orders,
        positions_repo=PositionsRepo(db),
        env="practice",
        code="123450",
        strategy="pb1_pullback_close",
        source_order=order,
        request_json=order["request_json"],
    )

    retry_engine, retry_position = _sell_case(db, kis)
    retried = _run_sell(retry_engine, retry_position)

    assert retried["submitted"] == 1
    assert kis.sell_calls == 2


def test_cancel_ack_without_fill_quantity_stays_fenced_in_pb1(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis()
    engine, position = _sell_case(db, kis)
    _run_sell(engine, position)

    orders = OrdersRepo(db)
    order = orders.list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]
    orders.mark_cancelled("practice", order["client_order_key"], {"msg1": "cancel accepted"})
    orders.record_execution_claim_for_order(
        order["client_order_key"],
        state="CANCELLED",
        cumulative_filled_qty=None,
        authoritative=False,
    )
    _project_pb1_exit_stage_truth(
        orders_repo=orders,
        positions_repo=PositionsRepo(db),
        env="practice",
        code="123450",
        strategy="pb1_pullback_close",
        source_order=order,
        request_json=order["request_json"],
    )

    retry_engine, retry_position = _sell_case(db, kis)
    retried = _run_sell(retry_engine, retry_position)

    assert kis.sell_calls == 1
    assert retried["order_result"] == "ORDER_SKIPPED_NO_SIGNAL"


def test_partial_fill_cancel_retries_only_claimed_remaining_target(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis()
    engine, position = _sell_case(db, kis)
    _run_sell(engine, position)

    orders = OrdersRepo(db)
    order = orders.list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]
    requested_qty = int(order["qty"])
    filled_qty = 2
    orders.record_execution_claim_for_order(
        order["client_order_key"],
        state="PARTIALLY_FILLED",
        cumulative_filled_qty=filled_qty,
        authoritative=True,
    )
    orders.mark_cancelled(
        "practice", order["client_order_key"], {"tot_ccld_qty": str(filled_qty)},
    )
    orders.record_execution_claim_for_order(
        order["client_order_key"],
        state="CANCELLED",
        cumulative_filled_qty=filled_qty,
        authoritative=True,
    )
    _project_pb1_exit_stage_truth(
        orders_repo=orders,
        positions_repo=PositionsRepo(db),
        env="practice",
        code="123450",
        strategy="pb1_pullback_close",
        source_order=order,
        request_json=order["request_json"],
    )

    retry_engine, retry_position = _sell_case(db, kis, holding_qty=18)
    retried = _run_sell(retry_engine, retry_position)

    assert retried["submitted"] == 1
    assert kis.sell_calls == 2
    assert kis.sell_quantities[-1] == requested_qty - filled_qty


def test_unresolved_lower_priority_sell_blocks_hard_stop_until_authoritative_remaining_qty(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = UnresolvedThenAcceptedSellKis()
    first_engine, position = _sell_case(db, kis)

    first = _run_sell(first_engine, position)
    prior_order = OrdersRepo(db).list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]
    requested_qty = int(prior_order["qty"])
    assert requested_qty > 1
    assert first["decision_reason"] == "KR_TAKE_PROFIT_TP1"
    assert prior_order["status"] == "UNRESOLVED_ACK"
    assert kis.sell_calls == 1

    emergency_position = dict(
        position,
        last_price=90.0,
        stop_price=95.0,
        orderable_qty=20,
        kis_qty=20,
    )
    emergency_balance = {
        "output1": [{
            "pdno": "123450",
            "hldg_qty": "20",
            "ord_psbl_qty": "20",
            "pchs_avg_pric": "100",
        }],
        "output2": [{}],
    }
    emergency_engine = _engine(
        db, kis, phase="manage", balance_snapshot=emergency_balance,
    )
    emergency_engine._resolve_price_with_fallback = lambda *_args, **_kwargs: (90.0, "test")
    blocked = emergency_engine._plan_exit_event(
        emergency_position,
        {"close": 90.0, "ma20": 101.0, "ma50": 99.0},
        pd.DataFrame(),
        "day",
    )

    assert blocked["decision_reason"] == "EXIT_HARD_STOP"
    assert blocked.get("reconcile_required") == 1, {
        key: blocked.get(key) for key in (
            "order_result", "order_skip_reasons", "submit_attempted", "submitted",
            "orderable_qty", "sell_qty", "execution_integrity_error",
        )
    }
    assert blocked["order_result"] == "ORDER_SKIPPED_DURABLE_SESSION_BLOCK"
    assert kis.sell_calls == 1

    filled_qty = 1
    orders = OrdersRepo(db)
    orders.mark_cancelled(
        "practice", prior_order["client_order_key"],
        {"tot_ccld_qty": str(filled_qty)},
    )
    orders.record_execution_claim_for_order(
        prior_order["client_order_key"],
        state="CANCELLED",
        cumulative_filled_qty=filled_qty,
        authoritative=True,
    )

    remaining_qty = 20 - filled_qty
    remaining_engine, remaining_position = _sell_case(
        db, kis, holding_qty=remaining_qty,
    )
    remaining_position.update(
        last_price=90.0,
        stop_price=95.0,
        orderable_qty=remaining_qty,
        kis_qty=remaining_qty,
    )
    remaining_engine._resolve_price_with_fallback = lambda *_args, **_kwargs: (90.0, "test")
    terminal_emergency = remaining_engine._plan_exit_event(
        remaining_position,
        {"close": 90.0, "ma20": 101.0, "ma50": 99.0},
        pd.DataFrame(),
        "day",
    )

    assert terminal_emergency["decision_reason"] == "EXIT_HARD_STOP"
    assert terminal_emergency["submitted"] == 1
    assert kis.sell_calls == 2
    assert kis.sell_quantities[-1] == remaining_qty


def test_acknowledged_sibling_sell_block_requests_reconciliation_before_hard_stop(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis()
    first_engine, position = _sell_case(db, kis)

    first = _run_sell(first_engine, position)
    prior_order = OrdersRepo(db).list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]
    assert first["submitted"] == 1
    assert prior_order["status"] == "ACKED"
    assert kis.sell_calls == 1

    emergency_position = dict(
        position,
        last_price=90.0,
        stop_price=95.0,
        orderable_qty=20,
        kis_qty=20,
    )
    emergency_balance = {
        "output1": [{
            "pdno": "123450",
            "hldg_qty": "20",
            "ord_psbl_qty": "20",
            "pchs_avg_pric": "100",
        }],
        "output2": [{}],
    }
    emergency_engine = _engine(
        db, kis, phase="manage", balance_snapshot=emergency_balance,
    )
    emergency_engine._resolve_price_with_fallback = lambda *_args, **_kwargs: (90.0, "test")
    blocked = emergency_engine._plan_exit_event(
        emergency_position,
        {"close": 90.0, "ma20": 101.0, "ma50": 99.0},
        pd.DataFrame(),
        "day",
    )

    assert blocked["decision_reason"] == "EXIT_HARD_STOP"
    assert blocked["reconcile_required"] == 1
    assert blocked["order_result"] == "ORDER_SKIPPED_DURABLE_SESSION_BLOCK"
    assert kis.sell_calls == 1


def test_pb1_semantic_sell_ledger_read_failure_fails_closed(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis()
    engine, position = _sell_case(db, kis)
    original = OrdersRepo.list_today_orders
    calls = 0

    def fail_semantic_lookup(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("SELL ledger unavailable")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(OrdersRepo, "list_today_orders", fail_semantic_lookup)

    result = _run_sell(engine, position)

    assert calls == 2
    assert kis.sell_calls == 0
    assert result["reconcile_required"] == 1
    assert result["order_result"] == "SEMANTIC_SELL_LEDGER_UNAVAILABLE"


def test_pb1_sell_claim_persistence_failure_blocks_broker_submit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis()
    engine, position = _sell_case(db, kis)

    def unavailable_claim(*_args, **_kwargs):
        raise OSError("execution claim store unavailable")

    monkeypatch.setattr(OrdersRepo, "claim_execution_action", unavailable_claim)
    result = _run_sell(engine, position)

    assert kis.sell_calls == 0
    assert result["order_result"] == "EXECUTION_CLAIM_UNAVAILABLE"
    assert result["order_skip_reasons"] == ["EXECUTION_CLAIM_UNAVAILABLE"]



def test_add_on_auth_reject_is_retryable_next_tick_with_new_key(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    parent = _prepare_parent_position(db)
    kis = AuthRejectThenAcceptBuyKis()

    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)
    assert kis.buy_calls == 1

    with db.connect() as conn:
        first_rows = list(
            conn.execute(
                sa.select(schema_for_engine(db).orders).where(
                    schema_for_engine(db).orders.c.stage == "PB1-ADD"
                )
            ).mappings()
        )
    assert len(first_rows) == 1
    assert first_rows[0]["status"] == "ERROR"
    assert first_rows[0]["submitted_at"] is None

    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)
    assert kis.buy_calls == 2

    with db.connect() as conn:
        rows = list(
            conn.execute(
                sa.select(schema_for_engine(db).orders)
                .where(schema_for_engine(db).orders.c.stage == "PB1-ADD")
                .order_by(schema_for_engine(db).orders.c.created_at)
            ).mappings()
        )
    assert len(rows) == 2
    assert rows[0]["client_order_key"] != rows[1]["client_order_key"]
    assert rows[1]["status"] == "ACKED"
    with db.connect() as conn:
        fill_count = conn.execute(sa.select(sa.func.count()).select_from(schema_for_engine(db).fills)).scalar_one()
        stored = dict(
            conn.execute(
                sa.select(schema_for_engine(db).positions).where(
                    schema_for_engine(db).positions.c.code == "005930"
                )
            ).mappings().one()
        )
    assert fill_count == 0
    assert stored["qty"] == 10
    assert int(stored["pyramid_level"] or 0) == 0
    assert float(stored["stop_price"] or 0.0) == 95.0



def _position_row(db):
    with db.connect() as conn:
        return dict(
            conn.execute(
                sa.select(schema_for_engine(db).positions).where(
                    schema_for_engine(db).positions.c.code == "005930"
                )
            ).mappings().one()
        )


def _add_order_row(db):
    with db.connect() as conn:
        return dict(
            conn.execute(
                sa.select(schema_for_engine(db).orders)
                .where(schema_for_engine(db).orders.c.stage == "PB1-ADD")
                .order_by(schema_for_engine(db).orders.c.created_at.desc())
            ).mappings().first()
        )


def _fill_count(db):
    with db.connect() as conn:
        return int(
            conn.execute(
                sa.select(sa.func.count()).select_from(schema_for_engine(db).fills)
            ).scalar_one()
        )


def test_add_on_ack_only_does_not_create_fill_or_advance_stage(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    parent = _prepare_parent_position(db)
    kis = AckOnlyBuyKis()

    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)

    order = _add_order_row(db)
    stored = _position_row(db)
    assert order["status"] == "ACKED"
    assert kis.buy_calls == 1
    assert _fill_count(db) == 0
    assert stored["qty"] == 10
    assert int(stored["pyramid_level"] or 0) == 0
    assert float(stored["stop_price"] or 0.0) == 95.0
    assert stored.get("last_add_price") in {None, 0, 0.0}


def test_add_on_partial_then_full_reconcile_advances_stage_only_at_full_fill(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    parent = _prepare_parent_position(db)
    kis = AckOnlyBuyKis()
    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)

    orders = OrdersRepo(db)
    fills = FillsRepo(db)
    positions = PositionsRepo(db)

    # One of two shares filled at an implied 102.0. Physical qty changes,
    # but pyramid stage/stop must remain unchanged.
    partial = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=datetime(2026, 9, 23, 13, 5, tzinfo=ZoneInfo("Asia/Seoul")),
        holdings_rows=[{
            "pdno": "005930",
            "hldg_qty": "11",
            "pchs_avg_pric": str((1000.0 + 102.0) / 11.0),
        }],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )
    stored_partial = _position_row(db)
    order_partial = _add_order_row(db)
    assert partial["fills"] == 1
    assert order_partial["status"] == "PARTIAL_FILLED"
    assert stored_partial["qty"] == 11
    assert int(stored_partial["pyramid_level"] or 0) == 0
    assert float(stored_partial["stop_price"] or 0.0) == 95.0

    # Simulate process restart: new repo instances over the same durable DB.
    orders = OrdersRepo(db)
    fills = FillsRepo(db)
    positions = PositionsRepo(db)
    full = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=datetime(2026, 9, 23, 13, 6, tzinfo=ZoneInfo("Asia/Seoul")),
        holdings_rows=[{
            "pdno": "005930",
            "hldg_qty": "12",
            "pchs_avg_pric": str((1000.0 + 204.0) / 12.0),
        }],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )
    stored_full = _position_row(db)
    order_full = _add_order_row(db)
    assert full["fills"] == 1
    assert order_full["status"] == "FILLED"
    assert stored_full["qty"] == 12
    assert int(stored_full["pyramid_level"] or 0) == 1
    assert abs(float(stored_full["last_add_price"]) - 102.0) < 1e-6
    assert abs(float(stored_full["stop_price"]) - 99.5) < 1e-6
    assert _fill_count(db) == 2


def test_add_on_partial_then_cancel_preserves_actual_qty_without_stage_advance(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    parent = _prepare_parent_position(db)
    kis = AckOnlyBuyKis()
    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)

    orders = OrdersRepo(db)
    fills = FillsRepo(db)
    positions = PositionsRepo(db)
    _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=datetime(2026, 9, 23, 13, 5, tzinfo=ZoneInfo("Asia/Seoul")),
        holdings_rows=[{
            "pdno": "005930",
            "hldg_qty": "11",
            "pchs_avg_pric": str((1000.0 + 102.0) / 11.0),
        }],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )

    order = _add_order_row(db)
    with db.begin() as conn:
        conn.execute(
            sa.update(schema_for_engine(db).orders)
            .where(schema_for_engine(db).orders.c.order_id == order["order_id"])
            .values(status="CANCELLED")
        )

    # Restart/reconcile after cancellation: no phantom second share and no
    # pyramid-stage/stop mutation.
    result = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=datetime(2026, 9, 23, 13, 7, tzinfo=ZoneInfo("Asia/Seoul")),
        holdings_rows=[{
            "pdno": "005930",
            "hldg_qty": "11",
            "pchs_avg_pric": str((1000.0 + 102.0) / 11.0),
        }],
        orders_repo=OrdersRepo(db),
        fills_repo=FillsRepo(db),
        positions_repo=PositionsRepo(db),
    )
    stored = _position_row(db)
    assert result["fills"] == 0
    assert stored["qty"] == 11
    assert int(stored["pyramid_level"] or 0) == 0
    assert float(stored["stop_price"] or 0.0) == 95.0
    assert _fill_count(db) == 1


def test_add_on_explicit_business_reject_is_retryable_even_after_submitted_timestamp(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    db = _new_db()
    parent = _prepare_parent_position(db)
    kis = ExplicitRejectThenAcceptBuyKis()

    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)
    assert kis.buy_calls == 1

    with db.connect() as conn:
        first = dict(
            conn.execute(
                sa.select(schema_for_engine(db).orders)
                .where(schema_for_engine(db).orders.c.stage == "PB1-ADD")
            ).mappings().one()
        )
    assert first["status"] == "ERROR"
    assert first["submitted_at"] is not None
    assert first["response_json"]["rt_cd"] == "1"

    _engine(db, kis)._place_add_on(parent, qty=2, price=101.0)
    assert kis.buy_calls == 2

    with db.connect() as conn:
        rows = list(
            conn.execute(
                sa.select(schema_for_engine(db).orders)
                .where(schema_for_engine(db).orders.c.stage == "PB1-ADD")
                .order_by(schema_for_engine(db).orders.c.created_at)
            ).mappings()
        )
    assert len(rows) == 2
    assert rows[0]["client_order_key"] != rows[1]["client_order_key"]
    assert rows[1]["status"] == "ACKED"


def test_pb1_sell_execution_lifecycle_uses_position_cycle_not_sid_mode(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    db = _new_db()
    kis = RetryableSellKis()
    engine, position = _sell_case(db, kis)

    _run_sell(engine, position)

    order = OrdersRepo(db).list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )[0]
    request_json = order["request_json"]
    assert request_json["position_lifecycle_id"] == str(position["position_cycle_id"])
    assert request_json["position_lifecycle_id"] != "sid:1:mode:1"


def test_pb1_entry_retry_qty_is_capped_to_durable_remaining_target(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setattr(
        OrdersRepo,
        "get_retryable_execution_action_snapshot",
        lambda *_args, **_kwargs: SimpleNamespace(remaining_target_qty=2),
    )

    class CaptureQtyKis(AckOnlyBuyKis):
        def __init__(self):
            super().__init__()
            self.quantities = []

        def buy_stock_limit(self, code: str, qty: int, price: float):
            self.quantities.append(qty)
            return super().buy_stock_limit(code, qty, price)

    db = _new_db()
    kis = CaptureQtyKis()
    candidate = _build_candidate("018260")
    candidate.planned_qty = 5

    result = _engine(db, kis)._place_entry(candidate)
    order = OrdersRepo(db).get_order_by_client_order_key(
        "practice", candidate.client_order_key
    )

    assert result["api_submitted"] == 1
    assert kis.quantities == [2]
    assert order["qty"] == 2
    assert order["request_json"]["requested_qty"] == 2
    assert order["request_json"]["entry_plan"]["qty"] == 2
