from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kis_wrapper import KisAuthError, KisOrderOutcomeUnknown
from trader.pb1_engine import PB1Engine
from trader.reconcile_kis import _promote_open_buy_orders_from_holdings
from trader.trade_plan import build_entry_exit_plan
from trader.window_router import WindowDecision
from tests.kr.test_kr_entry_authoritative_gate_state import _build_candidate


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
    schema_for_engine(db).metadata.create_all(db)
    return db


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
        account_id="acct",
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
        account_id="acct",
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
        account_id="practice:unknown",
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
