from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision
from trader.execution_state import exit_stage_for_reason
from trader.kr.pb1_stability import normalize_sell_reason_family


class FakeKis:
    def __init__(self) -> None:
        self.sell_calls = 0
        self.sell_quantities: list[int] = []
        self.balance_invalidations: list[tuple[str, list[str]]] = []

    def sell_stock_market(self, code: str, qty: int) -> dict:
        self.sell_calls += 1
        self.sell_quantities.append(qty)
        return {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": f"S-{code}-{qty}"}}

    def invalidate_balance_cache(self, *, reason: str, codes: list[str] | None = None) -> None:
        self.balance_invalidations.append((reason, list(codes or [])))


class _NoopUniverseRepo:
    pass


def _make_engine(db=None, kis=None, balance_snapshot=None) -> tuple[PB1Engine, FakeKis]:
    db = db or sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    kis = kis or FakeKis()
    engine = PB1Engine(
        universe_repo=_NoopUniverseRepo(),
        orders_repo=OrdersRepo(db),
        fills_repo=FillsRepo(db),
        positions_repo=PositionsRepo(db),
        ledger_repo=LedgerEventsRepo(db),
        kis=kis,
        window=WindowDecision(name="day", phase="manage"),
        window_label="day",
        phase="manage",
        dry_run=False,
        intended_live=True,
        env="practice",
        run_id="test",
        order_allowed=True,
        trading_day=True,
        now_kst_value=datetime(2026, 8, 6, 13, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        balance_snapshot=balance_snapshot,
        balance_source="api" if balance_snapshot else None,
    )
    return engine, kis


def test_sell_ack_survives_new_pb1_engine_instance(monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    kis = FakeKis()
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "14", "ord_psbl_qty": "14",
                             "pchs_avg_pric": "271660"}], "output2": [{"ord_psbl_cash": "0"}]}
    positions = PositionsRepo(db)
    persisted, created = positions.get_or_create_imported_cycle_for_kis_holding(
        env="practice", strategy="pb1_pullback_close", account_id="practice:unknown",
        sid=1, mode=1, code="010060", market="J", qty=14, avg_price=271660,
    )
    assert created
    pos = _pos(code="010060", qty=14, kis_qty=14, orderable_qty=14)
    pos.update(avg_buy_price=271660.0, last_price=260000.0, stop_price=265000.0,
               position_cycle_id=str(persisted["position_cycle_id"]),
               portfolio_epoch_id=str(persisted["portfolio_epoch_id"]),
               position_meta={"position_cycle_id": str(persisted["position_cycle_id"])})
    engine1, _ = _make_engine(db, kis, balance)
    first = engine1._plan_exit_event(pos, {"close": 260000.0}, pd.DataFrame(), "day")
    assert first["submitted"] == 1
    assert kis.sell_calls == 1
    del engine1
    engine2, _ = _make_engine(db, kis, balance)
    second = engine2._plan_exit_event(pos, {"close": 260000.0}, pd.DataFrame(), "day")
    assert kis.sell_calls == 1
    assert second["order_result"] == "ORDER_SKIPPED_DURABLE_SESSION_BLOCK"
    rows = OrdersRepo(db).list_today_orders("practice", side="SELL", code="010060", status_exclude=())
    assert str(rows[0]["position_cycle_id"]) == str(persisted["position_cycle_id"])


def test_three_pb1_engines_reuse_one_persisted_imported_cycle():
    db = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(db)
    schema.metadata.create_all(db)
    kis = FakeKis()
    with db.begin() as conn:
        conn.execute(sa.insert(schema.positions).values(
            position_id="legacy-engine-pos", position_cycle_id="legacy-cycle-oci",
            portfolio_epoch_id="legacy-epoch-oci", opened_at=datetime(2026, 5, 27),
            position_origin="RECOVERY", env="practice", strategy="pb1_pullback_close",
            sid=1, mode=1, code="010060", market="J", qty=14, avg_buy_price=271660,
            total_cost=3803240, realized_pnl=0, status="OPEN", max_price=397500,
            last_trail_stop=334107, position_meta={"holding_bars": 68},
        ))
    balance_rows = [{"pdno": "010060", "hldg_qty": "14", "ord_psbl_qty": "14",
                     "pchs_avg_pric": "271660", "prpr": "267000"}]
    cycles = []
    for _ in range(3):
        engine, _ = _make_engine(db, kis)
        ledger = PositionsRepo(db).list_positions_by_codes(
            env="practice", strategy="pb1_pullback_close", codes=["010060"])
        contexts = engine._build_holding_contexts_from_balance_rows(balance_rows, ledger)
        cycles.append(contexts[0].position_meta["position_cycle_id"])
    assert len(set(map(str, cycles))) == 1
    with db.begin() as conn:
        open_rows = list(conn.execute(sa.select(schema.positions).where(
            sa.and_(schema.positions.c.code == "010060", schema.positions.c.status == "OPEN"))).mappings())
    assert len(open_rows) == 1
    assert str(open_rows[0]["position_cycle_id"]) == str(cycles[0])
    assert str(open_rows[0]["portfolio_epoch_id"]) != "legacy-epoch-oci"


def test_changing_full_exit_reason_cannot_bypass_durable_guard():
    assert exit_stage_for_reason("TRAIL_STOP_HIT") == "FULL_EXIT"
    assert exit_stage_for_reason("MA20_BREAK") == "FULL_EXIT"
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    repo = OrdersRepo(db)
    repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=14,
        limit_price=None, stage="FULL_EXIT", client_order_key="reason-change",
        request_json={"exit_reason": "TRAIL_STOP_HIT", "trade_session": "day"}, status="ACKED",
        position_cycle_id="cycle-x",
    )
    engine, _ = _make_engine(db, FakeKis())
    blocked, _ = engine._durable_sell_block(
        code="010060", position_cycle_id="cycle-x",
        exit_stage=exit_stage_for_reason("MA20_BREAK"),
    )
    assert blocked


def test_filled_tp1_allows_tp2_or_emergency_full_exit_with_fresh_remaining_balance():
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    OrdersRepo(db).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=None, stage="TP1", client_order_key="tp1-filled",
        request_json={"exit_reason": "TAKE_PROFIT_1", "trade_session": "day"}, status="FILLED",
        position_cycle_id="cycle-x",
    )
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7",
                             "pchs_avg_pric": "271660"}], "output2": [{}]}
    engine, _ = _make_engine(db, FakeKis(), balance)
    blocked, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-x", exit_stage="TP2")
    assert not blocked
    blocked_full, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-x", exit_stage="FULL_EXIT")
    assert not blocked_full


def test_production_partial_reason_stage_mapping_and_reason_family():
    assert exit_stage_for_reason("ABS_TP1_10PCT") == "TP1"
    assert exit_stage_for_reason("PROFIT_PROTECT_8PCT") == "PROFIT_PROTECT_PARTIAL_1"
    assert exit_stage_for_reason("DEFENSE_RISK_OFF_TRIM") == "DEFENSE_TRIM_1"
    assert exit_stage_for_reason("EXIT_HARD_STOP") == "FULL_EXIT"
    assert normalize_sell_reason_family("EXIT_HARD_STOP") == "HARD_STOP"
    assert normalize_sell_reason_family("TRAIL_STOP_HIT") == "TRAIL_STOP_HIT"
    assert normalize_sell_reason_family("PROFIT_PROTECT_8PCT") == "PROFIT_CAPTURE"


def test_profit_protect_fill_allows_later_emergency_full_exit():
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    OrdersRepo(db).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=None, stage="PROFIT_PROTECT_PARTIAL_1", client_order_key="profit-filled",
        request_json={"exit_reason": "PROFIT_PROTECT_8PCT", "trade_session": "day"},
        status="FILLED", position_cycle_id="cycle-x",
    )
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7",
                             "pchs_avg_pric": "271660"}], "output2": [{}]}
    engine, _ = _make_engine(db, FakeKis(), balance)
    blocked, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-x",
                                             exit_stage=exit_stage_for_reason("EXIT_HARD_STOP"))
    assert not blocked


def _hard_stop_after_prior_partial(monkeypatch, *, prior_stage: str, prior_status: str):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    OrdersRepo(db).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=None, stage=prior_stage, client_order_key=f"prior-{prior_stage}-{prior_status}",
        request_json={"exit_stage": prior_stage, "trade_session": "day",
                      "pre_order_holding_qty": 14, "requested_qty": 7, "submitted_qty": 7},
        status=prior_status, position_cycle_id="cycle-x",
    )
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7",
                             "pchs_avg_pric": "271660"}], "output2": [{}]}
    kis = FakeKis()
    kis.sell_calls = 1
    kis.sell_quantities = [7]
    engine, _ = _make_engine(db, kis, balance)
    pos = _pos(code="010060", qty=7, kis_qty=7, orderable_qty=7)
    pos.update(avg_buy_price=271660.0, last_price=260000.0, stop_price=265000.0,
               position_cycle_id="cycle-x", position_meta={"position_cycle_id": "cycle-x"})
    result = engine._plan_exit_event(pos, {"close": 260000.0}, pd.DataFrame(), "day")
    return result, kis


def test_tp1_filled_then_hard_stop_allows_remaining_full_exit(monkeypatch):
    result, kis = _hard_stop_after_prior_partial(monkeypatch, prior_stage="TP1", prior_status="FILLED")
    assert result["submitted"] == 1
    assert kis.sell_calls == 2 and kis.sell_quantities == [7, 7]


def test_pending_tp1_then_hard_stop_still_blocks_duplicate_sell(monkeypatch):
    result, kis = _hard_stop_after_prior_partial(monkeypatch, prior_stage="TP1", prior_status="ACKED")
    assert result["order_result"] == "ORDER_SKIPPED_DURABLE_SESSION_BLOCK"
    assert kis.sell_calls == 1


def test_tp1_qty_confirmed_price_unresolved_then_full_exit(monkeypatch):
    result, kis = _hard_stop_after_prior_partial(
        monkeypatch, prior_stage="TP1", prior_status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED")
    assert result["submitted"] == 1 and kis.sell_calls == 2


def test_tp2_filled_then_hard_stop_allows_remaining_full_exit(monkeypatch):
    result, kis = _hard_stop_after_prior_partial(monkeypatch, prior_stage="TP2", prior_status="FILLED")
    assert result["submitted"] == 1 and kis.sell_calls == 2


def _pos(*, code: str, qty: int, kis_qty: int, orderable_qty: int) -> dict:
    return {
        "code": code,
        "name": code,
        "mode": 1,
        "sid": 1,
        "qty": qty,
        "kis_qty": kis_qty,
        "orderable_qty": orderable_qty,
        "avg_buy_price": 10000.0,
        "last_price": 9000.0,
        "market": "J",
        "stop_price": 11000.0,
        "holding_days": 1,
        "entry_date": "2026-08-05",
        "holding_source": "kis_balance",
    }


def test_no_sellable_qty_blocks_session_and_skips_sell_api() -> None:
    engine, kis = _make_engine()
    code = "005830"
    engine._register_session_no_sellable(code=code, reason="KIS_NO_SELLABLE_QTY")

    payload = engine._plan_exit_event(
        _pos(code=code, qty=1, kis_qty=0, orderable_qty=0),
        {"close": 9000.0},
        pd.DataFrame(),
        "day",
    )

    assert payload is not None
    assert payload["order_result"] == "ORDER_SKIPPED_SESSION_BLOCKED"
    assert "KIS_NO_SELLABLE_QTY" in payload.get("order_skip_reasons", [])
    assert kis.sell_calls == 0
    assert code in engine._session_sell_blocked_codes


def test_sell_accepted_in_session_prevents_resubmit() -> None:
    engine, kis = _make_engine()
    code = "005830"
    engine._register_session_sell_accepted(code=code, qty=1, price=9000.0, order_id="ODNO-1")

    payload = engine._plan_exit_event(
        _pos(code=code, qty=1, kis_qty=1, orderable_qty=1),
        {"close": 9000.0},
        pd.DataFrame(),
        "day",
    )

    assert payload is not None
    assert payload["order_result"] == "ORDER_SKIPPED_SESSION_BLOCKED"
    assert "SELL_ACCEPTED" in payload.get("order_skip_reasons", [])
    assert kis.sell_calls == 0


def test_db_kis_mismatch_pending_close_status_blocks_sell(monkeypatch) -> None:
    engine, kis = _make_engine()
    code = "005830"
    monkeypatch.setattr(engine, "_has_sell_accepted_today", lambda _code: True)

    payload = engine._plan_exit_event(
        _pos(code=code, qty=1, kis_qty=0, orderable_qty=0),
        {"close": 9000.0},
        pd.DataFrame(),
        "day",
    )

    assert payload is not None
    assert payload["order_result"] in {"ORDER_SKIPPED_POSITION_MISMATCH", "ORDER_SKIPPED_SESSION_BLOCKED"}
    assert kis.sell_calls == 0


def test_sell_ack_invalidates_balance_and_blocks_resubmit(monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine, kis = _make_engine()
    code = "005830"

    sell_position = _pos(code=code, qty=1, kis_qty=1, orderable_qty=1)
    sell_position["stop_price"] = 9500.0
    first = engine._plan_exit_event(
        sell_position,
        {"close": 9000.0},
        pd.DataFrame(),
        "day",
    )

    assert first is not None and first["submitted"] == 1
    assert first["order_result"] == "ORDER_OK"
    assert kis.sell_calls == 1
    assert kis.balance_invalidations == [(f"sell_ack:{code}", [code])]
    assert engine._balance_snapshot is None
    second = engine._plan_exit_event(
        sell_position,
        {"close": 9000.0},
        pd.DataFrame(),
        "day",
    )
    assert second["order_result"] == "ORDER_SKIPPED_SESSION_BLOCKED"
    assert kis.sell_calls == 1
