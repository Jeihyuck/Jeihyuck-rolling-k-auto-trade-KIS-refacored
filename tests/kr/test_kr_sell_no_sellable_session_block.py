from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
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


@pytest.mark.parametrize("status", ["REJECTED", "ACKED", "UNRESOLVED_ACK"])
def test_unconfirmed_tp1_never_unlocks_tp2(status):
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    OrdersRepo(db).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=None, stage="TP1", client_order_key=f"tp1-{status}",
        request_json={"trade_session": "day", "submitted_qty": 7, "pre_order_holding_qty": 14},
        status=status, position_cycle_id="cycle-x",
    )
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7"}], "output2": [{}]}
    engine, _ = _make_engine(db, FakeKis(), balance)
    blocked, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-x", exit_stage="TP2")
    assert blocked


@pytest.mark.parametrize("status", ["PARTIAL_FILLED", "FILLED"])
def test_confirmed_tp1_with_fresh_remaining_balance_unlocks_tp2(status):
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    OrdersRepo(db).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=None, stage="TP1", client_order_key=f"confirmed-{status}",
        request_json={"trade_session": "day", "submitted_qty": 7, "pre_order_holding_qty": 14},
        status=status, position_cycle_id="cycle-x",
    )
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "7", "ord_psbl_qty": "7"}], "output2": [{}]}
    engine, _ = _make_engine(db, FakeKis(), balance)
    blocked, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-x", exit_stage="TP2")
    assert not blocked


def test_failed_tp1_does_not_block_protective_full_exit():
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    OrdersRepo(db).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="010060", market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=None, stage="TP1", client_order_key="failed-protective",
        request_json={"trade_session": "day"}, status="REJECTED", position_cycle_id="cycle-x",
    )
    engine, _ = _make_engine(db, FakeKis(), {"output1": [{"pdno": "010060", "hldg_qty": "14", "ord_psbl_qty": "14"}], "output2": [{}]})
    blocked, _ = engine._durable_sell_block(code="010060", position_cycle_id="cycle-x", exit_stage="FULL_EXIT")
    assert not blocked


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
    assert payload["terminal_event"] == "FINAL_SKIP"
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
    assert payload["terminal_event"] == "FINAL_SKIP"
    assert kis.sell_calls == 0


def test_kr_pb1_sell_accepted_blocks_same_cycle_resubmit(monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    kis = FakeKis()
    balance = {"output1": [{"pdno": "010060", "hldg_qty": "14", "ord_psbl_qty": "14",
                             "pchs_avg_pric": "271660"}], "output2": [{"ord_psbl_cash": "0"}]}
    positions = PositionsRepo(db)
    persisted, _ = positions.get_or_create_imported_cycle_for_kis_holding(
        env="practice", strategy="pb1_pullback_close", account_id="practice:unknown",
        sid=1, mode=1, code="010060", market="J", qty=14, avg_price=271660,
    )
    pos = _pos(code="010060", qty=14, kis_qty=14, orderable_qty=14)
    pos.update(avg_buy_price=271660.0, last_price=260000.0, stop_price=265000.0,
               position_cycle_id=str(persisted["position_cycle_id"]),
               portfolio_epoch_id=str(persisted["portfolio_epoch_id"]),
               position_meta={"position_cycle_id": str(persisted["position_cycle_id"])})

    engine1, _ = _make_engine(db, kis, balance)
    first = engine1._plan_exit_event(pos, {"close": 260000.0}, pd.DataFrame(), "day")
    assert first["submitted"] == 1
    assert kis.sell_calls == 1

    engine2, _ = _make_engine(db, kis, balance)
    second = engine2._plan_exit_event(pos, {"close": 260000.0}, pd.DataFrame(), "day")
    assert second["order_result"] == "ORDER_SKIPPED_DURABLE_SESSION_BLOCK"
    assert kis.sell_calls == 1


def test_kr_pb1_no_sellable_qty_creates_durable_submit_fence() -> None:
    engine, kis = _make_engine()
    code = "005830"
    engine._register_session_no_sellable(code=code, reason="KIS_NO_SELLABLE_QTY")
    first = engine._plan_exit_event(
        _pos(code=code, qty=1, kis_qty=0, orderable_qty=0),
        {"close": 9000.0}, pd.DataFrame(), "day",
    )
    second = engine._plan_exit_event(
        _pos(code=code, qty=1, kis_qty=0, orderable_qty=0),
        {"close": 9000.0}, pd.DataFrame(), "day",
    )
    assert first["submitted"] == second["submitted"] == 0
    assert first["terminal_event"] == second["terminal_event"] == "FINAL_SKIP"
    assert "KIS_NO_SELLABLE_QTY" in first["order_skip_reasons"]
    assert code in engine._session_sell_blocked_codes
    assert kis.sell_calls == 0


def test_kr_pb1_no_sellable_qty_does_not_repeat_broker_submit() -> None:
    engine, kis = _make_engine()
    code = "005830"
    engine._register_session_no_sellable(code=code, reason="KIS_NO_SELLABLE_QTY")
    for _ in range(3):
        payload = engine._plan_exit_event(
            _pos(code=code, qty=1, kis_qty=0, orderable_qty=0),
            {"close": 9000.0}, pd.DataFrame(), "day",
        )
        assert payload["order_result"] == "ORDER_SKIPPED_SESSION_BLOCKED"
    assert kis.sell_calls == 0


def test_kr_pb1_fresh_sellable_positive_allows_retry(monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine, kis = _make_engine()
    code = "005830"
    engine._register_session_no_sellable(code=code)
    payload = engine._plan_exit_event(
        _pos(code=code, qty=1, kis_qty=1, orderable_qty=1),
        {"close": 9000.0}, pd.DataFrame(), "day",
    )
    assert payload["order_result"] != "ORDER_SKIPPED_SESSION_BLOCKED"
    assert code not in engine._session_sell_blocked_codes
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


def test_duplicate_durable_sell_row_never_reuses_old_baseline_for_new_broker_submit(monkeypatch) -> None:
    """2026-09-09 LG regression: created=False must be terminal before KIS submit."""
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    balance = {"output1": [{"pdno": "003550", "hldg_qty": "11", "ord_psbl_qty": "11",
                             "pchs_avg_pric": "111400"}], "output2": [{}]}
    engine, kis = _make_engine(balance_snapshot=balance)
    monkeypatch.setattr(engine, "_should_block_order", lambda *args, **kwargs: (False, None))
    monkeypatch.setattr(
        engine.orders_repo,
        "create_intent_idempotent",
        lambda **kwargs: ("existing-old-order-row", False),
    )

    pos = _pos(code="003550", qty=11, kis_qty=11, orderable_qty=11)
    pos.update(
        avg_buy_price=111400.0,
        last_price=100000.0,
        stop_price=105000.0,
        holding_source="kis_balance",
        position_cycle_id="lg-cycle",
        portfolio_epoch_id="lg-epoch",
        position_meta={"position_cycle_id": "lg-cycle"},
    )

    payload = engine._plan_exit_event(pos, {"close": 100000.0}, pd.DataFrame(), "day")

    assert payload is not None
    assert payload["submitted"] == 0
    assert payload["submit_attempted"] == 0
    assert "DUPLICATE_INTENT_ROW_REUSED" in payload["order_skip_reasons"]
    assert kis.sell_calls == 0


def test_explicit_policy_missing_high_profit_cannot_use_stale_swing_family(monkeypatch, caplog) -> None:
    """PR108/111 contract: high profit alone cannot bypass POLICY_MISSING hard-stop-only."""
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    balance = {"output1": [{"pdno": "067290", "hldg_qty": "10", "ord_psbl_qty": "10",
                             "pchs_avg_pric": "10000"}], "output2": [{}]}
    engine, kis = _make_engine(balance_snapshot=balance)
    monkeypatch.setattr(engine, "_resolve_price_with_fallback", lambda code, ohlcv_close=None: (16000.0, "test"))

    pos = _pos(code="067290", qty=10, kis_qty=10, orderable_qty=10)
    pos.update(
        avg_buy_price=10000.0,
        last_price=16000.0,
        stop_price=9000.0,
        entry_thesis="POLICY_MISSING",
        exit_policy_family="SWING_STAGED_EXIT",
        policy_source="missing",
        entry_exit_plan_json={},
        entry_meta_json={},
        position_meta={},
        holding_source="kis_balance",
    )

    caplog.set_level("INFO")
    payload = engine._plan_exit_event(
        pos,
        {"close": 16000.0, "ma20": 12000.0, "ma50": 11000.0},
        pd.DataFrame(),
        "day",
    )

    assert payload is not None
    assert payload["submitted"] == 0
    assert kis.sell_calls == 0
    assert "POLICY_MISSING_HARD_STOP_ONLY" in str(payload.get("decision_reason") or payload.get("reasons"))
    assert "[EXIT][POLICY_AUTHORITY][CONFLICT_BLOCKED]" in caplog.text


def test_explicit_policy_missing_still_allows_hard_stop(monkeypatch) -> None:
    """Single-authority fix must never disable the PR108/111 hard-stop lane."""
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    balance = {"output1": [{"pdno": "067290", "hldg_qty": "10", "ord_psbl_qty": "10",
                             "pchs_avg_pric": "10000"}], "output2": [{}]}
    engine, kis = _make_engine(balance_snapshot=balance)
    monkeypatch.setattr(engine, "_resolve_price_with_fallback", lambda code, ohlcv_close=None: (8500.0, "test"))

    pos = _pos(code="067290", qty=10, kis_qty=10, orderable_qty=10)
    pos.update(
        avg_buy_price=10000.0,
        last_price=8500.0,
        stop_price=9000.0,
        entry_thesis="POLICY_MISSING",
        exit_policy_family="SWING_STAGED_EXIT",
        policy_source="missing",
        entry_exit_plan_json={},
        entry_meta_json={},
        position_meta={},
        holding_source="kis_balance",
    )

    payload = engine._plan_exit_event(
        pos,
        {"close": 8500.0, "ma20": 10000.0, "ma50": 9800.0},
        pd.DataFrame(),
        "day",
    )

    assert payload is not None
    assert payload["submitted"] == 1
    assert kis.sell_calls == 1


def test_policy_missing_close_preserves_hard_stop_with_empty_plan(monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    balance = {"output1": [{"pdno": "067290", "hldg_qty": "10", "ord_psbl_qty": "10",
                             "pchs_avg_pric": "10000"}], "output2": [{}]}
    engine, kis = _make_engine(balance_snapshot=balance)
    monkeypatch.setattr(engine, "_resolve_price_with_fallback", lambda code, ohlcv_close=None: (8000.0, "test"))
    pos = _pos(code="067290", qty=10, kis_qty=10, orderable_qty=10)
    pos.update(
        avg_buy_price=10000.0,
        last_price=8000.0,
        stop_price=9000.0,
        entry_thesis="POLICY_MISSING",
        exit_policy_family="POLICY_MISSING",
        policy_source="missing",
        entry_exit_plan_json={},
        entry_meta_json={},
        position_meta={},
        holding_source="kis_balance",
    )

    payload = engine._plan_exit_event(
        pos,
        {"close": 8000.0, "ma20": 9000.0, "ma50": 9500.0},
        pd.DataFrame(),
        "close",
    )

    assert payload is not None
    assert payload["submitted"] == 1
    assert kis.sell_calls == 1
    assert payload["decision_reason"] == "EXIT_HARD_STOP"


def test_policy_missing_close_ignores_stale_day_force_exit_without_hard_stop(monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    balance = {"output1": [{"pdno": "095340", "hldg_qty": "10", "ord_psbl_qty": "10",
                             "pchs_avg_pric": "10000"}], "output2": [{}]}
    engine, kis = _make_engine(balance_snapshot=balance)
    monkeypatch.setattr(engine, "_resolve_price_with_fallback", lambda code, ohlcv_close=None: (12000.0, "test"))
    stale_day_plan = {
        "entry_thesis": "BREAKOUT_DAYTRADE",
        "trade_horizon": "DAY_TRADE",
        "exit_policy_family": "INTRADAY_PROFIT_PROTECT",
        "eod_action": "FORCE_EXIT",
        "force_eod_close": True,
    }
    pos = _pos(code="095340", qty=10, kis_qty=10, orderable_qty=10)
    pos.update(
        avg_buy_price=10000.0,
        last_price=12000.0,
        stop_price=9000.0,
        entry_thesis="POLICY_MISSING",
        exit_policy_family="POLICY_MISSING",
        policy_source="missing",
        entry_exit_plan_json=stale_day_plan,
        entry_meta_json={},
        position_meta={},
        holding_source="kis_balance",
    )

    payload = engine._plan_exit_event(
        pos,
        {"close": 12000.0, "ma20": 11000.0, "ma50": 10500.0},
        pd.DataFrame(),
        "close",
    )

    assert payload is not None
    assert payload["submitted"] == 0
    assert kis.sell_calls == 0
    assert payload["decision_reason"] == "POLICY_MISSING_HARD_STOP_ONLY"


def test_created_sell_intent_retries_with_new_key_and_fresh_baseline(monkeypatch) -> None:
    """A pre-submit CREATED row must not starve a later protective SELL."""
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    balance = {"output1": [{"pdno": "003550", "hldg_qty": "11", "ord_psbl_qty": "11",
                             "pchs_avg_pric": "111400"}], "output2": [{}]}
    engine, kis = _make_engine(db, FakeKis(), balance)
    code = "003550"
    base_key = engine._client_order_key(code, 1, "SELL", "day", "exit")
    engine.orders_repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code=code, market="J", side="SELL", ord_type="MARKET", qty=3,
        limit_price=120000.0, stage="FULL_EXIT", client_order_key=base_key,
        request_json={"pre_order_holding_qty": 99, "requested_qty": 3},
        status="CREATED", position_cycle_id="cycle-retry-created",
    )

    pos = _pos(code=code, qty=11, kis_qty=11, orderable_qty=11)
    pos.update(
        avg_buy_price=111400.0, last_price=100000.0, stop_price=105000.0,
        position_cycle_id="cycle-retry-created",
        position_meta={"position_cycle_id": "cycle-retry-created"},
    )
    payload = engine._plan_exit_event(pos, {"close": 100000.0}, pd.DataFrame(), "day")

    assert payload is not None
    assert payload["submitted"] == 1
    assert kis.sell_calls == 1
    retry_key = payload["client_order_key"]
    assert retry_key != base_key and retry_key.startswith(base_key + ":retry")
    old_row = engine.orders_repo.get_order_by_client_order_key("practice", base_key)
    retry_row = engine.orders_repo.get_order_by_client_order_key("practice", retry_key)
    assert old_row["status"] == "CREATED"
    assert retry_row["status"] == "ACKED"
    assert int((retry_row["request_json"] or {}).get("pre_order_holding_qty") or 0) == 11


def test_explicit_broker_rejection_can_retry_sell_with_new_key(monkeypatch) -> None:
    """Confirmed rejection is terminal evidence, so a later SELL may retry safely."""
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    balance = {"output1": [{"pdno": "005830", "hldg_qty": "7", "ord_psbl_qty": "7",
                             "pchs_avg_pric": "10000"}], "output2": [{}]}
    engine, kis = _make_engine(db, FakeKis(), balance)
    code = "005830"
    base_key = engine._client_order_key(code, 1, "SELL", "day", "exit")
    engine.orders_repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code=code, market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=9000.0, stage="FULL_EXIT", client_order_key=base_key,
        request_json={}, status="CREATED", position_cycle_id="cycle-retry-reject",
    )
    engine.orders_repo.mark_error(
        "practice", base_key,
        {"rt_cd": "1", "msg_cd": "TEMP_REJECT", "msg1": "confirmed broker rejection"},
    )

    pos = _pos(code=code, qty=7, kis_qty=7, orderable_qty=7)
    pos.update(position_cycle_id="cycle-retry-reject",
               position_meta={"position_cycle_id": "cycle-retry-reject"})
    payload = engine._plan_exit_event(pos, {"close": 9000.0}, pd.DataFrame(), "day")

    assert payload is not None
    assert payload["submitted"] == 1
    assert kis.sell_calls == 1
    assert payload["client_order_key"] != base_key


def test_ambiguous_sell_error_remains_fenced_against_duplicate_submit(monkeypatch) -> None:
    """Lost/unknown broker ACK must never be retried just because status is ERROR."""
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    balance = {"output1": [{"pdno": "005830", "hldg_qty": "7", "ord_psbl_qty": "7",
                             "pchs_avg_pric": "10000"}], "output2": [{}]}
    engine, kis = _make_engine(db, FakeKis(), balance)
    code = "005830"
    base_key = engine._client_order_key(code, 1, "SELL", "day", "exit")
    engine.orders_repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code=code, market="J", side="SELL", ord_type="MARKET", qty=7,
        limit_price=9000.0, stage="FULL_EXIT", client_order_key=base_key,
        request_json={}, status="CREATED", position_cycle_id="cycle-ambiguous",
    )
    engine.orders_repo.mark_submitted("practice", base_key, None, {"resp": None}, submitted_qty=7)
    engine.orders_repo.mark_error("practice", base_key, {"resp": None})

    pos = _pos(code=code, qty=7, kis_qty=7, orderable_qty=7)
    pos.update(position_cycle_id="cycle-ambiguous",
               position_meta={"position_cycle_id": "cycle-ambiguous"})
    payload = engine._plan_exit_event(pos, {"close": 9000.0}, pd.DataFrame(), "day")

    assert payload is not None
    assert payload["submitted"] == 0
    assert "DUPLICATE_INTENT_ROW_REUSED" in payload["order_skip_reasons"]
    assert kis.sell_calls == 0
