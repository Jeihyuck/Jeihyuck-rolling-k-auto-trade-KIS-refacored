"""PR257 real PB1 practice SELL intent / broker ACK / claim / restart contract.

Uses the existing SQLite durable orders+positions+claims fixture and fake KIS.
No network, real account or live KIS orders are used.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from tests.kr.test_pr144_pb1_ambiguous_order_paths import (
    AmbiguousSellKis, RetryableSellKis, _new_db, _sell_case,
)
from trader.db.repos import OrdersRepo, PositionsRepo
from trader.reconcile_kis import _project_pb1_exit_stage_truth


def _setup(monkeypatch, db, kis, *, mark=96.8, prior_soft_tick=False):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (True, "ok"))
    monkeypatch.setattr("trader.pb1_engine._position_policy_missing_contract", lambda *_args: False)
    monkeypatch.setattr("trader.pb1_engine._resolve_position_horizon", lambda *_args, **_kwargs: "SWING")
    monkeypatch.setattr("trader.pb1_engine._resolve_position_book", lambda *_args, **_kwargs: "SWING_BOOK")
    monkeypatch.setattr("trader.pb1_engine._horizon_to_exit_family",
                        lambda *_args, **_kwargs: "SWING_STAGED_EXIT")
    monkeypatch.setattr("trader.pb1_engine._resolve_swing_staged_exit",
                        lambda *_args, **_kwargs: {
                            "exit_ok": False, "reason": "NO_SWING_EXIT",
                            "qty": 0, "sell_pct": None, "update_meta": {},
                        })
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "0")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("KR_PB1_PAPER_STOP36_ENABLED", "1")
    monkeypatch.setenv("KR_SWING_BLOCK_SAME_DAY_SOFT_EXIT", "0")
    engine, position = _sell_case(db, kis, holding_qty=20)
    engine._resolve_price_with_fallback = lambda *_a, **_kw: (mark, "test")
    position.update(
        strategy="pb1_pullback_close",
        trade_horizon="SWING",
        entry_style_selected="ENTRY_PULLBACK",
        exit_policy_family="SWING_STAGED_EXIT",
        stop_price_at_entry=92.0,
        initial_stop=92.0,
        entry_exit_plan_json={
            "exit_policy_family": "SWING_STAGED_EXIT",
            "risk_plan": {"risk_R": 8.0, "initial_stop": 92.0},
        },
    )
    position["position_meta"] = dict(position.get("position_meta") or {})
    if prior_soft_tick:
        position["position_meta"].update({
            "kr_paper_stop36_cycle": str(position["position_cycle_id"]),
            "kr_paper_stop36_last_tick": "previous-independent-tick",
            "kr_paper_stop36_breach_ticks": 1,
        })
    return engine, position


def _tick(engine, position, *, mark):
    return engine._plan_exit_event(
        position, {"close": mark, "ma20": 78.0, "ma50": 70.0},
        pd.DataFrame(), "day",
    )


def _stored(db, position):
    return PositionsRepo(db).get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="123450", position_cycle_id=str(position["position_cycle_id"]),
        portfolio_epoch_id=str(position["portfolio_epoch_id"]),
    )


def _order(db):
    rows = OrdersRepo(db).list_today_orders(
        "practice", side="SELL", code="123450", status_exclude=(),
    )
    assert len(rows) == 1
    return rows[0]


def test_soft_stop_two_tick_trigger_submits_exact_half_once_and_waits_for_claim(monkeypatch):
    db = _new_db()
    kis = RetryableSellKis()
    engine, pos = _setup(monkeypatch, db, kis, prior_soft_tick=True)
    result = _tick(engine, pos, mark=96.8)
    assert result.get("submitted") == 1
    assert result["decision_reason"] == "KR_PB1_SOFT_STOP_3PCT"
    assert kis.sell_quantities == [10]
    order = _order(db)
    assert int(order["qty"]) == 10
    assert order["request_json"]["exit_stage"] == "KR_PAPER_SOFT_STOP_PARTIAL_1"
    assert order["request_json"]["execution_meta_update"]["kr_paper_stop36_soft_done"] is True
    assert not _stored(db, pos)["position_meta"].get("kr_paper_stop36_soft_done")

    # The same accepted economic order cannot be resubmitted after restart.
    restarted, again = _setup(monkeypatch, db, kis, prior_soft_tick=True)
    retried = _tick(restarted, again, mark=96.8)
    assert kis.sell_quantities == [10]
    assert retried.get("submitted", 0) == 0

    orders = OrdersRepo(db)
    orders.record_execution_claim_for_order(
        order["client_order_key"], state="PARTIALLY_FILLED",
        cumulative_filled_qty=4, authoritative=True,
    )
    _project_pb1_exit_stage_truth(
        orders_repo=orders, positions_repo=PositionsRepo(db),
        env="practice", code="123450", strategy="pb1_pullback_close",
        source_order=order, request_json=order["request_json"],
    )
    assert not _stored(db, pos)["position_meta"].get("kr_paper_stop36_soft_done")

    orders.record_execution_claim_for_order(
        order["client_order_key"], state="FILLED",
        cumulative_filled_qty=10, authoritative=True,
    )
    _project_pb1_exit_stage_truth(
        orders_repo=orders, positions_repo=PositionsRepo(db),
        env="practice", code="123450", strategy="pb1_pullback_close",
        source_order=order, request_json=order["request_json"],
    )
    completed = _stored(db, pos)
    assert completed["position_meta"]["kr_paper_stop36_soft_done"] is True
    assert completed["position_meta"]["kr_paper_stop36_soft_done_cycle"] == pos["position_cycle_id"]
    db.dispose()


def test_hard_stop_six_percent_submits_full_quantity_immediately(monkeypatch):
    db = _new_db()
    kis = RetryableSellKis()
    engine, pos = _setup(monkeypatch, db, kis, mark=94.0)
    result = _tick(engine, pos, mark=94.0)
    assert result["decision_reason"] == "EXIT_HARD_STOP"
    assert result.get("submitted") == 1
    assert kis.sell_quantities == [20]
    assert _order(db)["request_json"]["exit_stage"] == "FULL_EXIT"
    db.dispose()


def test_ambiguous_soft_sell_ack_is_durable_and_not_marked_complete(monkeypatch):
    db = _new_db()
    kis = AmbiguousSellKis()
    engine, pos = _setup(monkeypatch, db, kis, prior_soft_tick=True)
    first = _tick(engine, pos, mark=96.8)
    assert first.get("reconcile_required") == 1
    assert kis.sell_quantities == [10]
    assert _order(db)["status"] == "UNRESOLVED_ACK"
    assert not _stored(db, pos)["position_meta"].get("kr_paper_stop36_soft_done")
    restarted, again = _setup(monkeypatch, db, kis, prior_soft_tick=True)
    second = _tick(restarted, again, mark=96.8)
    assert second.get("submitted", 0) == 0
    assert kis.sell_quantities == [10]
    db.dispose()


def test_soft_stop_completion_can_only_follow_the_matching_reason():
    # A later overlay/protective hard exit must never carry an unrelated
    # -3% one-shot completion flag into its durable request_json.
    import inspect
    from trader.pb1_engine import PB1Engine
    source = inspect.getsource(PB1Engine._plan_exit_event)
    assert 'str(exit_eval.primary_reason or "").upper() != "KR_PB1_SOFT_STOP_3PCT"' in source
    assert 'exit_execution_meta_update.pop("kr_paper_stop36_soft_done", None)' in source
