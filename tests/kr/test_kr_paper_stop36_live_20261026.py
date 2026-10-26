"""Real KR PB1 paper-only 3/6 stop signals and durable stage contracts."""
from datetime import datetime
from zoneinfo import ZoneInfo
import inspect

from trader.kr.paper_stop36_policy import (
    POLICY_VERSION, evaluate_kr_pb1_paper_stop36,
)
from trader.execution_state import exit_stage_for_reason, legal_next_exit_stage
from trader.kr.pb1_stability import normalize_sell_reason_family
from trader.kr.exit_guards import same_day_soft_exit_block
from trader.pb1_engine import PB1Engine


def pos(**overrides):
    p = {
        "env": "practice", "strategy": "pb1_pullback_close",
        "exit_policy_family": "SWING_STAGED_EXIT", "code": "053800",
        "position_cycle_id": "cycle-1", "portfolio_epoch_id": "portfolio-1",
        "avg_buy_price": 100.0, "stop_price_at_entry": 92.0,
        "orderable_qty": 10, "qty": 10, "position_meta": {},
        "entry_exit_plan_json": {
            "exit_policy_family": "SWING_STAGED_EXIT",
            "risk_plan": {"risk_R": 8.0, "initial_stop": 92.0},
        },
    }
    p.update(overrides)
    return p


def eval_(p, mark, tick):
    return evaluate_kr_pb1_paper_stop36(
        p, mark=mark, env="practice",
        strategy_owner="pb1_pullback_close", tick_id=tick,
    )


def test_soft_two_distinct_ticks_half_quantity_then_broker_done_blocks_repeat():
    p = pos()
    first = eval_(p, 96.9, "tick-1")
    assert first.eligible and first.soft_breach_count == 1
    assert not first.soft_confirmed and not first.hard_touched
    p["position_meta"].update(first.update_meta)
    replay = eval_(p, 96.8, "tick-1")
    assert replay.soft_breach_count == 1 and not replay.soft_confirmed
    second = eval_(p, 96.8, "tick-2")
    assert second.soft_confirmed and second.sell_qty == 5
    assert second.sell_pct == 0.5
    assert "kr_paper_stop36_soft_done" not in second.update_meta
    p["position_meta"].update(second.update_meta)
    p["position_meta"]["kr_paper_stop36_soft_done"] = True
    p["position_meta"]["kr_paper_stop36_soft_done_cycle"] = "cycle-1"
    third = eval_(p, 96.7, "tick-3")
    assert third.eligible and not third.soft_confirmed


def test_soft_counter_resets_when_price_recovers_and_cannot_leak_new_cycle():
    p = pos()
    p["position_meta"].update(eval_(p, 96.9, "tick-1").update_meta)
    reset = eval_(p, 98.0, "tick-2")
    assert reset.soft_breach_count == 0
    p["position_meta"].update(reset.update_meta)
    assert eval_(p, 96.9, "tick-3").soft_breach_count == 1
    p["position_cycle_id"] = "cycle-2"
    assert eval_(p, 96.9, "tick-4").soft_breach_count == 1


def test_hard_six_percent_is_immediate_full_and_old_tighter_stop_wins():
    p = pos()
    hard = eval_(p, 94.0, "tick-1")
    assert hard.hard_touched and hard.sell_qty == 10
    assert hard.effective_hard_stop == 94.0
    tighter = eval_(pos(stop_price_at_entry=97.0), 96.9, "tick-1")
    assert tighter.hard_touched and tighter.effective_hard_stop == 97.0
    assert p["entry_exit_plan_json"]["risk_plan"]["risk_R"] == 8.0


def test_no_other_owner_or_real_account_or_missing_ledger_can_activate():
    for change in (
        {"code": "122630"},
        {"strategy": "KR_INFINITE"},
        {"exit_policy_family": "CORE_TREND_FOLLOW"},
        {"position_cycle_id": ""},
        {"portfolio_epoch_id": ""},
        {"entry_exit_plan_json": {}},
    ):
        assert not eval_(pos(**change), 90.0, "tick-1").eligible
    p = pos()
    assert not evaluate_kr_pb1_paper_stop36(
        p, mark=90, env="real", strategy_owner="pb1_pullback_close",
        tick_id="tick-1").eligible
    assert not evaluate_kr_pb1_paper_stop36(
        p, mark=90, env="practice", strategy_owner="KR_INFINITE",
        tick_id="tick-1").eligible


def test_partial_stage_is_distinct_and_can_transition_to_full_protection():
    assert exit_stage_for_reason(
        "KR_PB1_SOFT_STOP_3PCT",
        requested_sell_qty=5, broker_qty_before=10, sell_pct=.5,
    ) == "KR_PAPER_SOFT_STOP_PARTIAL_1"
    assert exit_stage_for_reason(
        "EXIT_HARD_STOP", requested_sell_qty=10, broker_qty_before=10,
    ) == "FULL_EXIT"
    assert legal_next_exit_stage("KR_PAPER_SOFT_STOP_PARTIAL_1", "FULL_EXIT")
    assert normalize_sell_reason_family("KR_PB1_SOFT_STOP_3PCT") == "SWING_STAGED_EXIT"


def test_same_day_soft_guard_still_applies_hard_does_not():
    now = datetime(2026, 10, 26, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    bought = datetime(2026, 10, 26, 9, 15, tzinfo=ZoneInfo("Asia/Seoul"))
    assert same_day_soft_exit_block(
        now_kst=now, bought_at=bought,
        exit_reason="KR_PB1_SOFT_STOP_3PCT",
    )[0] is True
    assert same_day_soft_exit_block(
        now_kst=now, bought_at=bought,
        exit_reason="EXIT_HARD_STOP",
    )[0] is False


def test_real_pb1_exit_path_routes_policy_and_waits_for_fill_to_mark_done():
    src = inspect.getsource(PB1Engine._plan_exit_event)
    assert 'evaluate_kr_pb1_paper_stop36(' in src
    assert 'self.env == "practice"' in src
    assert 'exit_policy_family == "SWING_STAGED_EXIT"' in src
    assert "kr_paper_stop36_soft_done" in src
    assert "kr_paper_stop36_soft_done_cycle" in src
    assert src.index('evaluate_kr_pb1_paper_stop36(') < src.index('create_intent_idempotent(')
    assert POLICY_VERSION == "KR_PAPER_PB1_SWING_STOP_3_6_V1"
