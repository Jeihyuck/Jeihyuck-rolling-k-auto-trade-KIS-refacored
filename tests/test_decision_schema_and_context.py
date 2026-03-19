from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.decision_schema import build_entry_evaluation, build_exit_evaluation
from trader.time_utils import resolve_trade_context


def test_resolve_trade_context_returns_locked_prev_close() -> None:
    now = datetime(2026, 3, 19, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    result = resolve_trade_context(now=now, env="practice")

    assert result["trade_date"] == "2026-03-19"
    assert result["as_of"] == "2026-03-18"
    assert result["calendar_prev"] == "2026-03-18"
    assert result["is_locked"] is True


def test_build_entry_evaluation_standardizes_reason_and_trigger_policy() -> None:
    evaluation = build_entry_evaluation(
        code="5830",
        as_of="2026-03-18",
        trade_date="2026-03-19",
        input_source="final30_locked",
        setup_ok=True,
        score_ok=True,
        risk_ok=True,
        sizing_ok=True,
        buyable_ok=False,
        trigger_ok=False,
        order_ready=False,
        reasons=["BUYABLE_TODAY_BUY_EXISTS"],
        setup_family="BREAKOUT",
        decision_reason="BUYABLE_TODAY_BUY_EXISTS",
        features={"entry_style_selected": "BREAKOUT"},
    )

    assert evaluation["code"] == "005830"
    assert evaluation["decision"] == "SKIP"
    assert evaluation["decision_family"] == "ENTRY_BREAKOUT"
    assert evaluation["decision_reason"] == "BUYABLE_TODAY_BUY_EXISTS"
    assert evaluation["entry_trigger_policy"] == "NONE"


def test_build_exit_evaluation_standardizes_skip_reason() -> None:
    evaluation = build_exit_evaluation(
        code="032830",
        as_of="2026-03-18",
        trade_date="2026-03-19",
        holding_qty=1,
        avg_price=231500,
        last_price=230500,
        entry_date="2026-03-19",
        days_held=0,
        stop_loss_hit=False,
        trailing_stop_hit=False,
        ma20_break=False,
        ma50_break=False,
        time_stop_hit=False,
        risk_off_hit=False,
        exit_ok=False,
        reasons=[],
        decision_reason="NO_EXIT_SIGNAL",
    )

    assert evaluation["decision"] == "SKIP"
    assert evaluation["decision_family"] == "SKIP"
    assert evaluation["decision_reason"] == "NO_EXIT_SIGNAL"