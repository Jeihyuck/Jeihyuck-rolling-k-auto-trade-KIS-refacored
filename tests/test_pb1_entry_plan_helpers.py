from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from trader.kr.pb1.entry_plan import build_entry_plan, infer_entry_family, validate_entry_plan_with_window
from trader.kr.pb1.run_context_state import resolve_run_context_state
from trader.kr.pb1.window_state import resolve_window_internal
from trader.pb1_engine import CandidateFeature, PB1Engine


def test_infer_entry_family_prefers_selected_style() -> None:
    style, family, trigger = infer_entry_family(
        {"entry_style_selected": "BREAKOUT", "breakout_ok": True},
        trigger_ok=False,
        trigger_info={"reason": "ok"},
    )
    assert (style, family, trigger) == ("BREAKOUT", "ENTRY_BREAKOUT", "BREAKOUT_TRIGGER")


def test_build_entry_plan_matches_wrapper(monkeypatch) -> None:
    fixed_now = datetime(2026, 7, 2, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    monkeypatch.setattr("trader.kr.pb1.entry_plan.now_kst", lambda: fixed_now)

    gate_state = {
        "setup_passed": True,
        "risk_passed": True,
        "sizing_passed": True,
        "buyable_passed": True,
        "authoritative_gate_passed": True,
        "sizing_reason": "SIZING_OK",
        "planned_qty": 1,
        "risk_reasons": ["ok"],
        "buyable_reasons": ["ok"],
    }
    features = {
        "entry_style_selected": "PULLBACK",
        "entry_reason": "ENTRY_PULLBACK",
        "close": 2560000,
        "entry_price": 2560000,
        "order_price": 2560000,
        "stop_price": 2107678.57,
        "initial_stop": 2107678.57,
        "trigger_policy": "PULLBACK_OVERRIDE",
        "setup_passed": True,
        "risk_passed": True,
        "sizing_passed": True,
        "buyable_passed": True,
        "risk_ok": True,
        "sizing_ok": True,
        "buyable_ok": True,
        "sizing_reason": "SIZING_OK",
        "risk_reasons": ["ok"],
        "buyable_reasons": ["ok"],
    }
    dummy_cf = SimpleNamespace(code="000660", market="J", features=features.copy(), planned_qty=1, score=0.0)
    helper_plan = build_entry_plan(
        cf=dummy_cf,
        gate_state=gate_state,
        entry_price=2560000,
        order_price=2560000,
        stop_price=2107678.57,
        trigger_ok=False,
        trigger_info={},
        entry_mode="",
        stage="PB1-AM",
        price_source="test",
    )

    engine = PB1Engine.__new__(PB1Engine)
    engine.window_internal = "morning"
    engine.phase = "entry"
    engine._ensure_candidate_authoritative_gate_state = lambda cf: gate_state
    cf = CandidateFeature(
        code="000660",
        market="J",
        features=features.copy(),
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
        planned_qty=1,
    )
    wrapper_plan = engine._build_entry_plan(
        cf,
        entry_price=2560000,
        order_price=2560000,
        stop_price=2107678.57,
        trigger_ok=False,
        trigger_info={},
        entry_mode="",
        stage="PB1-AM",
        price_source="test",
    )

    assert helper_plan == wrapper_plan


def test_validate_entry_plan_with_window_checks_close_stage() -> None:
    ok, reasons = validate_entry_plan_with_window(
        {
            "code": "000660",
            "side": "BUY",
            "stage": "PB1-CLOSE",
            "entry_style": "PULLBACK",
            "entry_family": "ENTRY_PULLBACK",
            "entry_reason": "ENTRY_PULLBACK",
            "trigger_policy": "PULLBACK_OVERRIDE",
            "entry_price": 1.0,
            "order_price": 1.0,
            "limit_price": 1.0,
            "stop_price": 0.5,
            "qty": 1,
        },
        window_internal="morning",
    )

    assert ok is False
    assert "intraday_buy_stage_must_not_be_close" in reasons


def test_resolve_run_context_state_matches_engine_init() -> None:
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=object(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=object(),
        kis=None,
        dry_run=True,
        env="practice",
        run_id="test",
        window_label="morning",
        phase="entry",
    )
    engine._today = "2026-07-02"

    helper_state = resolve_run_context_state(
        today="2026-07-02",
        as_of=None,
        trade_date=None,
        run_ctx={"as_of": "2026-07-01", "trade_date": "2026-07-02"},
        derived_as_of=None,
    )
    engine._init_run_context_state(
        as_of=None,
        trade_date=None,
        run_ctx={"as_of": "2026-07-01", "trade_date": "2026-07-02"},
        derived_as_of=None,
    )

    assert helper_state == {
        "as_of": engine._as_of,
        "trade_date": engine._trade_date,
        "as_of_source": engine._as_of_source,
    }


def test_resolve_window_internal_matches_engine_wrapper() -> None:
    warnings: list[tuple[str, str]] = []

    helper_window = resolve_window_internal(
        internal="morning",
        window_label="morning",
        warn_on_mismatch=lambda normalized, internal: warnings.append((normalized, internal)),
    )

    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=object(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=object(),
        kis=None,
        dry_run=True,
        env="practice",
        run_id="test",
        window_label="morning",
        phase="entry",
        now_kst_value=datetime(2026, 7, 2, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert helper_window == engine._resolve_window_internal()
    assert warnings == []
