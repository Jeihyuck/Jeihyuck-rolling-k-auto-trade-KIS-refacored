import pytest

from trader.pb1_engine import CandidateFeature, PB1Engine


class DummyLedgerRepo:
    def append_event(self, **_payload):
        pass


def make_engine():
    return PB1Engine(
        universe_repo=object(),
        orders_repo=object(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=DummyLedgerRepo(),
        kis=None,
        dry_run=True,
        env="practice",
        run_id="test",
        window_label="morning",
        phase="entry",
    )


def test_pullback_override_builds_valid_entry_plan():
    engine = make_engine()
    cf = CandidateFeature(
        code="000660",
        market="J",
        features={
            "close": 2628000.0,
            "order_price": 2628000.0,
            "entry_price": 2628000.0,
            "stop_price": 2175678.57,
            "initial_stop": 2175678.57,
            "score": 103.29,
            "entry_style_selected": "PULLBACK",
            "pullback_ok": True,
            "pivot": 0.0,
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
    )
    cf.planned_qty = 1

    plan = engine._build_entry_plan(
        cf,
        entry_price=2628000.0,
        order_price=2628000.0,
        stop_price=2175678.57,
        trigger_ok=False,
        trigger_info={"reason": "vcp_fail"},
        entry_mode="OR",
        stage="PB1-AM",
        price_source="daily_close_fallback",
    )

    ok, reasons = engine._validate_entry_plan(plan)

    assert ok is True
    assert reasons == []
    assert plan["entry_style"] == "PULLBACK"
    assert plan["entry_family"] == "ENTRY_PULLBACK"
    assert plan["trigger_policy"] == "PULLBACK_OVERRIDE"
    assert plan["qty"] == 1


def test_entry_plan_validation_never_returns_unknown():
    engine = make_engine()
    ok, reasons = engine._validate_entry_plan(None)

    assert ok is False
    assert reasons
    assert "unknown" not in ",".join(reasons).lower()


def test_entry_stage_not_close_for_am_pm():
    engine = make_engine()
    engine.window_internal = "morning"
    assert engine._entry_stage_name() == "PB1-AM"

    engine.window_internal = "afternoon"
    assert engine._entry_stage_name() == "PB1-PM"

@pytest.mark.parametrize(
    ("style", "trigger_ok", "decision_family", "expected_trigger_policy"),
    [
        ("PULLBACK", False, "ENTRY_PULLBACK_OVERRIDE", "PULLBACK_OVERRIDE"),
        ("PULLBACK", True, "ENTRY_BREAKOUT_CONFIRMED", "PULLBACK_OVERRIDE"),
        ("BREAKOUT", False, "ENTRY_BREAKOUT", "BREAKOUT_TRIGGER"),
        ("BREAKOUT", True, "ENTRY_BREAKOUT_CONFIRMED", "BREAKOUT_TRIGGER"),
        ("MOMENTUM", False, "ENTRY_MOMENTUM_CONTINUATION", "MOMENTUM_CONTINUATION"),
        ("MOMENTUM", True, "ENTRY_BREAKOUT_CONFIRMED", "MOMENTUM_CONTINUATION"),
    ],
)
def test_final30_setup_identity_survives_live_trigger(style, trigger_ok, decision_family, expected_trigger_policy):
    engine = make_engine()
    family = f"ENTRY_{style}"
    cf = CandidateFeature(
        code="005930",
        market="J",
        features={
            "close": 100000.0,
            "order_price": 100000.0,
            "entry_price": 100000.0,
            "stop_price": 95000.0,
            "initial_stop": 95000.0,
            "score": 90.0,
            "entry_style_selected": style,
            "entry_reason": family,
            "entry_decision_family": decision_family,
            "entry_trigger_policy": "BREAKOUT_CONFIRMED" if trigger_ok else "SETUP_OVERRIDE",
            "breakout_score": 80.0,
            "pullback_score": 80.0,
            "momentum_score": 80.0,
            "pivot": 99000.0,
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=["pb1_from_final30"],
    )
    cf.planned_qty = 1

    plan = engine._build_entry_plan(
        cf,
        entry_price=100000.0,
        order_price=100000.0,
        stop_price=95000.0,
        trigger_ok=trigger_ok,
        trigger_info={"reason": "breakout_confirmed" if trigger_ok else "not_confirmed"},
        entry_mode="OR",
        stage="PB1-AM",
        price_source="test",
    )

    assert plan["entry_style"] == style
    assert plan["entry_family"] == family
    assert plan["entry_reason"] == family
    assert plan["trigger_policy"] == expected_trigger_policy
    assert cf.features["entry_style_selected"] == style
    assert cf.features["entry_reason"] == family
    assert cf.features["entry_decision_family"] == decision_family

    prepared = engine._prepare_entry_exit_plan(cf, entry_price_for_plan=100000.0)
    assert prepared is not None
    frozen, entry_meta = prepared
    assert frozen["entry_style_selected"] == family
    assert frozen["entry_reason"] == family
    assert entry_meta["entry_decision_family"] == decision_family
    assert entry_meta["entry_trigger_policy"] == ("BREAKOUT_CONFIRMED" if trigger_ok else "SETUP_OVERRIDE")


def test_momentum_breakout_confirmation_keeps_momentum_frozen_sell_plan():
    engine = make_engine()
    cf = CandidateFeature(
        code="005930",
        market="J",
        features={
            "close": 100000.0,
            "order_price": 100000.0,
            "entry_price": 100000.0,
            "stop_price": 95000.0,
            "initial_stop": 95000.0,
            "score": 90.0,
            "entry_style_selected": "MOMENTUM",
            "entry_reason": "ENTRY_MOMENTUM",
            "entry_decision_family": "ENTRY_BREAKOUT_CONFIRMED",
            "entry_trigger_policy": "BREAKOUT_CONFIRMED",
            "momentum_score": 90.0,
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=["pb1_from_final30"],
    )
    cf.planned_qty = 1

    engine._build_entry_plan(
        cf,
        entry_price=100000.0,
        order_price=100000.0,
        stop_price=95000.0,
        trigger_ok=True,
        trigger_info={"reason": "breakout_confirmed"},
        entry_mode="OR",
        stage="PB1-AM",
        price_source="test",
    )
    frozen, entry_meta = engine._prepare_entry_exit_plan(cf, entry_price_for_plan=100000.0)

    assert frozen["entry_style_selected"] == "ENTRY_MOMENTUM"
    assert frozen["entry_reason"] == "ENTRY_MOMENTUM"
    assert frozen["entry_thesis"] == "MOMENTUM_RECLAIM"
    assert frozen["trade_horizon"] == "DAY_TRADE"
    assert frozen["exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"
    assert entry_meta["entry_decision_family"] == "ENTRY_BREAKOUT_CONFIRMED"
    assert entry_meta["entry_trigger_policy"] == "BREAKOUT_CONFIRMED"

