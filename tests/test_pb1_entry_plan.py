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
