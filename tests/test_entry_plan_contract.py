from datetime import datetime
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import CandidateFeature, PB1Engine
from trader.window_router import WindowDecision
from trader.trade_plan import build_entry_exit_plan, seed_plan_fields_for_entry_style


class FakeKis:
    def buy_stock_limit(self, code: str, qty: int, price: float) -> dict:
        return {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": f"O-{code}-{qty}"}}


def make_engine():
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    return PB1Engine(
        universe_repo=object(),
        orders_repo=OrdersRepo(db),
        fills_repo=FillsRepo(db),
        positions_repo=PositionsRepo(db),
        ledger_repo=LedgerEventsRepo(db),
        kis=FakeKis(),
        window=WindowDecision(name="day", phase="entry"),
        window_label="day",
        phase="entry",
        dry_run=False,
        intended_live=True,
        env="practice",
        run_id="test",
        order_allowed=True,
        trading_day=True,
        now_kst_value=datetime(2026, 7, 2, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )


def test_entry_plan_survives_from_orderable_to_submit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = make_engine()
    cf = CandidateFeature(
        code="000660",
        market="J",
        features={
            "entry_style_selected": "PULLBACK",
            "entry_reason": "ENTRY_PULLBACK",
            "close": 2560000,
            "entry_price": 2560000,
            "order_price": 2560000,
            "stop_price": 2107678.57,
            "initial_stop": 2107678.57,
            "trigger_policy": "PULLBACK_OVERRIDE",
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
        planned_qty=1,
        client_order_key="test-key",
    )
    plan = engine._build_entry_plan(
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
    cf.entry_plan = plan
    cf.features["entry_plan"] = plan
    ok, reasons = engine._validate_entry_plan(cf.entry_plan)
    assert ok, reasons
    status = engine._place_entry(cf)
    assert status["api_submitted"] == 1
    assert status["submit_attempted"] == 1


def test_build_entry_exit_plan_accepts_raw_and_entry_styles():
    for style in [
        "PULLBACK", "BREAKOUT", "MOMENTUM", "VCP",
        "ENTRY_PULLBACK", "ENTRY_BREAKOUT", "ENTRY_MOMENTUM", "ENTRY_VCP",
    ]:
        plan = build_entry_exit_plan(
            code="000660",
            market="J",
            entry_style_selected=style,
            entry_reason=style,
            entry_price=2560000,
            features={"initial_stop": 2107678.57},
        ).to_dict()
        assert plan["entry_style_selected"].startswith("ENTRY_")
        assert plan["trade_horizon"] in {"SWING", "DAY_TRADE"}


def test_entry_exit_plan_fail_open_reaches_submit(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = make_engine()
    cf = CandidateFeature(
        code="000660", market="J",
        features={"entry_style_selected": "PULLBACK", "entry_reason": "ENTRY_PULLBACK", "close": 2560000, "entry_price": 2560000, "order_price": 2560000, "stop_price": 2107678.57, "initial_stop": 2107678.57, "trigger_policy": "PULLBACK_OVERRIDE"},
        setup_ok=True, reasons=[], mode=1, mode_reasons=[], planned_qty=1, client_order_key="test-key-exit-plan-fail",
    )
    cf.entry_plan = engine._build_entry_plan(cf, entry_price=2560000, order_price=2560000, stop_price=2107678.57, trigger_ok=False, trigger_info={}, entry_mode="", stage="PB1-AM", price_source="test")
    cf.features["entry_plan"] = cf.entry_plan
    status = engine._place_entry(cf)
    assert status["api_submitted"] == 1
    assert "entry_exit_plan" in cf.features


def test_seed_plan_fields_accepts_raw_styles():
    assert seed_plan_fields_for_entry_style("PULLBACK")["trade_horizon"] == "SWING"
    assert seed_plan_fields_for_entry_style("BREAKOUT")["trade_horizon"] == "DAY_TRADE"
    assert seed_plan_fields_for_entry_style("MOMENTUM")["trade_horizon"] == "DAY_TRADE"
    assert seed_plan_fields_for_entry_style("VCP")["trade_horizon"] == "SWING"
