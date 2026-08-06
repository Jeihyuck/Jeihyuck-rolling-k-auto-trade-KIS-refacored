from datetime import datetime
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import CandidateFeature, PB1Engine
from trader.window_router import WindowDecision


class FakeKis:
    def __init__(self) -> None:
        self.buy_calls = 0

    def buy_stock_limit(self, code: str, qty: int, price: float) -> dict:
        self.buy_calls += 1
        return {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": f"O-{code}-{qty}"}}


def _make_engine() -> PB1Engine:
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
        now_kst_value=datetime(2026, 8, 6, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )


def _build_candidate(code: str) -> CandidateFeature:
    cf = CandidateFeature(
        code=code,
        market="J",
        features={
            "entry_style_selected": "PULLBACK",
            "entry_reason": "ENTRY_PULLBACK",
            "close": 10000.0,
            "entry_price": 10000.0,
            "order_price": 10000.0,
            "stop_price": 9500.0,
            "initial_stop": 9500.0,
            "trigger_policy": "PULLBACK_OVERRIDE",
            "setup_passed": True,
            "risk_passed": True,
            "sizing_passed": True,
            "buyable_passed": True,
            "risk_reasons": ["ok"],
            "buyable_reasons": ["ok"],
            "sizing_reason": "SIZING_OK",
            "risk_ok": True,
            "sizing_ok": True,
            "buyable_ok": True,
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
        planned_qty=1,
        client_order_key=f"test-{code}",
    )
    cf.setup_passed = True
    cf.risk_passed = True
    cf.sizing_passed = True
    cf.buyable_passed = True
    cf.authoritative_gate_passed = True
    cf.sizing_reason = "SIZING_OK"
    return cf


def test_entry_pullback_candidates_do_not_hard_block(monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = _make_engine()

    for code in ("086280", "005830"):
        cf = _build_candidate(code)
        plan = engine._build_entry_plan(
            cf,
            entry_price=10000.0,
            order_price=10000.0,
            stop_price=9500.0,
            trigger_ok=False,
            trigger_info={"reason": "pullback"},
            entry_mode="OR",
            stage="PB1-AM",
            price_source="test",
        )
        cf.entry_plan = plan
        cf.features["entry_plan"] = plan

        ok, reasons, _diag = engine._assert_authoritative_order_candidate(cf=cf, gate_snapshot={"kis_holding_qty": 0})
        assert ok is True
        assert "RISK_NOT_PASSED" not in reasons
        assert "SIZING_NOT_PASSED" not in reasons

        status = engine._place_entry(cf)
        assert status["api_submitted"] == 1


def test_hard_block_diagnostic_contains_gate_state_fields(caplog) -> None:
    engine = _make_engine()
    cf = _build_candidate("086280")
    cf.risk_passed = False
    cf.sizing_passed = False
    cf.features["risk_passed"] = False
    cf.features["sizing_passed"] = False
    cf.features["risk_ok"] = False
    cf.features["sizing_ok"] = False
    cf.features["sizing_reason"] = "ORDER_PX_ABOVE_POSITION_CAP"

    plan = engine._build_entry_plan(
        cf,
        entry_price=10000.0,
        order_price=10000.0,
        stop_price=9500.0,
        trigger_ok=False,
        trigger_info={"reason": "pullback"},
        entry_mode="OR",
        stage="PB1-AM",
        price_source="test",
    )
    cf.entry_plan = plan
    cf.features["entry_plan"] = plan

    engine._place_entry(cf)

    hard_block_logs = [rec.message for rec in caplog.records if "[ORDER][HARD_BLOCK][AUTHORITATIVE_GATE]" in rec.message]
    assert hard_block_logs
    line = hard_block_logs[-1]
    assert "source=" in line
    assert "setup_passed=" in line
    assert "risk_passed=" in line
    assert "sizing_passed=" in line
    assert "buyable_passed=" in line
    assert "planned_qty=" in line
    assert "sizing_reason=" in line
