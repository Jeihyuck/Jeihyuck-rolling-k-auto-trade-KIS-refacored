from datetime import datetime
from zoneinfo import ZoneInfo

import logging
import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import CandidateFeature, PB1Engine
from trader.window_router import WindowDecision


KST = ZoneInfo("Asia/Seoul")


class FakeKis:
    def buy_stock_limit(self, code: str, qty: int, price: float) -> dict:
        return {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": f"ODNO-{code}-{qty}-{int(price)}"}}


def _new_engine() -> tuple[PB1Engine, PositionsRepo]:
    db_engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db_engine).metadata.create_all(db_engine)
    positions_repo = PositionsRepo(db_engine)
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=OrdersRepo(db_engine),
        fills_repo=FillsRepo(db_engine),
        positions_repo=positions_repo,
        ledger_repo=LedgerEventsRepo(db_engine),
        kis=FakeKis(),
        window=WindowDecision(name="day", phase="entry"),
        window_label="day",
        phase="entry",
        dry_run=False,
        env="practice",
        run_id="run-test",
        intended_live=True,
        now_kst_value=datetime(2026, 5, 4, 9, 6, tzinfo=KST),
        compute_only_full_run=True,
        trading_day=True,
        order_allowed=True,
    )
    return engine, positions_repo


def test_accepted_order_does_not_update_position_meta(monkeypatch, caplog):
    engine, positions_repo = _new_engine()
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    positions_repo.update_position_fields = lambda **kwargs: (_ for _ in ()).throw(AssertionError("position meta must not update on accepted-only"))

    cf = CandidateFeature(
        code="066970",
        market="KOSDAQ",
        features={
            "entry_reason": "ENTRY_PULLBACK",
            "entry_style_selected": "ENTRY_PULLBACK",
            "entry_decision_family": "ENTRY_PULLBACK",
            "entry_trigger_policy": "NONE",
            "entry_price": 214000.0,
            "close": 214000.0,
            "stop_price": 181392.857,
            "score": 90.0,
            "planned_cap": 214000.0,
            "buyable_ok": True,
        },
        setup_ok=True,
        reasons=["ok"],
        mode=1,
        mode_reasons=[],
        client_order_key="accepted-meta-test",
        planned_qty=1,
    )
    engine._buyable_gate_context = {
        "066970": {
            "holding_qty": 0,
            "kis_holding_qty": 0,
            "today_buy_exists": False,
            "today_submit_exists": False,
            "today_fill_exists": False,
            "today_sell_exists": False,
            "open_order_exists": False,
            "cooldown_active": False,
        }
    }

    caplog.set_level(logging.INFO)
    status = engine._place_entry(cf)

    assert status["accepted"] == 1
    assert "[POSITION][META][SKIP_ACCEPTED_ONLY] code=066970" in caplog.text