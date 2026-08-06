from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


class FakeKis:
    def __init__(self) -> None:
        self.sell_calls = 0

    def sell_stock_market(self, code: str, qty: int) -> dict:
        self.sell_calls += 1
        return {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": f"S-{code}-{qty}"}}


class _NoopUniverseRepo:
    pass


def _make_engine() -> tuple[PB1Engine, FakeKis]:
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    kis = FakeKis()
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
    )
    return engine, kis


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
