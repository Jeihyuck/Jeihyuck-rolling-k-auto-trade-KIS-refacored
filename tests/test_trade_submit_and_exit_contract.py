from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import logging

import pandas as pd
import sqlalchemy as sa

from trader.data.ohlcv_provider import OHLCVResult
from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import CandidateFeature, HoldingContext, PB1Engine
from trader.window_router import WindowDecision


KST = ZoneInfo("Asia/Seoul")


class FakeKis:
    def buy_stock_limit(self, code: str, qty: int, price: float) -> dict:
        return {
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {"ODNO": f"ODNO-{code}-{qty}-{int(price)}"},
        }


def _new_db_engine():
    db_engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(db_engine)
    schema.metadata.create_all(db_engine)
    return db_engine


def _make_engine(*, db_engine, now_kst: datetime, dry_run: bool, intended_live: bool, kis: object | None):
    orders_repo = OrdersRepo(db_engine)
    fills_repo = FillsRepo(db_engine)
    positions_repo = PositionsRepo(db_engine)
    ledger_repo = LedgerEventsRepo(db_engine)
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=orders_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        ledger_repo=ledger_repo,
        kis=kis,
        window=WindowDecision(name="day", phase="entry"),
        window_label="day",
        phase="entry",
        dry_run=dry_run,
        env="practice",
        run_id="run-test",
        intended_live=intended_live,
        now_kst_value=now_kst,
        compute_only_full_run=True,
        trading_day=True,
        order_allowed=True,
    )
    return engine, orders_repo, fills_repo, positions_repo, ledger_repo


def _make_candidate(client_order_key: str) -> CandidateFeature:
    return CandidateFeature(
        code="141080",
        market="KOSDAQ",
        features={
            "entry_reason": "ENTRY_PULLBACK",
            "entry_style_selected": "ENTRY_PULLBACK",
            "entry_decision_family": "ENTRY_PULLBACK",
            "entry_trigger_policy": "NONE",
            "entry_price": 1000.0,
            "close": 1000.0,
            "stop_price": 950.0,
            "score": 91.0,
            "planned_cap": 1000.0,
            "buyable_ok": True,
        },
        setup_ok=True,
        reasons=["ok"],
        mode=1,
        mode_reasons=[],
        client_order_key=client_order_key,
        planned_qty=1,
    )


def test_retryable_existing_intent_uses_retry_key_and_submits(monkeypatch, caplog):
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 25, 14, 0, tzinfo=KST)
    engine, orders_repo, _fills_repo, _positions_repo, _ledger_repo = _make_engine(
        db_engine=db_engine,
        now_kst=now_kst,
        dry_run=False,
        intended_live=True,
        kis=FakeKis(),
    )
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))

    base_key = engine._client_order_key("141080", 1, "BUY", "close", "PB1")
    orders_repo.create_intent_idempotent(
        env=engine.env,
        run_id=engine.run_id,
        strategy=engine.STRATEGY_NAME,
        sid=1,
        mode=1,
        code="141080",
        market="KOSDAQ",
        side="BUY",
        ord_type="LIMIT",
        qty=1,
        limit_price=1000.0,
        stage="PB1-CLOSE",
        client_order_key=base_key,
        request_json={},
        status="CREATED",
    )
    engine._buyable_gate_context = {
        "141080": {
            "holding_qty": 0,
            "today_buy_exists": False,
            "today_submit_exists": True,
            "today_fill_exists": False,
            "open_order_exists": False,
            "cooldown_active": False,
            "last_buy_event_at": None,
            "last_fill_event_at": None,
            "last_order_submit_at": now_kst.isoformat(),
        }
    }
    cf = _make_candidate(base_key)

    caplog.set_level(logging.INFO)
    status = engine._place_entry(cf)

    assert status["api_submitted"] == 1
    assert status["accepted"] == 1
    assert status["terminal_event"] == "API_RESULT"
    assert cf.client_order_key != base_key
    assert ":retry" in cf.client_order_key
    assert "[ORDER][PRE_SUBMIT][RETRY_KEY]" in caplog.text
    assert "[ORDER][API_REQUEST] code=141080" in caplog.text
    assert "[ORDER][API_RESULT] code=141080" in caplog.text
    assert "[ORDER][FINAL_SKIP] code=141080" not in caplog.text


def test_run_exit_always_logs_ma_values_for_holdings(caplog):
    db_engine = _new_db_engine()
    now_kst = datetime(2026, 3, 25, 15, 10, tzinfo=KST)
    engine, _orders_repo, _fills_repo, _positions_repo, _ledger_repo = _make_engine(
        db_engine=db_engine,
        now_kst=now_kst,
        dry_run=True,
        intended_live=False,
        kis=None,
    )

    dates = pd.date_range(end=now_kst.date(), periods=60, freq="B")
    closes = pd.Series(range(100, 160), dtype=float)
    df = pd.DataFrame(
        {
            "date": dates,
            "open": closes - 1,
            "high": closes + 1,
            "low": closes - 2,
            "close": closes,
            "volume": 100000,
        }
    )

    engine.ohlcv_provider.get_ohlcv = lambda *args, **kwargs: OHLCVResult(df=df.copy(), meta={"source": "db", "rows": len(df)})
    engine._exit_holdings_meta = {"source": "db_positions"}
    holdings = [
        HoldingContext(
            code="032830",
            name="",
            holding_qty=1,
            orderable_qty=1,
            avg_price=120.0,
            last_price=159.0,
            market_value=159.0,
            unrealized_pnl=39.0,
            unrealized_pct=32.5,
            source="db_positions",
            market="KOSDAQ",
            mode=1,
            sid=1,
            entry_date="2026-03-10",
            days_held=15,
            last_fill_at=now_kst.isoformat(),
            position_meta={"code": "032830", "market": "KOSDAQ", "mode": 1, "sid": 1},
        )
    ]

    caplog.set_level(logging.INFO)
    evaluations = engine._run_exit_always(holdings_for_exit=holdings, marks_fallback={})

    assert len(evaluations) == 1
    assert "[EXIT][OHLCV][SOURCE] code=032830 source=db rows=60" in caplog.text
    assert "[EXIT][MA_CTX] code=032830 ma20=None" not in caplog.text
    assert "[EXIT][MA_CTX] code=032830" in caplog.text
    assert "source=db" in caplog.text