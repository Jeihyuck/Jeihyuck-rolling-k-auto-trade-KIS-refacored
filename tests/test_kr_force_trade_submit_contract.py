from datetime import datetime
import subprocess
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kr.runner.trade_session_runner import normalize_kr_session_completion
from trader.pb1_engine import CandidateFeature, PB1Engine, round_to_tick
from trader.trade_plan import build_entry_exit_plan
from trader.window_router import WindowDecision


class FakeKis:
    def buy_stock_limit(self, code: str, qty: int, price: float) -> dict:
        return {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": f"O-{code}-{qty}"}}


def make_engine():
    db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(db).metadata.create_all(db)
    return PB1Engine(
        universe_repo=object(), orders_repo=OrdersRepo(db), fills_repo=FillsRepo(db),
        positions_repo=PositionsRepo(db), ledger_repo=LedgerEventsRepo(db), kis=FakeKis(),
        window=WindowDecision(name="day", phase="entry"), window_label="day", phase="entry",
        dry_run=False, intended_live=True, env="practice", run_id="test", order_allowed=True,
        trading_day=True, now_kst_value=datetime(2026, 7, 6, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )


def make_cf(code="000810", style="PULLBACK", entry_price=659000, order_price=662295, stop_price=535785.71):
    return CandidateFeature(
        code=code, market="J",
        features={
            "entry_style_selected": style, "entry_reason": "ENTRY_PULLBACK", "close": entry_price,
            "entry_price": entry_price, "order_price": order_price, "limit_price": order_price,
            "stop_price": stop_price, "initial_stop": stop_price, "trigger_policy": "PULLBACK_OVERRIDE",
        },
        setup_ok=True, reasons=[], mode=1, mode_reasons=[], planned_qty=1, client_order_key=f"test-{code}",
    )


def test_build_entry_exit_plan_pullback_raw_success():
    plan = build_entry_exit_plan(code="000810", market="J", entry_style_selected="PULLBACK", entry_reason="ENTRY_PULLBACK", entry_price=659000, features={"initial_stop": 535785.71}).to_dict()
    assert plan["entry_style_selected"] == "ENTRY_PULLBACK"
    assert plan["entry_style_raw"] == "PULLBACK"


def test_entry_plan_check_then_no_entry_exit_missing_skip(monkeypatch):
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KR")
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = make_engine()
    cf = make_cf()
    cf.entry_plan = engine._build_entry_plan(cf, entry_price=659000, order_price=662295, stop_price=535785.71, trigger_ok=False, trigger_info={}, entry_mode="", stage="PB1-AM", price_source="test")
    status = engine._place_entry(cf)
    assert status["api_submitted"] == 1
    assert status.get("skipped_reason") != "entry_exit_plan_missing_or_invalid"


def test_2026_07_06_000810_replay_reaches_api_submit(monkeypatch):
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KR")
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = make_engine()
    cf = make_cf()
    cf.entry_plan = engine._build_entry_plan(cf, entry_price=659000, order_price=662295, stop_price=535785.71, trigger_ok=False, trigger_info={}, entry_mode="", stage="PB1-AM", price_source="replay")
    status = engine._place_entry(cf)
    assert status["api_submitted"] == 1
    assert status["submit_attempted"] == 1


def test_candidate_but_zero_api_submit_is_retryable():
    status, reason, completed, retryable = normalize_kr_session_completion(status="OK_NO_TRADE", summary_reason="PB1_SESSION_DONE", marker={"completed": 1}, pb1_status="RETRYABLE_ORDER_BUILD_ERROR", pb1_exit_reason="ORDER_CANDIDATE_BUT_ZERO_API_SUBMIT")
    assert status == "RETRYABLE_ORDER_BUILD_ERROR"
    assert reason == "ORDER_CANDIDATE_BUT_ZERO_API_SUBMIT"
    assert completed == 0 and retryable == 1


def test_kr_practice_price_fallback_to_final30_close(monkeypatch):
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KR")
    monkeypatch.setenv("PB1_PRICE_FALLBACK_TO_FINAL30_CLOSE", "1")
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = make_engine()
    cf = make_cf(entry_price=0, order_price=0, stop_price=100)
    cf.features = {"entry_style_selected": "PULLBACK", "entry_reason": "ENTRY_PULLBACK", "final30_close": 10000, "stop_price": 9700, "initial_stop": 9700, "trigger_policy": "PULLBACK_OVERRIDE"}
    cf.entry_plan = engine._build_entry_plan(cf, entry_price=10000, order_price=0, stop_price=9700, trigger_ok=False, trigger_info={}, entry_mode="", stage="PB1-AM", price_source="test")
    cf.entry_plan["order_price"] = 0
    cf.entry_plan["limit_price"] = 0
    cf.entry_plan["entry_price"] = 10000
    status = engine._place_entry(cf)
    assert status["api_submitted"] == 1
    assert cf.features["order_price"] == round_to_tick(10000 * 1.005)


def test_force_min_trade_builds_candidate_when_orderable_zero(monkeypatch):
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KR")
    monkeypatch.setenv("PB1_PRACTICE_FORCE_MIN_TRADE", "1")
    engine = make_engine()
    candidate = engine._build_practice_force_min_trade_candidate(
        scan_members=[{"code": "000810", "close": 659000, "atr_pct": 13.13, "entry_style_selected": "PULLBACK"}],
        candidates=[], held_codes=set(), open_buy_codes=set(), today_buy_codes=set(),
        buyable_gate_context={}, entry_mode="",
    )
    assert candidate is not None
    assert candidate.code == "000810"
    assert candidate.planned_qty == 1
    assert candidate.features["force_reason"] == "PRACTICE_FORCE_MIN_TRADE"
    assert candidate.entry_plan["qty"] == 1


def test_force_min_trade_ignores_stale_today_buy_code(monkeypatch, caplog):
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KR")
    monkeypatch.setenv("PB1_PRACTICE_FORCE_MIN_TRADE", "1")
    engine = make_engine()
    candidate = engine._build_practice_force_min_trade_candidate(
        scan_members=[{"code": "000810", "close": 659000, "atr_pct": 13.13}],
        candidates=[], held_codes=set(), open_buy_codes=set(), today_buy_codes={"000810"},
        buyable_gate_context={"000810": {"today_buy_events": [{"status": "INTENT_ONLY"}]}},
        entry_mode="",
    )
    assert candidate is not None
    assert "[KR][PRACTICE_FORCE_MIN_TRADE][STALE_TODAY_BUY_IGNORED] code=000810 status=INTENT_ONLY" in caplog.text


def test_force_min_trade_blocks_active_today_buy_code(monkeypatch):
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KR")
    monkeypatch.setenv("PB1_PRACTICE_FORCE_MIN_TRADE", "1")
    engine = make_engine()
    candidate = engine._build_practice_force_min_trade_candidate(
        scan_members=[{"code": "000810", "close": 659000, "atr_pct": 13.13}],
        candidates=[], held_codes=set(), open_buy_codes=set(), today_buy_codes={"000810"},
        buyable_gate_context={"000810": {"today_buy_events": [{"status": "SUBMITTED"}]}},
        entry_mode="",
    )
    assert candidate is None


def test_us_path_no_touch():
    diff_names = subprocess.check_output(["git", "diff", "--name-only", "HEAD^", "HEAD"], text=True).splitlines()
    assert not any(path.startswith("trader/us/") or path.startswith("scripts/wsl/run-us-") for path in diff_names)
