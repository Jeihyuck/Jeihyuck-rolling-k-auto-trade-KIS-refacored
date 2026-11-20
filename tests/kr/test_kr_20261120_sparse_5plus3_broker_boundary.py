"""FINAL v3: sparse genuine-form KR minutes through production BUY broker boundary.

Fixtures synthesize the prices but run the real completed-minute evaluator,
durable proof storage, signed entry-plan build, pre-submit gate, order writer,
and fake broker adapter. Never place a network KIS order.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from tests.kr.test_kr_entry_authoritative_gate_state import (
    _make_engine, _build_candidate,
)
from tests.kr.test_kr_20261118_sparse_native_intraday_entry import (
    sparse_opening_and_native_rebound,
)
from trader.account_state import get_account_key
from trader.kr import opening_bar_contract as opening


KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 18, 9, 38, 20, tzinfo=KST)
ACCOUNT = get_account_key(env="practice")


def _native_rows(*, drop_minute=None):
    rows = []
    for original in sparse_opening_and_native_rebound():
        if original["time"] == drop_minute:
            continue
        rows.append({
            **original,
            "open": original["open"] * 100,
            "high": original["high"] * 100,
            "low": original["low"] * 100,
            "close": original["close"] * 100,
        })
    return rows


def _candidate(engine, code: str):
    cf = _build_candidate(code)
    cf.features.update({
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_reason": "ENTRY_PULLBACK",
        "close": 10350.0, "entry_price": 10350.0,
        "order_price": 10350.0, "limit_price": 10350.0,
        "stop_price": 9950.0, "initial_stop": 9950.0,
        "price_source": "verified_native_limit",
    })
    cf.entry_plan = engine._build_entry_plan(
        cf, entry_price=10350.0, order_price=10350.0,
        stop_price=9950.0, trigger_ok=False,
        trigger_info={"reason": "verified_post_open_rebound"},
        entry_mode="OR", stage="PB1-AM",
        price_source="verified_native_limit",
    )
    cf.features["entry_plan"] = cf.entry_plan
    return cf


def _prepare(monkeypatch, tmp_path, *, code: str, drop_minute=None):
    import trader.pb1_engine as pb1
    monkeypatch.setattr(opening, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    monkeypatch.setattr(pb1, "now_kst", lambda: NOW)
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KR")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr(pb1, "validate_tradeable", lambda kis, sym: (True, "ok"))
    # Do not stub evaluate_proof_for_candidate or any signal/risk gate.
    opening.save_opening_proof(
        env="practice", account_scope=ACCOUNT, code=code,
        now=NOW, raw_bars=_native_rows(drop_minute=drop_minute),
        vwap=None, vwap_source="UNKNOWN",
    )
    signal = opening.evaluate_proof_for_candidate(
        env="practice", account_scope=ACCOUNT, code=code,
        now=NOW, style="ENTRY_PULLBACK", setup_ok=True,
        trigger_ok=False, pivot=None, order_price=10350.0,
    )
    engine = _make_engine()
    engine._now_kst = NOW
    return engine, signal


def test_16_opening_and_consecutive_5plus3_reaches_actual_buy_adapter_once(monkeypatch, tmp_path):
    engine, signal = _prepare(monkeypatch, tmp_path, code="093320")
    assert signal["opening_count"] == 16
    assert signal["reference_source"] == "KIS_NATIVE_POST_OPENING_5_PLUS_3"
    assert signal["state"] == "READY_SHADOW", signal

    cf = _candidate(engine, "093320")
    result = engine._place_entry(cf)
    assert result["api_submitted"] == 1, result
    assert engine.kis.buy_calls == 1  # one fake adapter, no network broker
    assert result["terminal_event"] == "API_RESULT"
    stored = engine.orders_repo.get_order_by_client_order_key(
        "practice", cf.client_order_key,
    )
    assert stored and stored["request_json"]["entry_contract_sha256"]
    assert stored["request_json"]["entry_exit_plan"]


def test_5plus3_missing_completed_minute_fails_before_broker(monkeypatch, tmp_path):
    engine, signal = _prepare(
        monkeypatch, tmp_path, code="251970", drop_minute="093400",
    )
    assert signal["state"] == "UNKNOWN", signal
    assert signal["reason"] == "POST_OPENING_NATIVE_WINDOW_GAP"
    result = engine._place_entry(_candidate(engine, "251970"))
    assert result["api_submitted"] == 0, result
    assert engine.kis.buy_calls == 0
