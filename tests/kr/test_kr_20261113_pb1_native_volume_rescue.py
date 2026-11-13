"""Exercise PB1Engine's *actual* pre-BUY KIS volume retry (no orders)."""
import time
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import trader.pb1_engine as pb1

NOW = datetime(2026, 11, 13, 12, 30, 20, tzinfo=ZoneInfo("Asia/Seoul"))


def _bars():
    days = pd.bdate_range(end="2026-11-12", periods=50)
    return pd.DataFrame([{"date": d, "volume": 1000} for d in days])


def _native():
    return {
        "source": "KIS_REST_TIME_ITEMCONCLUSION",
        "source_identity": "AUTHENTICATED_REST_REQUEST_BOUND",
        "requested_symbol": "219130",
        "stck_cntg_hour": "123010",
        "acml_vol": 3000,
        "last": 10000,
        "received_at": NOW.timestamp(),
        "raw": {"stck_cntg_hour": "123010", "acml_vol": "3000",
                "stck_pbpr": "10000"},
        "quote_source_contract": "SAME_OUTPUT2_EXECUTION_ROW",
    }


def _fixture(monkeypatch, reader):
    monkeypatch.setattr(pb1, "now_kst", lambda: NOW)
    engine = pb1.PB1Engine.__new__(pb1.PB1Engine)
    engine.env = "practice"
    engine.STRATEGY_NAME = "pb1_pullback_close"
    engine._trade_date = "2026-11-13"
    engine._tick_native_volume_quote_calls = 0
    engine._tick_start_time = time.monotonic()
    engine.kis = SimpleNamespace(get_kr_entry_native_trade_quote=reader)
    cf = SimpleNamespace(code="219130", market="KOSDAQ",
                         features={"above_vwap": True})
    return engine, cf


def _run(engine, cf, *, initial_reason="kis_exchange_quote_time_missing",
         style="ENTRY_MOMENTUM", trigger_ok=True):
    return engine._retry_kr_native_volume_proof(
        cf=cf, bars=_bars(), daily_meta={"source": "db"},
        entry_style=style, trigger_ok=trigger_ok,
        trigger_info={"vol_ok": True},
        initial_proof={"confirmation_state": "UNKNOWN",
                       "failure_category": "DATA_UNVERIFIED",
                       "failure_reason": initial_reason,
                       "quote_source": "KIS_WEBSOCKET"},
    )


def test_native_proof_reaches_verified_only_via_valid_same_row_source(monkeypatch):
    calls = []
    def read(code):
        calls.append(code)
        return _native()
    engine, cf = _fixture(monkeypatch, read)
    result = _run(engine, cf)
    assert calls == ["219130"]
    assert engine._tick_native_volume_quote_calls == 1
    assert result["confirmation_state"] == "VERIFIED", result
    assert result["independent_native_trade_row"] is True
    assert result["initial_quote_evidence_failure"] == "kis_exchange_quote_time_missing"
    assert result["quote_source"] == "KIS_REST_TIME_ITEMCONCLUSION"


@pytest.mark.parametrize("reader", [
    lambda code: {},
    lambda code: {"source": "UNKNOWN", "stck_cntg_hour": "123010",
                  "acml_vol": 3000, "received_at": NOW.timestamp()},
])
def test_unavailable_or_unproven_source_never_authorizes(monkeypatch, reader):
    engine, cf = _fixture(monkeypatch, reader)
    proof = _run(engine, cf)
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["failure_reason"] == "kis_exchange_quote_time_missing"


def test_exception_is_fail_closed_no_retry_or_order(monkeypatch):
    calls = []
    def bad(code):
        calls.append(code)
        raise RuntimeError("broker price timeout")
    engine, cf = _fixture(monkeypatch, bad)
    proof = _run(engine, cf)
    assert calls == ["219130"]
    assert proof["confirmation_state"] == "UNKNOWN"


def test_policy_trigger_fail_is_not_rescued_with_other_source(monkeypatch):
    calls = []
    engine, cf = _fixture(monkeypatch, lambda code: calls.append(code) or _native())
    proof = _run(engine, cf, initial_reason="breakout_price_trigger_missing",
                 style="ENTRY_BREAKOUT", trigger_ok=False)
    assert proof["confirmation_state"] == "UNKNOWN"
    assert calls == []


def test_per_tick_cap_and_deadline_preserve_seller_time(monkeypatch):
    calls = []
    engine, cf = _fixture(monkeypatch, lambda code: calls.append(code) or _native())
    engine._tick_native_volume_quote_calls = 4
    assert _run(engine, cf)["confirmation_state"] == "UNKNOWN"
    assert calls == []
    engine._tick_native_volume_quote_calls = 0
    engine._tick_start_time = time.monotonic() - 140.0
    assert _run(engine, cf)["confirmation_state"] == "UNKNOWN"
    assert calls == []


def test_non_pb1_owner_does_not_query_kis(monkeypatch):
    calls = []
    engine, cf = _fixture(monkeypatch, lambda code: calls.append(code) or _native())
    engine.STRATEGY_NAME = "KR_INFINITE_V1"
    assert _run(engine, cf)["confirmation_state"] == "UNKNOWN"
    assert calls == []


def test_real_account_does_not_enable_practice_fallback(monkeypatch):
    calls = []
    engine, cf = _fixture(monkeypatch, lambda code: calls.append(code) or _native())
    engine.env = "real"
    assert _run(engine, cf)["confirmation_state"] == "UNKNOWN"
    assert calls == []
