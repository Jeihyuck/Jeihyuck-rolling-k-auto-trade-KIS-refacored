"""PR #355: native KIS trade-time and cumulative-volume arrive from ONE row.

No live KIS, order, or production DB access. These fixtures model the exact
FHPST01060000 output2 field names, not a guarantee of real broker availability.
"""
from datetime import datetime
import threading
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd

import trader.kis_wrapper as kw
from trader.kr.volume_proof import build_kr_volume_proof

KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 13, 12, 30, 20, tzinfo=KST)


class Resp:
    status_code = 200
    def __init__(self, data):
        self.data = data
    def json(self):
        return self.data


def _broker(monkeypatch, rows, *, allowed=True):
    monkeypatch.setattr(kw, "now_kst", lambda: NOW)
    # Quote receipt and decision clock must refer to the SAME synthetic KST instant.
    # The implementation samples time.time(); do not let current CI wall time
    # make a valid historical fixture appear stale or future-dated.
    monkeypatch.setattr(kw.time, "time", lambda: NOW.timestamp())
    monkeypatch.setattr(kw, "get_kis_gate", lambda: SimpleNamespace(
        wait_until_allowed=lambda *_a, **_k: allowed
    ))
    calls = []
    broker = kw.KisAPI.__new__(kw.KisAPI)
    broker.env = "practice"
    broker._concurrency_sem = threading.Semaphore(1)
    broker._limiter = SimpleNamespace(wait=lambda *_args: None)
    broker._headers = lambda *_args: {"tr_id": "FHPST01060000"}
    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return Resp({"rt_cd": "0", "output1": {
            # A summary clock/volume from output1 is NOT used in proof.
            "acml_vol": "987654321", "stck_prpr": "99999",
        }, "output2": rows})
    broker._safe_request = request
    return broker, calls


def _row(clock="123010", cumulative="2400", price="10000", **extra):
    return {"stck_cntg_hour": clock, "acml_vol": cumulative,
            "stck_pbpr": price, **extra}


def _proof(quote):
    days = pd.bdate_range(end="2026-11-12", periods=50)
    bars = pd.DataFrame([{"date": d, "volume": 1000} for d in days])
    return build_kr_volume_proof(
        bars=bars, snapshot=quote, trade_date="2026-11-13",
        observed_at=NOW, code="219130", market="KOSDAQ",
        entry_style="ENTRY_MOMENTUM",
    )


def test_one_native_output2_row_can_reach_verified_volume_proof(monkeypatch):
    broker, calls = _broker(monkeypatch, [_row()])
    quote = broker.get_kr_entry_native_trade_quote("219130")
    assert len(calls) == 1
    assert calls[0][0] == "GET"
    assert calls[0][1].endswith("/inquire-time-itemconclusion")
    assert calls[0][2]["headers"]["tr_id"] == "FHPST01060000"
    assert calls[0][2]["params"]["FID_INPUT_ISCD"] == "219130"
    assert quote["source"] == "KIS_REST_TIME_ITEMCONCLUSION"
    assert quote["quote_source_contract"] == "SAME_OUTPUT2_EXECUTION_ROW"
    assert quote["acml_vol"] == 2400
    assert quote["stck_cntg_hour"] == "123010"
    assert quote["last"] == 10000
    proof = _proof(quote)
    assert proof["confirmation_state"] == "VERIFIED", proof
    assert proof["quote_identity_attestation"] == "AUTHENTICATED_REST_REQUEST"
    assert proof["quote_trade_date_attestation"] == "SESSION_INFERRED_NOT_NATIVE"
    assert proof["exchange_quote_age_seconds"] == 10


def test_output1_summary_not_spliced_into_missing_trade_row(monkeypatch):
    broker, calls = _broker(monkeypatch, [_row(cumulative="")])
    assert broker.get_kr_entry_native_trade_quote("219130") == {}
    assert len(calls) == 1


def test_explicit_different_broker_symbol_does_not_attest_source(monkeypatch):
    broker, _ = _broker(monkeypatch, [_row(stck_shrn_iscd="005930")])
    assert broker.get_kr_entry_native_trade_quote("219130") == {}


def test_old_native_trade_row_remains_stale_even_when_http_response_new(monkeypatch):
    broker, _ = _broker(monkeypatch, [_row(clock="102000")])
    quote = broker.get_kr_entry_native_trade_quote("219130")
    proof = _proof(quote)
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["failure_reason"] == "kis_exchange_quote_stale_or_future"


def test_missing_trade_clock_not_filled_from_request_time(monkeypatch):
    broker, _ = _broker(monkeypatch, [{"acml_vol": "1000", "stck_pbpr": "10000"}])
    assert broker.get_kr_entry_native_trade_quote("219130") == {}


def test_rate_gate_denial_produces_no_network_call(monkeypatch):
    broker, calls = _broker(monkeypatch, [_row()], allowed=False)
    assert broker.get_kr_entry_native_trade_quote("219130") == {}
    assert calls == []


def test_other_environment_never_requests_native_fallback(monkeypatch):
    broker, calls = _broker(monkeypatch, [_row()])
    broker.env = "real"
    assert broker.get_kr_entry_native_trade_quote("219130") == {}
    assert calls == []


def test_native_quote_invalid_day_not_used_as_today(monkeypatch):
    broker, _ = _broker(monkeypatch, [_row(stck_bsop_date="20261112")])
    assert broker.get_kr_entry_native_trade_quote("219130") == {}


def test_observer_end_to_end_gate_remains_strict_on_no_trade_identity(monkeypatch):
    broker, _ = _broker(monkeypatch, [_row()])
    quote = broker.get_kr_entry_native_trade_quote("219130")
    bad = _proof({**quote, "requested_symbol": "005930"})
    assert bad["confirmation_state"] != "VERIFIED"
    assert bad["failure_reason"] == "kis_quote_identity_unverified"
