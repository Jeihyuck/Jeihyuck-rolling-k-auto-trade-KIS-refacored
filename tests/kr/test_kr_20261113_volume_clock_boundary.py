"""PR-A: KR real-time volume source-clock boundary (synthetic, no broker calls)."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from trader.kr.volume_proof import build_kr_volume_proof

KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 13, 12, 30, tzinfo=KST)


def _proof(quote):
    days = pd.bdate_range(end="2026-11-12", periods=50)
    bars = pd.DataFrame([{"date": day, "volume": 1000.0} for day in days])
    return build_kr_volume_proof(
        bars=bars, snapshot=quote, trade_date="2026-11-13",
        observed_at=NOW, code="219130", market="KOSDAQ",
        entry_style="ENTRY_MOMENTUM",
    )


@pytest.mark.parametrize("clock", [None, "", "   "])
def test_missing_exchange_clock_never_fabricates_midnight(clock):
    quote = {"acml_vol": 2000, "source": "KIS_WEBSOCKET"}
    if clock is not None:
        quote["stck_cntg_hour"] = clock
    proof = _proof(quote)
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["failure_reason"] == "kis_exchange_quote_time_missing"
    assert proof["quote_as_of"] is None
    assert proof["exchange_quote_age_seconds"] is None
    assert proof["quote_source"] == "KIS_WEBSOCKET"


@pytest.mark.parametrize("clock", ["000000", 0, "bogus", "246000", "9999999"])
def test_explicit_invalid_exchange_clock_does_not_authorize_buy(clock):
    proof = _proof({"acml_vol": 2000, "stck_cntg_hour": clock})
    assert proof["confirmation_state"] != "VERIFIED"
    assert proof["failure_reason"] == "kis_exchange_quote_time_invalid"
    assert proof["quote_as_of"] is None


def test_fresh_native_exchange_clock_remains_verified():
    proof = _proof({"acml_vol": 2000, "stck_cntg_hour": "122900",
                    "source": "KIS_REST", "stck_bsop_date": "20261113"})
    assert proof["confirmation_state"] == "VERIFIED", proof
    assert proof["quote_source"] == "KIS_REST"
    assert proof["exchange_quote_age_seconds"] == 60


def test_real_stale_clock_still_fail_closed():
    proof = _proof({"acml_vol": 2000, "stck_cntg_hour": "093000"})
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["failure_reason"] == "kis_exchange_quote_stale_or_future"
    assert proof["quote_as_of"] == "2026-11-13T09:30:00+09:00"


def test_explicitly_invalid_primary_timestamp_does_not_fallback_to_cached_raw():
    proof = _proof({"acml_vol": 2000, "stck_cntg_hour": None,
                    "raw": {"stck_cntg_hour": "122900"}})
    assert proof["confirmation_state"] != "VERIFIED"
    assert proof["failure_reason"] == "kis_exchange_quote_time_missing"


def test_quote_without_exchange_clock_cannot_be_made_fresh_from_receipt_time():
    proof = _proof({"acml_vol": 2000, "received_at": NOW.timestamp(),
                    "age_sec": 0.1, "source": "KIS_WEBSOCKET"})
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["quote_as_of"] is None
