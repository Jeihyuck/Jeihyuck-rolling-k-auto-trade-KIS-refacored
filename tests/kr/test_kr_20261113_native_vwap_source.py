"""PR356: no unbound/expired VWAP can authorize Momentum 1m timing.

Synthetic broker-response shapes only; source availability in live WSL
still requires KIS read-only native capture, not this fixture.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from scripts.kr_opening_observer import verified_native_kr_vwap

KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 13, 10, 0, 30, tzinfo=KST)


def _quote(**extras):
    q = {
        "source": "KIS_REST_INQUIRE_PRICE",
        "source_identity": "AUTHENTICATED_REST_REQUEST_BOUND",
        "requested_symbol": "219130",
        "received_at": NOW.timestamp(),
        "raw": {
            "stck_bsop_date": "20261113",
            "stck_cntg_hour": "100020",
            "wghn_avrg_stck_prc": "10050",
        },
    }
    q.update(extras)
    return q


def test_source_bound_native_vwap_is_verified_without_creating_order():
    price, reason = verified_native_kr_vwap(_quote(), code="219130", observed_at=NOW)
    assert (price, reason) == (10050.0, "VERIFIED_NATIVE_KIS_VWAP")


def test_explicit_invalid_top_level_exchange_clock_cannot_steal_raw():
    price, reason = verified_native_kr_vwap(
        _quote(stck_cntg_hour=None), code="219130", observed_at=NOW,
    )
    assert price is None
    assert reason == "NATIVE_QUOTE_CLOCK_MISSING"


def test_explicit_bad_top_level_vwap_cannot_use_valid_raw():
    price, reason = verified_native_kr_vwap(
        _quote(wghn_avrg_stck_prc=0), code="219130", observed_at=NOW,
    )
    assert price is None
    assert reason == "NATIVE_VWAP_MISSING_OR_INVALID"


def test_missing_native_day_fails_closed_not_inferred():
    q = _quote()
    del q["raw"]["stck_bsop_date"]
    assert verified_native_kr_vwap(q, code="219130", observed_at=NOW)[1] == (
        "NATIVE_QUOTE_DATE_UNVERIFIED"
    )


def test_wrong_rest_identity_fails_before_using_clock_or_price():
    assert verified_native_kr_vwap(
        _quote(requested_symbol="005930"), code="219130", observed_at=NOW,
    )[1] == "REST_REQUEST_IDENTITY_UNVERIFIED"


def test_unbound_source_label_does_not_grant_native_vwap():
    assert verified_native_kr_vwap(
        _quote(source_identity=""), code="219130", observed_at=NOW,
    )[1] == "REST_REQUEST_IDENTITY_UNVERIFIED"


def test_ws_price_only_cannot_create_native_vwap():
    quote = {"source": "KIS_WEBSOCKET", "symbol": "219130",
             "last": 10000, "received_at": NOW.timestamp()}
    assert verified_native_kr_vwap(
        quote, code="219130", observed_at=NOW,
    )[1] == "NATIVE_QUOTE_DATE_UNVERIFIED"


def test_old_quote_receipt_with_fresh_native_clock_is_still_unverified():
    quote = _quote(received_at=(NOW - timedelta(minutes=4)).timestamp())
    assert verified_native_kr_vwap(
        quote, code="219130", observed_at=NOW,
    )[1] == "QUOTE_RECEIPT_STALE_OR_FUTURE"


def test_midnight_clock_invalid_not_fabricated_from_none():
    for clock in (None, "", "000000"):
        quote = _quote()
        quote["raw"]["stck_cntg_hour"] = clock
        price, reason = verified_native_kr_vwap(
            quote, code="219130", observed_at=NOW,
        )
        assert price is None
        assert reason in {"NATIVE_QUOTE_CLOCK_MISSING", "NATIVE_QUOTE_CLOCK_INVALID"}
