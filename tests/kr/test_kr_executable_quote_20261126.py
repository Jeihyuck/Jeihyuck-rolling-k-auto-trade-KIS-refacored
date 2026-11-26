"""Selected PB1 quote recovery: reconstructed incident evidence, no KIS calls."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from trader.kr.executable_quote import compose_orderbook, valid_book_supplement
from trader.kr.entry_opportunity_v2 import validate_live_quote

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 11, 26, 10, 42, 22, tzinfo=KST)
CODE = "251970"


def price(now=NOW):
    return dict(source="KIS_REST_INQUIRE_PRICE",
                source_identity="AUTHENTICATED_REST_REQUEST_BOUND",
                requested_symbol=CODE, market="KR", last=80100,
                bid=None, ask=None, received_at=now.timestamp()-1,
                stck_bsop_date=now.strftime("%Y%m%d"), acml_vol=123456)


def response(**overrides):
    row = dict(aspr_acpt_hour="104221", bidp1="80100", askp1="80200")
    row.update(overrides)
    return SimpleNamespace(status_code=200, json=lambda: {
        "rt_cd": "0", "output1": row, "output2": {"antc_cnpr": "99999"},
    })


def test_missing_book_and_hypothetical_verified_recovery():
    # Only last/reference/cap and missing bid/ask are observed incident facts.
    # The positive book is synthetic, never a claim about actual 10:42 quotes.
    q = price()
    timing = dict(state="READY_SHADOW", entry_policy="US_ALIGNED_V2",
                  latest_bar_ts=(NOW-timedelta(seconds=142)).isoformat(),
                  reference_price=80100, max_entry_price=80300)
    args = dict(code=CODE, timing=timing, now=NOW, order_price=80200)
    assert validate_live_quote(**args, quote=q)["state"] == "UNKNOWN"
    result = compose_orderbook(q, response(), code=CODE,
                               started=NOW.timestamp()-0.5, received=NOW.timestamp())
    assert result and result["ask"] == 80200 and result["bid"] == 80100
    assert result["received_at"] == q["received_at"]
    assert result["book_received_at"] == NOW.timestamp()
    assert result["acml_vol"] == q["acml_vol"]
    assert valid_book_supplement(result, CODE, now=NOW.timestamp())
    assert validate_live_quote(**args, quote=result)["state"] == "READY"
    assert validate_live_quote(**{**args, "order_price": 80100}, quote=result)["state"] == "WAIT"
    # The book and price are independent; expiry must not use only the old
    # 15-second single-frame quote threshold.
    assert not valid_book_supplement(result, CODE, now=NOW.timestamp()+6)
    assert validate_live_quote(**{**args, "now": NOW+timedelta(seconds=6)}, quote=result)["state"] == "UNKNOWN"


@pytest.mark.parametrize("overrides", [
    {"askp1": None}, {"bidp1": "0"}, {"bidp1": "80300"},
    {"aspr_acpt_hour": None}, {"aspr_acpt_hour": "104000"},
    {"aspr_acpt_hour": "104230"}, {"pdno": "012750"},
    {"stck_bsop_date": "20261125"}, {"askp1": "NaN"},
])
def test_bad_orderbook_does_not_create_executable_quote(overrides):
    assert compose_orderbook(price(), response(**overrides), code=CODE,
                             started=NOW.timestamp()-0.5, received=NOW.timestamp()) is None


def test_stale_price_is_not_rejuvenated_by_fresh_book():
    stale = {**price(), "received_at": NOW.timestamp()-17.59}
    assert compose_orderbook(stale, response(), code=CODE,
                             started=NOW.timestamp()-0.5, received=NOW.timestamp()) is None


def test_wrapper_reads_output1_and_never_uses_expected_auction_price(monkeypatch):
    from trader import kis_wrapper as kw
    api = object.__new__(kw.KisAPI)
    api.env = "practice"
    api._headers = lambda *args: {}
    api._safe_request = lambda *args, **kwargs: response()
    assert api._get_hoga_quote(CODE)["askp1"] == "80200"


def test_wrapper_selected_refresh_is_bounded_and_keeps_price_identity(monkeypatch):
    from trader import kis_wrapper as kw
    api = object.__new__(kw.KisAPI)
    api.env = "practice"
    calls = []
    api._headers = lambda *args: {}
    api.get_price_snapshot = lambda *args, **kwargs: {
        **price(), "received_at": NOW.timestamp()-17.59}
    api.get_price_quote = lambda *args, **kwargs: calls.append(("PRICE", kwargs)) or price()
    api._safe_request = lambda *args, **kwargs: calls.append(("BOOK", kwargs)) or response()
    monkeypatch.setattr(kw.time, "time", lambda: NOW.timestamp())
    result = api.get_kr_entry_executable_quote(CODE)
    assert result["ask"] == 80200
    assert result["received_at"] == price()["received_at"]
    assert calls[0][1]["entry_refresh"] is True
    assert calls[1][1]["kr_read_attempt_cap"] == 1
    assert api.get_kr_entry_executable_quote(CODE)["ask"] == 80200
    assert len(calls) == 2
