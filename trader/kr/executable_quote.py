"""Separate authenticated KIS price and top-of-book observations for PB1."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from math import isfinite

KST = timezone(timedelta(hours=9))
MAX_AGE = 5.0
SOURCES = {
    "KIS_WEBSOCKET": "KIS_WS_PARSED_SYMBOL",
    "KIS_REST_INQUIRE_PRICE": "AUTHENTICATED_REST_REQUEST_BOUND",
}


def _positive(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _symbol(value):
    c = str(value or "").removeprefix("A")
    return c if len(c) == 6 and c.isascii() and c.isdigit() else ""


def verified_price(quote, code, *, now):
    if not isinstance(quote, dict) or not _symbol(code):
        return False
    source = SOURCES.get(quote.get("source"))
    ids = [_symbol(quote[k]) for k in ("symbol", "requested_symbol") if quote.get(k)]
    receipt = _positive(quote.get("received_at"))
    day = str(quote.get("stck_bsop_date") or "").replace("-", "")
    return bool(source and quote.get("source_identity") == source
                and quote.get("market") == "KR" and ids
                and all(c == code for c in ids) and not quote.get("stale")
                and receipt is not None and 0 <= now-receipt <= MAX_AGE
                and (not day or day == datetime.fromtimestamp(now, KST).strftime("%Y%m%d"))
                and _positive(quote.get("last") or quote.get("prpr") or quote.get("stck_prpr")))


def complete_book(quote):
    if not isinstance(quote, dict):
        return False
    bid, ask = _positive(quote.get("bid")), _positive(quote.get("ask"))
    return bid is not None and ask is not None and bid <= ask


def compose_orderbook(price, response, *, code, started, received):
    """No fabricated last, volume or exchange timestamp; no refresh of price age.

    The authenticated request binds the book to code. Explicit contradictions
    and delayed responses are rejected; receipt and native book clock are
    preserved separately. A missing native clock is not a fresh book proof.
    """
    if not verified_price(price, code, now=received) or not 0 <= received-started <= 2.5:
        return None
    if getattr(response, "status_code", None) != 200:
        return None
    try:
        body = response.json()
    except (ValueError, TypeError, AttributeError):
        return None
    if not isinstance(body, dict) or str(body.get("rt_cd")) != "0":
        return None
    row = body.get("output1")
    if not isinstance(row, dict):
        return None
    for field in ("stck_shrn_iscd", "pdno", "symbol"):
        if row.get(field) and _symbol(row[field]) != code:
            return None
    local = datetime.fromtimestamp(received, KST)
    day = str(row.get("stck_bsop_date") or "").replace("-", "")
    if day and day != local.strftime("%Y%m%d"):
        return None
    clock = str(row.get("aspr_acpt_hour") or "").strip()
    if len(clock) != 6 or not clock.isdigit():
        return None
    try:
        native = datetime.strptime(local.strftime("%Y%m%d")+clock, "%Y%m%d%H%M%S").replace(tzinfo=KST)
    except ValueError:
        return None
    if not -1 <= received-native.timestamp() <= MAX_AGE+1:
        return None
    bid, ask = _positive(row.get("bidp1")), _positive(row.get("askp1"))
    if bid is None or ask is None or bid > ask:
        return None
    # Preserve original price proof; overwrite BOTH sides from one book.
    return {**price, "bid": bid, "ask": ask,
            "book_source": "KIS_REST_ORDERBOOK_OUTPUT1",
            "book_source_identity": "AUTHENTICATED_REST_REQUEST_BOUND",
            "book_requested_symbol": code,
            "book_received_at": received, "book_native_clock": clock,
            "book_request_started_at": started,
            "book_native_timestamp": native.timestamp(),
            "book_native_day_reported": bool(day)}


def valid_book_supplement(quote, code, *, now):
    """Recheck BOTH timestamps at the eventual order decision, never on cache read."""
    if "book_source" not in quote:
        return True  # existing single-frame quote path
    if (quote.get("book_source") != "KIS_REST_ORDERBOOK_OUTPUT1"
            or quote.get("book_source_identity") != "AUTHENTICATED_REST_REQUEST_BOUND"
            or quote.get("book_requested_symbol") != code or not complete_book(quote)
            or not verified_price(quote, code, now=now)):
        return False
    receipt = _positive(quote.get("book_received_at"))
    native = _positive(quote.get("book_native_timestamp"))
    start = _positive(quote.get("book_request_started_at"))
    return bool(receipt is not None and native is not None and start is not None
                and 0 <= now-receipt <= MAX_AGE and -1 <= now-native <= MAX_AGE+1
                and 0 <= receipt-start <= 2.5
                and datetime.fromtimestamp(native, KST).date() == datetime.fromtimestamp(now, KST).date())
