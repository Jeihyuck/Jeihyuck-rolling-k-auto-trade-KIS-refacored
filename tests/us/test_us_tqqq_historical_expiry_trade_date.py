from datetime import datetime, timezone

from trader.us.infinite.ttl_reconcile import historical_zero_fill_not_live_expiry


def _order(trade_date: str):
    return {
        "trade_date": trade_date,
        "order_no": "0000000018",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": 3,
    }


def _zero_fill():
    return {
        "order_no": "18",
        "symbol": "TQQQ",
        "side": "BUY",
        "requested_qty": "3",
        "filled_qty": "0",
        "remaining_qty": "3",
        "status": "OPEN",
    }


def test_utc_next_day_but_same_us_trade_date_is_not_historical():
    # 2026-09-15 02:00 UTC == 2026-09-14 22:00 America/New_York.
    # A Sep-14 US order is still same-trade-date and must never be force expired.
    now = datetime(2026, 9, 15, 2, 0, tzinfo=timezone.utc)
    result = historical_zero_fill_not_live_expiry(
        _order("2026-09-14"),
        first_observation=_zero_fill(),
        cancel_error=RuntimeError("모의투자 원주문번호가 존재하지 않습니다"),
        retry_observation=_zero_fill(),
        now=now,
    )
    assert result is None


def test_prior_us_trade_date_can_expire_with_full_three_part_broker_proof():
    now = datetime(2026, 9, 15, 14, 0, tzinfo=timezone.utc)  # Sep-15 10:00 ET
    result = historical_zero_fill_not_live_expiry(
        _order("2026-09-14"),
        first_observation=_zero_fill(),
        cancel_error=RuntimeError("모의투자 원주문번호가 존재하지 않습니다"),
        retry_observation=_zero_fill(),
        now=now,
    )
    assert result is not None
    assert result["status"] == "EXPIRED"
    assert result["filled_qty"] == 0
    assert result["broker_proof"]["current_us_trade_date"] == "2026-09-15"


def test_http500_never_qualifies_even_for_prior_us_trade_date():
    now = datetime(2026, 9, 15, 14, 0, tzinfo=timezone.utc)
    result = historical_zero_fill_not_live_expiry(
        _order("2026-09-14"),
        first_observation=_zero_fill(),
        cancel_error=RuntimeError("HTTP 500 Internal Server Error"),
        retry_observation=_zero_fill(),
        now=now,
    )
    assert result is None
