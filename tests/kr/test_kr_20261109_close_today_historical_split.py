"""2026-11-09 CLOSE: current priced SELL evidence vs old unsettled debt."""
from datetime import date, datetime, timezone

from scripts.kr_close_settlement_audit import audit_sell_settlement_by_day


def _order(code, key, day):
    return {
        "code": code, "order_id": key, "side": "SELL",
        "status": "FILLED", "qty": 5, "kis_odno": key,
        "created_at": datetime(day.year, day.month, day.day, 1, 0, tzinfo=timezone.utc),
        "position_cycle_id": "cycle-" + key,
    }


def _fill(order):
    return {
        "order_id": order["order_id"], "side": "SELL",
        "qty": order["qty"], "price": 50000,
        "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
    }


def test_old_kodex_unpriced_sell_does_not_false_fail_nov9_today_lane():
    old = _order("122630", "0000008799", date(2026, 10, 27))
    today = _order("121600", "today-nano", date(2026, 11, 9))
    result = audit_sell_settlement_by_day(
        orders=[old, today], fills=[_fill(today)], positions=[],
        trade_date=date(2026, 11, 9),
    )
    assert result["status"] == "ECONOMIC_SETTLEMENT_PENDING"
    assert result["gap_count"] == 1
    assert result["today_settlement"]["status"] == "PRICED_SELL_EVIDENCE_MATCHED"
    assert result["today_priced_sell_gap_count"] == 0
    assert result["historical_priced_sell_gap_count"] == 1
    assert result["historical_settlement_debt"]["gap_count"] == 1


def test_today_unpriced_sell_is_never_mislabelled_historical():
    today = _order("121600", "today-unpriced", date(2026, 11, 9))
    result = audit_sell_settlement_by_day(
        orders=[today], fills=[], positions=[], trade_date=date(2026, 11, 9),
    )
    assert result["today_priced_sell_gap_count"] == 1
    assert result["historical_priced_sell_gap_count"] == 0


def test_missing_source_date_stays_unknown_not_today_settled():
    unknown = _order("122630", "unknown", date(2026, 10, 27))
    unknown.pop("created_at")
    result = audit_sell_settlement_by_day(
        orders=[unknown], fills=[], positions=[], trade_date=date(2026, 11, 9),
    )
    assert result["unknown_source_day_gap_count"] == 1
    assert result["today_priced_sell_gap_count"] == 0
    assert result["gap_count"] == 1


def test_unknown_date_must_not_hide_filled_current_day():
    unknown = _order("122630", "unknown", date(2026, 10, 27))
    unknown["created_at"] = "unparseable"
    today = _order("121600", "today", date(2026, 11, 9))
    result = audit_sell_settlement_by_day(
        orders=[today, unknown], fills=[_fill(today)], positions=[],
        trade_date=date(2026, 11, 9),
    )
    assert result["gap_count"] == 1
    assert result["today_settlement"]["status"] == "PRICED_SELL_EVIDENCE_MATCHED"
    assert result["unknown_source_day_gap_count"] == 1
