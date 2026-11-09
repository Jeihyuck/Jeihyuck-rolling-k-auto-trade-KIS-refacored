"""2026-11-09 CLOSE: yesterday's missing proof never rewrites today's result."""
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from scripts.kr_close_settlement_audit import audit_sell_settlement
from trader.kr.close_settlement_state_258 import assess_close_economic_settlement


def _order(code, qty, order_id, day):
    return {
        "code": code, "side": "SELL", "qty": qty, "order_id": order_id,
        "kis_odno": order_id, "status": "FILLED",
        "position_cycle_id": "cycle-" + order_id,
        "created_at": datetime(day.year, day.month, day.day, 9, 30,
                               tzinfo=ZoneInfo("Asia/Seoul")),
    }


def test_today_nano_proven_but_old_infinite_unsettled():
    orders = [
        _order("122630", 5, "old5", date(2026, 10, 27)),
        _order("122630", 6, "old6", date(2026, 10, 27)),
        _order("121600", 15, "today", date(2026, 11, 9)),
    ]
    fills = [{
        "order_id": "today", "side": "SELL", "qty": 15, "price": 12400,
        "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
    }]
    report = audit_sell_settlement(
        orders=orders, fills=fills, positions=[], trade_date=date(2026, 11, 9),
    )
    assert report["gap_count"] == 2
    assert report["historical_gap_count"] == 2
    assert report["today_gap_count"] == 0
    assert report["today_sell_orders_checked"] == 1
    assert report["status"] == "ECONOMIC_SETTLEMENT_PENDING"

    close = assess_close_economic_settlement(
        accepted_policy_sells=1, postsubmit_reconcile_ok=True,
        audit_report=report, audit_attempted=True,
    )
    assert close["economic_settlement_state"] == "ECONOMIC_SETTLEMENT_PENDING"
    assert close["today_economic_settlement_state"] == "TODAY_ECONOMIC_SETTLED"
    assert close["historical_economic_settlement_state"] == "HISTORICAL_ECONOMIC_PENDING"


def test_undated_record_never_proves_today_settled():
    orders = [{"order_id": "unknown", "code": "121600", "side": "SELL",
               "qty": 1, "status": "FILLED"}]
    report = audit_sell_settlement(
        orders=orders, fills=[], positions=[], trade_date=date(2026, 11, 9),
    )
    outcome = assess_close_economic_settlement(
        accepted_policy_sells=1, postsubmit_reconcile_ok=True,
        audit_report=report, audit_attempted=True,
    )
    assert report["undated_gap_count"] == 1
    assert outcome["today_economic_settlement_state"] == "TODAY_SETTLEMENT_UNVERIFIED"


def test_new_sell_unpriced_blocks_today_not_only_historical():
    orders = [_order("121600", 15, "today", date(2026, 11, 9))]
    report = audit_sell_settlement(
        orders=orders, fills=[], positions=[], trade_date=date(2026, 11, 9),
    )
    outcome = assess_close_economic_settlement(
        accepted_policy_sells=1, postsubmit_reconcile_ok=True,
        audit_report=report, audit_attempted=True,
    )
    assert outcome["today_economic_settlement_state"] == "TODAY_ECONOMIC_SETTLEMENT_PENDING"
    assert outcome["economic_settlement_state"] == "ECONOMIC_SETTLEMENT_PENDING"
