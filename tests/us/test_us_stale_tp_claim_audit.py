"""Aged US_STANDARD TP claims remain protected, never silently reset."""
from trader.us.protective_tp_recovery import classify_stale_tp_conflict


def _order(date="2026-10-06", state="IN_FLIGHT"):
    return {"trade_date": date, "claim_state": state, "order_no": "0000034237"}


def test_mrvl_amd_oct6_debt_detected_on_oct9():
    assert classify_stale_tp_conflict(_order(), current_trade_date="2026-10-09") == (
        "STALE_TP_BROKER_TERMINAL_PROOF_REQUIRED"
    )


def test_same_day_open_is_not_stale():
    assert classify_stale_tp_conflict(_order("2026-10-09"), current_trade_date="2026-10-09") == "SAME_DAY_TP_CLAIM"


def test_closed_claim_is_not_aged_debt():
    assert classify_stale_tp_conflict(_order(state="SATISFIED"), current_trade_date="2026-10-09") == "NOT_ACTIVE_CLAIM"


def test_malformed_dates_fail_closed():
    assert classify_stale_tp_conflict(_order(""), current_trade_date="2026-10-09") == "INVALID_TRADE_DATE_FENCED"
    assert classify_stale_tp_conflict(_order("2026-10-10"), current_trade_date="2026-10-09") == "FUTURE_TRADE_DATE_FENCED"
