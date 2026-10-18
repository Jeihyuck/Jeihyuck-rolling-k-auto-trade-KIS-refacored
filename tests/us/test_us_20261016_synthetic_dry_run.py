"""Read-only evidence audit classification; no DB calls."""
from scripts.us_order_evidence_dry_run import classify, READ_ONLY_SQL


def test_synthetic_only_fill_is_review_not_authoritative_close():
    result = classify({
        "trade_date": "2026-10-14", "symbol": "ABBV", "side": "SELL",
        "qty_requested": 2, "qty_filled": 2, "order_status": "FILLED",
        "attempt_state": "ACKED", "action_state": "IN_FLIGHT",
        "authoritative": False, "synthetic_count": 1, "broker_count": 0,
    })
    assert result["finding"] == "SYNTHETIC_ONLY_FILLED_ORDER"
    assert result["verification_required"]
    assert result["proposed_change"] == "NONE_DRY_RUN"
    assert not result["release_fence"]


def test_confirmed_broker_fill_is_not_counted_as_synthetic():
    result = classify({
        "trade_date": "2026-10-14", "symbol": "NET", "side": "SELL",
        "qty_requested": 2, "qty_filled": 2, "order_status": "FILLED",
        "attempt_state": "FILLED", "action_state": "SATISFIED",
        "authoritative": True, "synthetic_count": 0, "broker_count": 1,
    })
    assert result["finding"] == "BROKER_EVIDENCE_PRESENT"
    assert not result["verification_required"]


def test_sql_is_bounded_account_date_and_read_only():
    assert ":start_date" in READ_ONLY_SQL
    assert ":end_date" in READ_ONLY_SQL
    assert "LIMIT 2000" in READ_ONLY_SQL
    assert "UPDATE " not in READ_ONLY_SQL
    assert "DELETE " not in READ_ONLY_SQL
    assert "INSERT " not in READ_ONLY_SQL
