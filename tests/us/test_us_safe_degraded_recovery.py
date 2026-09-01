from trader.us.runner.trade_session_runner import advance_timeout_execution_mode


def test_safe_degraded_recovers_only_with_full_authoritative_health():
    mode, allowed = advance_timeout_execution_mode("NORMAL", 3, threshold=3)
    assert (mode, allowed) == ("SAFE_DEGRADED", False)
    quote_only = {"status": "OK", "balance_fetch_failed": True}
    assert advance_timeout_execution_mode(mode, 3, threshold=3, healthy_tick=quote_only) == ("SAFE_DEGRADED", False)
    healthy = {"balance_fetch_failed": False, "ack_reconcile_after_route_status": "OK",
               "unresolved_ack_count": 0, "fill_source_status": "OK", "durable_fence_status": "ACTIVE"}
    assert advance_timeout_execution_mode(mode, 3, threshold=3, healthy_tick=healthy) == ("NORMAL", True)
