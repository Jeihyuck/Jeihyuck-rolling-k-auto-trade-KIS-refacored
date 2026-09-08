from trader.us.runner.session_timeout_utils import advance_timeout_execution_mode, tick_has_authoritative_execution_health


def test_timeout_helpers_match_runner_contract():
    assert not tick_has_authoritative_execution_health({"balance_fetch_failed": True})
    healthy = {
        "balance_fetch_failed": False,
        "ack_reconcile_after_route_status": "OK",
        "unresolved_ack_count": 0,
        "fill_source_status": "OK",
        "durable_fence_status": "ACTIVE",
    }
    assert tick_has_authoritative_execution_health(healthy)
    assert advance_timeout_execution_mode("NORMAL", 3, threshold=3, healthy_tick=healthy) == ("NORMAL", True)


def test_timeout_helpers_preserve_safe_degraded_transition():
    assert advance_timeout_execution_mode("NORMAL", 1, threshold=3) == ("NORMAL", True)
    assert advance_timeout_execution_mode("NORMAL", 3, threshold=3) == ("SAFE_DEGRADED", False)
