from trader.us.runner.close_audit import audit_us_close_report


def test_oct8_broker_and_local_conflict_is_visible_without_inventing_fills():
    report = {
        "broker_recovery_health": {
            "available": True,
            "unresolved_execution_actions": 2,
            "duplicate_semantic_submit_detection_count": 141,
        },
        "report_consistency": "FAILED",
        "close_integrity_status": "RECONCILE_REQUIRED",
        "kis_actual_fill_order_count": 13,
        "daily_fills_confirmed_total": 0,
        "unique_broker_order_count": 15,
    }
    before = repr(report)
    result = audit_us_close_report(report)
    codes = {item["code"] for item in result["findings"]}
    assert {"UNRESOLVED_EXECUTION_ACTIONS",
            "BLOCKED_EXECUTION_CLAIM_ATTEMPTS",
            "CLOSE_REPORT_CONSISTENCY_FAILED",
            "CLOSE_RECONCILIATION_REQUIRED",
            "BROKER_LOCAL_DAILY_AGGREGATE_DIVERGENCE",
            "BROKER_ORDER_NUMBER_COVERAGE_REVIEW"} <= codes
    assert result["p0_count"] == 3
    assert result["p1_count"] == 3
    assert repr(report) == before


def test_healthy_close_has_no_findings():
    assert audit_us_close_report({
        "broker_recovery_health": {"available": True},
        "report_consistency": "OK",
        "close_integrity_status": "OK",
        "kis_actual_fill_order_count": 3,
        "daily_fills_confirmed_total": 3,
        "unique_broker_order_count": 3,
    })["findings"] == []


def test_missing_broker_health_is_visible():
    result = audit_us_close_report({})
    assert result["p0_count"] == 1
    assert result["findings"][0]["code"] == "BROKER_RECOVERY_HEALTH_UNAVAILABLE"
