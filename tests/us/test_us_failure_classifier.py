# -*- coding: utf-8 -*-
"""tests/us/test_us_failure_classifier.py

US Failure Classifier 단위 테스트.
"""
import pytest
from trader.us.harness.failure_classifier import classify_failure, classify_failures_bulk


class TestClassifyFailure:
    def test_auth_401(self):
        result = classify_failure("HTTP 401 Unauthorized response from KIS")
        assert result["type"] == "US_KIS_AUTH_ERROR"

    def test_auth_403(self):
        result = classify_failure("Error: 403 Forbidden token expired")
        assert result["type"] == "US_KIS_AUTH_ERROR"

    def test_tr_id_error(self):
        result = classify_failure("Invalid TR_ID: TTTS0308U not found")
        assert result["type"] == "US_KIS_TR_ID_ERROR"

    def test_market_gate_broken(self):
        log = "[US_MARKET][CLOSED] skipping trade\n[US_ORDER][ACK] order filled"
        result = classify_failure(log)
        assert result["type"] == "US_MARKET_GATE_BROKEN"

    def test_duplicate_order_blocked(self):
        result = classify_failure("[US_DUPLICATE][BLOCK] client_order_key=abc already exists")
        assert result["type"] == "US_DUPLICATE_ORDER_BLOCKED"

    def test_balance_parse_error(self):
        result = classify_failure("balance parse failed: key 'output_9' not found")
        assert result["type"] == "US_BALANCE_PARSE_ERROR"

    def test_symbol_mapping(self):
        result = classify_failure("reject_unknown_symbol: not in registry symbol='ZZZZZ'")
        assert result["type"] == "US_SYMBOL_MAPPING_ERROR"

    def test_market_closed(self):
        result = classify_failure("[US_MARKET][CLOSED] trade skipped")
        assert result["type"] == "US_MARKET_CLOSED"

    def test_order_rejected(self):
        result = classify_failure("[US_ORDER][REJECT] reason=insufficient_cash")
        assert result["type"] == "US_ORDER_REJECTED"

    def test_traceback(self):
        result = classify_failure("Traceback (most recent call last):\n  File foo.py...")
        assert result["type"] == "US_UNKNOWN_RUNTIME_ERROR"

    def test_unknown(self):
        result = classify_failure("everything went fine today")
        assert result["type"] == "US_UNKNOWN"

    def test_empty_string(self):
        result = classify_failure("")
        assert result["type"] == "US_UNKNOWN"

    def test_none(self):
        result = classify_failure(None)  # type: ignore
        assert result["type"] == "US_UNKNOWN"

    def test_harness_fail(self):
        result = classify_failure("[US_HARNESS][FAIL] expected marker not found")
        assert result["type"] == "US_HARNESS_ASSERT_FAIL"


class TestClassifyFailuresBulk:
    def test_filters_unknown(self):
        lines = [
            "normal log line",
            "[US_DUPLICATE][BLOCK] order exists",
            "HTTP 401 bad token",
            "irrelevant line",
        ]
        results = classify_failures_bulk(lines)
        types = [r["type"] for r in results]
        assert "US_DUPLICATE_ORDER_BLOCKED" in types
        assert "US_KIS_AUTH_ERROR" in types
        # 'normal log line' 과 'irrelevant line'은 포함되지 않음
        assert len(results) == 2
