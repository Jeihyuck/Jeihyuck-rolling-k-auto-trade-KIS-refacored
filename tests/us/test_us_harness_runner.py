# -*- coding: utf-8 -*-
"""tests/us/test_us_harness_runner.py

Harness Runner 단위 테스트.
"""
from __future__ import annotations

import pytest


class TestLogParser:
    def test_extract_markers(self):
        from trader.us.harness.log_parser import extract_markers
        text = "[US_PREP][OK] loaded 22 tickers\n[US_STRATEGY][SCORED] intents=0"
        markers = extract_markers(text)
        assert "[US_PREP][OK]" in markers
        assert "[US_STRATEGY][SCORED]" in markers

    def test_has_marker(self):
        from trader.us.harness.log_parser import has_marker
        assert has_marker("[US_ORDER][ACK] symbol=NVDA", "[US_ORDER][ACK]")
        assert not has_marker("[US_ORDER][ACK] symbol=NVDA", "[US_ORDER][REJECT]")

    def test_all_expected_present(self):
        from trader.us.harness.log_parser import all_expected_markers_present
        log = "[US_PREP][OK] done\n[US_STRATEGY][SCORED] ok"
        ok, missing = all_expected_markers_present(log, ["[US_PREP][OK]", "[US_STRATEGY][SCORED]"])
        assert ok
        assert missing == []

    def test_missing_marker_detected(self):
        from trader.us.harness.log_parser import all_expected_markers_present
        log = "[US_PREP][OK] done"
        ok, missing = all_expected_markers_present(log, ["[US_PREP][OK]", "[US_STRATEGY][SCORED]"])
        assert not ok
        assert "[US_STRATEGY][SCORED]" in missing

    def test_forbidden_marker_detected(self):
        from trader.us.harness.log_parser import any_forbidden_marker_present
        log = "Traceback (most recent call last)"
        found, items = any_forbidden_marker_present(log, ["Traceback", "[US_HARNESS][FAIL]"])
        assert found
        assert "Traceback" in items


class TestValidators:
    def test_pass(self):
        from trader.us.harness.validators import validate_scenario
        log = "[US_PREP][OK] done"
        result = validate_scenario("test_pass", log, ["[US_PREP][OK]"], [], exit_code=0)
        assert result.passed

    def test_fail_missing_marker(self):
        from trader.us.harness.validators import validate_scenario
        log = "just a log"
        result = validate_scenario("test_miss", log, ["[US_PREP][OK]"], [], exit_code=0)
        assert not result.passed
        assert "[US_PREP][OK]" in result.missing_markers

    def test_fail_forbidden_marker(self):
        from trader.us.harness.validators import validate_scenario
        log = "[US_PREP][OK] done\nTraceback"
        result = validate_scenario("test_forbidden", log, ["[US_PREP][OK]"], ["Traceback"], exit_code=0)
        assert not result.passed
        assert "Traceback" in result.forbidden_found

    def test_fail_exit_code(self):
        from trader.us.harness.validators import validate_scenario
        log = "[US_PREP][OK] done"
        result = validate_scenario("test_exit", log, ["[US_PREP][OK]"], [], exit_code=1)
        assert not result.passed


class TestHarnessManifest:
    def test_manifest_loads(self):
        from trader.us.harness.runner import load_manifest
        scenarios = load_manifest()
        assert isinstance(scenarios, list)
        assert len(scenarios) > 0

    def test_all_scenarios_have_name(self):
        from trader.us.harness.runner import load_manifest
        for scenario in load_manifest():
            assert "name" in scenario

    def test_all_scenarios_have_command(self):
        from trader.us.harness.runner import load_manifest
        for scenario in load_manifest():
            assert "command" in scenario

    def test_scenario_names(self):
        from trader.us.harness.runner import load_manifest
        names = {s["name"] for s in load_manifest()}
        expected = {
            "offline_quote_success",
            "market_closed_skip",
            "duplicate_order_block",
            "insufficient_cash_block",
            "partial_fill_reconcile",
            "kis_rate_limit_retry",
        }
        assert expected <= names

    def test_failure_classifier_integration(self):
        from trader.us.harness.failure_classifier import classify_failure
        result = classify_failure("[US_HARNESS][FAIL] expected marker not found")
        assert result["type"] == "US_HARNESS_ASSERT_FAIL"
