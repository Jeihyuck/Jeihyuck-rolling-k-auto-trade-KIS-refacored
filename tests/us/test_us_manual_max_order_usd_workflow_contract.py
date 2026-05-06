# -*- coding: utf-8 -*-
"""US workflow manual max order USD override contract tests."""

from pathlib import Path


def test_us_trade_am_has_manual_max_order_usd_input_and_dry_run_guard():
    text = Path(".github/workflows/us-trade-am.yml").read_text(encoding="utf-8")

    assert "manual_max_order_usd" in text
    assert "Manual dry-run only" in text
    assert "US_MAX_ORDER_USD" in text
    assert "US_RISK_CONFIG" in text
    assert "MANUAL_OVERRIDE" in text
    assert "manual_max_order_usd is allowed only when dry_run=true" in text


def test_us_trade_afternoon_has_manual_max_order_usd_input_and_dry_run_guard():
    text = Path(".github/workflows/us-trade-afternoon.yml").read_text(encoding="utf-8")

    assert "manual_max_order_usd" in text
    assert "Manual dry-run only" in text
    assert "US_MAX_ORDER_USD" in text
    assert "US_RISK_CONFIG" in text
    assert "MANUAL_OVERRIDE" in text
    assert "manual_max_order_usd is allowed only when dry_run=true" in text


def test_default_max_order_usd_still_2500_for_safety():
    am = Path(".github/workflows/us-trade-am.yml").read_text(encoding="utf-8")
    aft = Path(".github/workflows/us-trade-afternoon.yml").read_text(encoding="utf-8")

    assert 'US_MAX_ORDER_USD: "2500"' in am
    assert 'US_MAX_ORDER_USD: "2500"' in aft
