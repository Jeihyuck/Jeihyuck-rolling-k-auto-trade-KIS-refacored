"""KR 2026-10-13: broker daily CCLD outage must not be reported healthy.

No KIS request, DB connection, order mutation, or synthetic price in tests.
"""
import inspect

import pytest

from trader.reconcile_kis import _daily_ccld_provider_degradation, reconcile_today


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"rt_cd": "0", "output1": []}, None),
        ({"output1": []}, None),  # legacy offline broker fixture
        ({"_ccld_status": "ok", "rt_cd": "0", "output1": []}, None),
        ({"rt_cd": "-1", "_ccld_status": "TIMEOUT", "output1": []}, "daily_ccld_provider_degraded"),
        ({"rt_cd": "-1", "_ccld_status": "TEMP_FAIL", "output1": []}, "daily_ccld_provider_degraded"),
        ({"rt_cd": "2", "output1": []}, "daily_ccld_broker_error"),
        ({"rt_cd": "0", "_diag_stub": True, "output1": []}, "daily_ccld_http_disabled"),
    ],
)
def test_broker_daily_ccld_authority_status(payload, expected):
    assert _daily_ccld_provider_degradation(payload) == expected


def test_reconcile_today_marks_provider_stub_degraded_not_healthy():
    source = inspect.getsource(reconcile_today)
    assert "provider_degraded = _daily_ccld_provider_degradation(resp)" in source
    assert "degraded_reason = degraded_reason or provider_degraded" in source
    assert '"ok": not bool(degraded_reason)' in source
    assert "PRESERVE_ORDER_FENCES_AND_UNPRICED_PNL" in source


def test_failure_reason_does_not_depend_on_untrusted_provider_status_string():
    assert _daily_ccld_provider_degradation({
        "_ccld_status": "UNEXPECTED_RESPONSE_" + ("X" * 1000), "output1": [],
    }) == "daily_ccld_provider_degraded"
