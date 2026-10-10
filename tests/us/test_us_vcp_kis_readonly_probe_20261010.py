"""Offline regression tests for the KIS VCP GET-only preflight.

The probe is not run against KIS by CI; no credentials and no order HTTP.
"""
import pytest

from scripts.us_vcp_kis_volume_readonly_probe import summarize_quotes, main


def sample(volume=185000, *, source="KIS_LIVE", age=0, price="101.15"):
    return {
        "last": price, "tvol": volume, "age_sec": age,
        "source": source, "stale": False, "quality": "FRESH",
        "latency_ms": 75.0,
    }


def test_readonly_probe_requires_two_fresh_monotonic_broker_observations():
    report = summarize_quotes([sample(185000), sample(186200)], regular_session=True)
    assert report["status"] == "OBSERVATIONS_PASS"
    assert report["sample_count"] == 2
    assert report["samples"][1]["reported_tvol"] == 186200


@pytest.mark.parametrize(
    ("rows", "regular", "reason"),
    [
        ([sample(1000)], True, "requires_two_independent_get_responses"),
        ([sample(1000), sample(1100)], False, "outside_us_regular_session"),
        ([sample(1100), sample(1000)], True, "sample_1_volume_decreased_same_session"),
        ([sample(0), sample(1000)], True, "sample_0_no_day_volume"),
        ([sample(1000), sample(1200, source="KIS_WEBSOCKET")], True, "sample_1_unverified_source"),
        ([sample(1000), sample(1200, age=40)], True, "sample_1_stale_timestamp"),
        ([sample(1000), sample(1200, price="nan")], True, "sample_1_bad_price"),
    ],
)
def test_readonly_probe_fails_closed_on_missing_or_unverifiable_evidence(rows, regular, reason):
    report = summarize_quotes(rows, regular_session=regular)
    assert report["status"] == "INCONCLUSIVE"
    assert reason in report["reasons"]


def test_readonly_probe_refuses_live_account_even_without_credentials(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "real")
    with pytest.raises(SystemExit):
        main(["--symbol", "AAPL"])


def test_readonly_probe_never_treats_zero_volume_or_zero_age_as_equal(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    report = summarize_quotes([sample(1000, age=0), sample(1100, age=0)], regular_session=True)
    assert report["status"] == "OBSERVATIONS_PASS"
    report = summarize_quotes([sample(1000, age=None), sample(1100)], regular_session=True)
    assert "sample_0_stale_timestamp" in report["reasons"]
