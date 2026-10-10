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


@pytest.mark.parametrize(
    ("et_day", "expected_regular"),
    [
        ("2026-11-26T10:30:00-05:00", False), # Thanksgiving full close
        ("2026-11-27T14:00:00-05:00", False), # Early close at 13:00 ET
        ("2026-11-27T10:30:00-05:00", True),  # Valid early-close session
        ("2026-10-10T10:30:00-04:00", False), # Saturday
        ("2026-10-09T10:30:00-04:00", True),  # Normal Friday
    ],
)
def test_readonly_probe_uses_market_calendar_not_just_weekday(
    monkeypatch, et_day, expected_regular,
):
    from datetime import datetime
    import scripts.us_vcp_kis_volume_readonly_probe as probe
    import trader.us.execution.kis_us_client as kis_module

    observed = datetime.fromisoformat(et_day)
    class FrozenClock:
        @staticmethod
        def now(zone):
            return observed.astimezone(zone)

    class ReadOnlyFakeKIS:
        def __init__(self, *, env, offline):
            assert env == "practice" and offline is False
            self.calls = 0

        def get_us_price(self, symbol, exchange):
            self.calls += 1
            return {
                "output": {"last": "101.0", "tvol": str(100000 + self.calls)},
                "_quote_source": "KIS_LIVE",
                "_quote_quality": "FRESH",
                "_quote_age_sec": 0.0,
            }

    monkeypatch.setattr(probe, "datetime", FrozenClock)
    monkeypatch.setattr(probe.time, "sleep", lambda _: None)
    monkeypatch.setattr(kis_module, "KisUSClient", ReadOnlyFakeKIS)
    monkeypatch.setenv("KIS_ENV", "practice")
    result = probe.run_probe("AAPL", "NASDAQ", samples=2, interval=1)
    assert result["regular_us_session"] is expected_regular
    assert result["status"] == (
        "OBSERVATIONS_PASS" if expected_regular else "INCONCLUSIVE"
    )
    if not expected_regular:
        assert "outside_us_regular_session" in result["reasons"]
