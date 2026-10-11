"""The KRX venue operational audit is read-only and fail-closed in practice."""
from scripts.kr_emergency_venue_readonly_probe import build_report


def _seed():
    return [
        {"code": "036930", "market": None},
        {"code": "042700", "market": None},
        {"code": "039030", "market": None},
    ]


def _emergency():
    return {"params": {"market_source": "krx_listing_verified"},
            "members": [{"code": "036930"}, {"code": "042700"}, {"code": "039030"}]}


def test_kr_venue_probe_reports_validated_cross_market_seed():
    market_map = {"036930": "KOSDAQ", "042700": "KOSPI", "039030": "KOSDAQ"}
    result = build_report(_seed(), market_map, elapsed_s=0.33, emergency=_emergency())
    assert result["status"] == "VERIFIED_INPUTS_ONLY"
    assert result["coverage"] == 1.0
    assert result["verified_by_market"] == {"KOSPI": 1, "KOSDAQ": 2}
    assert result["historical_orders_changed"] is False


def test_kr_venue_probe_fails_closed_on_missing_coverage():
    result = build_report(
        _seed(), {"036930": "KOSDAQ", "042700": "KOSPI"},
        elapsed_s=0.1, emergency=None,
    )
    assert result["status"] == "INCONCLUSIVE"
    assert result["unknown_count"] == 1


def test_kr_venue_probe_detects_conflicting_explicit_market():
    seed = _seed()
    seed[0]["market"] = "KOSPI"
    result = build_report(
        seed, {"036930": "KOSDAQ", "042700": "KOSPI", "039030": "KOSDAQ"},
        elapsed_s=0.1, emergency=_emergency(),
    )
    assert result["status"] == "INCONCLUSIVE"
    assert result["declared_conflicts"] == ["036930"]


def test_kr_venue_probe_requires_actual_fallback_payload():
    result = build_report(
        _seed(), {"036930": "KOSDAQ", "042700": "KOSPI", "039030": "KOSDAQ"},
        elapsed_s=0.1, emergency=None,
    )
    assert result["status"] == "INCONCLUSIVE"
