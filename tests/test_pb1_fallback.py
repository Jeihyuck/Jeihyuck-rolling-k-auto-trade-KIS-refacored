from trader.pb1_engine import FilterThresholds, evaluate_filters


def test_pb1_tier_fallback_relaxes_volume_threshold():
    features = {
        "close": 105.0,
        "ma20": 100.0,
        "ma50": 98.0,
        "ma20_slope": 1.0,
        "pullback_pct": 7.0,
        "vol_contraction": 0.94,
        "volu_contraction": 0.98,
        "volume_missing": False,
    }
    thresholds = FilterThresholds(
        vol_contraction_max=0.95,
        volu_contraction_max=0.90,
        pullback_min=0.05,
        pullback_max=0.12,
        require_both_contractions=True,
    )

    ok_tier1, reasons_tier1 = evaluate_filters(features, "KOSPI", thresholds, require_volume=True)
    ok_tier2, _ = evaluate_filters(
        features,
        "KOSPI",
        thresholds.with_overrides(volu_contraction_max=1.05),
        require_volume=True,
    )

    assert ok_tier1 is False
    assert "volu_contraction_fail" in reasons_tier1
    assert ok_tier2 is True
