"""Four-style PREP stage telemetry must never alter a trading decision."""
from trader.us.watchlist_builder import summarize_us_entry_style_funnel


def _row(style, **extra):
    return {"entry_style_selected": style, **extra}


def test_us_funnel_counts_each_entry_family_without_hybrid_double_count():
    broad = [
        _row("pb1_pullback"), _row("momentum"),
        _row("breakout", breakout_pass=True),
        _row("momentum_pullback", momentum_pass=True),
        _row("vcp", vcp_pass=True, trend_template_pass=True, pivot_price=100.0),
    ]
    result = summarize_us_entry_style_funnel(broad, broad[:3], broad[-2:])
    assert result["broader_scored"]["total"] == 5
    assert result["broader_scored"]["styles"] == {
        "breakout": 1, "momentum": 1, "momentum_pullback": 1,
        "pb1_pullback": 1, "vcp": 1,
    }
    assert result["broader_scored"]["vcp_proof_pass"] == 1
    assert result["broader_scored"]["momentum_proof_pass"] == 1
    assert result["broader_scored"]["breakout_proof_pass"] == 1
    assert result["final30"]["styles"] == {"momentum_pullback": 1, "vcp": 1}


def test_us_funnel_excludes_unproven_vcp_and_malformed_pivot():
    rows = [
        _row("vcp", vcp_pass=False, trend_template_pass=True, pivot_price=120.0),
        _row("vcp", vcp_pass=True, trend_template_pass=False, pivot_price=120.0),
        _row("vcp", vcp_pass=True, trend_template_pass=True, pivot_price="not-a-number"),
        _row("vcp", vcp_pass=True, trend_template_pass=True, pivot_price=120.0),
    ]
    result = summarize_us_entry_style_funnel(rows, [], [])
    assert result["broader_scored"]["vcp_proof_pass"] == 1
    assert result["final30"]["total"] == 0


def test_us_funnel_is_observation_only_does_not_mutate_candidate_rows():
    rows = [_row("momentum", momentum_pass=True)]
    original = [dict(x) for x in rows]
    report = summarize_us_entry_style_funnel(rows, rows, rows)
    assert rows == original
    assert report["final30"]["styles"] == {"momentum": 1}


def test_no_proof_provider_is_not_reported_as_zero():
    # On dual-agent base, the independent proof provider is not wired yet.
    rows = [_row("pb1_pullback"), _row("momentum"), _row("breakout")]
    result = summarize_us_entry_style_funnel(rows, rows, rows)
    for stage in ("broader_scored", "top50", "final30"):
        for family in ("momentum", "breakout", "vcp"):
            assert result[stage][family + "_proof_pass"] is None
            assert result[stage]["proof_evaluated_counts"][family] == 0
            assert result[stage]["proof_coverage"][family] == "NOT_EVALUATED"


def test_explicit_zero_proof_is_distinguishable_from_missing_evidence():
    rows = [
        _row("momentum", momentum_pass=False, breakout_pass=False,
             vcp_pass=False, trend_template_pass=False, pivot_price=None),
        _row("pb1_pullback"),  # Existing PREP row without proof provider
    ]
    result = summarize_us_entry_style_funnel(rows, [], [])["broader_scored"]
    for family in ("momentum", "breakout", "vcp"):
        assert result[family + "_proof_pass"] == 0
        assert result["proof_evaluated_counts"][family] == 1
        assert result["proof_coverage"][family] == "PARTIAL"


def test_complete_false_proof_is_accurate_zero():
    rows = [
        _row("momentum", momentum_pass=False),
        _row("pullback", momentum_pass=False),
    ]
    result = summarize_us_entry_style_funnel(rows, [], [])["broader_scored"]
    assert result["momentum_proof_pass"] == 0
    assert result["proof_coverage"]["momentum"] == "COMPLETE"
    assert result["vcp_proof_pass"] is None


def test_no_rows_has_no_false_zero_proof_counts():
    result = summarize_us_entry_style_funnel([], [], [])["final30"]
    assert result["total"] == 0
    assert all(v == "NOT_EVALUATED" for v in result["proof_coverage"].values())
    assert result["momentum_proof_pass"] is None
