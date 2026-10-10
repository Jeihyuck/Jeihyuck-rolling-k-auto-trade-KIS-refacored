"""Proof-backed four-family PREP funnel. Telemetry cannot change BUY decisions."""
from trader.us.watchlist_builder import summarize_us_entry_style_funnel


def row(style, *, signals=None, **extra):
    result = {"entry_style_selected": style, **extra}
    if signals is not None:
        result["independent_proof_status"] = dict(signals)
    return result


def test_all_four_styles_counted_once_and_signal_votes_from_real_evaluator():
    rows = [
        row("pb1_pullback", signals={"vcp": False, "momentum": False, "breakout": False}),
        row("momentum", signals={"vcp": False, "momentum": True, "breakout": False}),
        row("breakout", signals={"vcp": False, "momentum": False, "breakout": True}),
        row("momentum_pullback", signals={"vcp": False, "momentum": True, "breakout": False}),
        row("vcp", signals={"vcp": True, "momentum": False, "breakout": False}),
    ]
    report = summarize_us_entry_style_funnel(rows, rows[:3], rows[-2:])
    broad = report["broader_scored"]
    assert broad["total"] == 5
    assert broad["styles"] == {"breakout": 1, "momentum": 1, "momentum_pullback": 1, "pb1_pullback": 1, "vcp": 1}
    assert broad["vcp_proof_pass"] == 1
    assert broad["momentum_proof_pass"] == 2  # hybrid signal exists but is NOT a standalone fill
    assert broad["breakout_proof_pass"] == 1
    assert all(v == "COMPLETE" for v in broad["proof_coverage"].values())
    assert report["final30"]["styles"] == {"momentum_pullback": 1, "vcp": 1}


def test_bare_positive_legacy_flags_are_not_accepted_as_verified_proof():
    rows = [row("vcp", vcp_pass=True, trend_template_pass=True, pivot_price=100.0),
            row("momentum", momentum_pass=True),
            row("breakout", breakout_pass=True)]
    report = summarize_us_entry_style_funnel(rows, [], [])["broader_scored"]
    for family in ("momentum", "breakout", "vcp"):
        assert report[family + "_proof_pass"] is None
        assert report["proof_evaluated_counts"][family] == 0
        assert report["proof_coverage"][family] == "NOT_EVALUATED"


def test_completed_daily_provenance_available_without_arbitration_toggle():
    rows = [
        row("momentum", independent_entry_contract_v1=True,
            entry_signal_proof_source="completed_daily_ohlcv", momentum_pass=True, breakout_pass=False),
        row("breakout", independent_entry_contract_v1=True,
            entry_signal_proof_source="completed_daily_ohlcv", momentum_pass=False, breakout_pass=True),
    ]
    report = summarize_us_entry_style_funnel(rows, [], [])["broader_scored"]
    assert report["momentum_proof_pass"] == 1
    assert report["breakout_proof_pass"] == 1
    assert report["vcp_proof_pass"] is None
    assert report["proof_coverage"]["momentum"] == "COMPLETE"


def test_vcp_contract_needs_source_volume_and_actual_pivot_break():
    common = dict(vcp_evidence_source="completed_daily_ohlcv", vcp_daily_avg_volume20=50000,
                  vcp_pass=True, trend_template_pass=True, pivot_price=100.0)
    rows = [row("vcp", close=99.0, **common),
            row("vcp", close=101.0, **common),
            row("vcp", close=102.0, **{**common, "vcp_daily_avg_volume20": 0}),
            row("vcp", close=102.0, **{**common, "vcp_evidence_source": "unknown"})]
    report = summarize_us_entry_style_funnel(rows, [], [])["broader_scored"]
    assert report["vcp_proof_pass"] == 1
    assert report["proof_evaluated_counts"]["vcp"] == 2
    assert report["proof_coverage"]["vcp"] == "PARTIAL"


def test_real_false_is_zero_and_missing_proof_is_not_zero():
    rows = [
        row("momentum", signals={"momentum": False, "breakout": False, "vcp": False}),
        row("pb1_pullback"),
    ]
    report = summarize_us_entry_style_funnel(rows, [], [])["broader_scored"]
    for family in ("momentum", "breakout", "vcp"):
        assert report[family + "_proof_pass"] == 0
        assert report["proof_evaluated_counts"][family] == 1
        assert report["proof_coverage"][family] == "PARTIAL"


def test_no_proof_provider_has_null_status_and_does_not_mutate_rows():
    rows = [row("momentum"), row("pb1_pullback")]
    before = [dict(x) for x in rows]
    report = summarize_us_entry_style_funnel(rows, rows, rows)
    assert rows == before
    for stage in ("broader_scored", "top50", "final30"):
        assert report[stage]["momentum_proof_pass"] is None
        assert report[stage]["proof_coverage"]["momentum"] == "NOT_EVALUATED"


def test_empty_stage_is_not_reported_as_proven_zero():
    report = summarize_us_entry_style_funnel([], [], [])["final30"]
    assert report["total"] == 0
    assert report["momentum_proof_pass"] is None
    assert all(v == "NOT_EVALUATED" for v in report["proof_coverage"].values())
