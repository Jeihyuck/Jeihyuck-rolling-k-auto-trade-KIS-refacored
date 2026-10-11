"""Evidence-first US strategy arbitration on one common completed-bar candidate cohort.

A qualified family competes only with other qualified families. Cross-sectional
ranks replace raw PB1-vs-Breakout score comparisons whose raw scales differ.
This never submits an order, changes sleeve ownership, or grants a new risk bypass.
"""
from __future__ import annotations

from collections import Counter

STYLES = ("pb1_pullback", "momentum", "breakout", "vcp")


def eligible_family_scores(row: dict) -> dict[str, float]:
    """Require verified completed-bar provenance for every participating family."""
    result = {}
    valid = (
        row.get("independent_entry_contract_v1") is True
        and row.get("entry_signal_proof_source") == "completed_daily_ohlcv"
    )
    if valid and row.get("pullback_pass") is True:
        result["pb1_pullback"] = max(0., min(1., float(row.get("pb1_score") or 0.)))
    if valid and row.get("momentum_pass") is True and row.get("standalone_momentum_score") is not None:
        result["momentum"] = max(0., min(1., float(row.get("momentum_score") or 0.)))
    if valid and row.get("breakout_pass") is True and float(row.get("breakout_pivot_price") or 0.) > 0:
        result["breakout"] = max(0., min(1., float(row.get("breakout_score") or 0.)))
    if (
        row.get("vcp_pass") is True and row.get("trend_template_pass") is True
        and row.get("vcp_evidence_source") == "completed_daily_ohlcv"
        and float(row.get("pivot_price") or 0.) > 0
        and float(row.get("vcp_daily_avg_volume20") or 0.) > 0
    ):
        result["vcp"] = max(0., min(1., float(row.get("vcp_score") or 0.)))
    return result


def rank_verified_families(rows: list[dict]) -> tuple[list[dict], dict]:
    """Rank each qualified family within its own cohort, with no fixed quotas.

    Keep existing total-signal and cluster restrictions downstream. Do not fill
    a shortage using unqualified Pullback or unverified VCP candidates.
    """
    qualified = [(row, eligible_family_scores(row)) for row in rows]
    distributions = {
        style: sorted(values[style] for _row, values in qualified if style in values)
        for style in STYLES
    }
    retained = []
    for row, values in qualified:
        if not values:
            continue
        ranks = {}
        for style, score in values.items():
            distro = distributions[style]
            # Relative position within family, not raw score comparison across
            # unrelated families. A small raw-score term breaks rank ties only.
            ranks[style] = round((sum(x <= score for x in distro) / len(distro)) * .99 + .01 * score, 6)
        winner = max(sorted(ranks), key=lambda style: ranks[style])
        row["independent_eligible_entry_styles"] = sorted(values)
        row["independent_arbitration_mode"] = "four_family_cohort_percentile"
        row["independent_family_quality_percentiles"] = ranks
        row["entry_style_selected"] = winner
        row["entry_style_raw"] = winner
        row["score_final"] = round(.5 * float(row.get("score_final") or 0.) + .5 * ranks[winner], 6)
        reason = row.get("reason_json")
        if isinstance(reason, dict):
            reason["entry_style_selected"] = winner
            reason["entry_style_raw"] = winner
            reason["independent_eligible_entry_styles"] = sorted(values)
            reason["independent_family_quality_percentiles"] = ranks
            reason["score_final"] = row["score_final"]
        retained.append(row)
    return retained, {
        "input_count": len(rows),
        "qualified": len(retained),
        "no_valid_family": len(rows)-len(retained),
        "family_qualified": {style: len(distributions[style]) for style in STYLES},
        "selected": dict(Counter(r["entry_style_selected"] for r in retained)),
    }
