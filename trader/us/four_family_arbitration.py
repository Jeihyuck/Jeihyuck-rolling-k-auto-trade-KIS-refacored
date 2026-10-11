"""Evidence-first US strategy arbitration on one common completed-bar candidate cohort.

A qualified family competes only with other qualified families. Cross-sectional
ranks replace raw PB1-vs-Breakout score comparisons whose raw scales differ.
This never submits an order, changes sleeve ownership, or grants a new risk bypass.
"""
from __future__ import annotations

from collections import Counter
import math
import os

STYLES = ("pb1_pullback", "momentum", "breakout", "vcp")


def verified_vcp_arming_eligibility(row: dict) -> tuple[bool, bool]:
    """Return (verified candidate, pre-breakout standby), never a BUY signal.

    With opt-in OFF, retain #205's qualified VCP evidence semantics.
    With opt-in ON, apply the identical pivot-nearness rule in the PREP
    selector and final four-family arbiter; the broker live trigger is separate.
    """
    try:
        pivot = float(row.get("pivot_price") or 0.)
        average_volume = float(row.get("vcp_daily_avg_volume20") or 0.)
        close = float(row.get("close") or row.get("price") or 0.)
    except (TypeError, ValueError, OverflowError):
        return False, False
    if not (
        row.get("vcp_pass") is True
        and row.get("trend_template_pass") is True
        and row.get("vcp_evidence_source") == "completed_daily_ohlcv"
        and all(math.isfinite(v) and v > 0 for v in (pivot, average_volume))
    ):
        return False, False
    armed = (
        os.getenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "0") == "1"
        and os.getenv("US_MINERVINI_VCP_PROOF_ENABLED", "0") == "1"
    )
    if not armed:
        return True, False
    from trader.strategies.pb1_minervini_v2 import MinerviniConfig
    band = MinerviniConfig().max_extension_from_pivot
    if not math.isfinite(close) or not pivot * (1. - band) <= close <= pivot * (1. + band):
        return False, False
    return True, close < pivot


def is_vcp_standby_only(row: dict) -> bool:
    """An armed, not-yet-broken-out VCP must not crowd out active BUY proofs."""
    return bool(
        row.get("entry_style_selected") == "vcp"
        and row.get("vcp_setup_armed_before_breakout") is True
    )


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
    vcp_qualified, vcp_standby = verified_vcp_arming_eligibility(row)
    row.pop("vcp_setup_armed_before_breakout", None)
    if vcp_qualified:
        result["vcp"] = max(0., min(1., float(row.get("vcp_score") or 0.)))
        if (
            os.getenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "0") == "1"
            and os.getenv("US_MINERVINI_VCP_PROOF_ENABLED", "0") == "1"
        ):
            row["vcp_setup_armed_before_breakout"] = bool(vcp_standby)
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
        # VCP standby can still win when it is the sole qualified style.
        # For the same ticker, however, an actual verified Pullback,
        # Momentum or Breakout may not be displaced by an untriggered VCP.
        selectable = (
            {style: rank for style, rank in ranks.items() if style != "vcp"}
            if row.get("vcp_setup_armed_before_breakout") is True and len(ranks) > 1
            else ranks
        )
        winner = max(sorted(selectable), key=lambda style: selectable[style])
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
            if "vcp_setup_armed_before_breakout" in row:
                reason["vcp_setup_armed_before_breakout"] = True
            reason["score_final"] = row["score_final"]
        retained.append(row)
    return retained, {
        "input_count": len(rows),
        "qualified": len(retained),
        "no_valid_family": len(rows)-len(retained),
        "family_qualified": {style: len(distributions[style]) for style in STYLES},
        "selected": dict(Counter(r["entry_style_selected"] for r in retained)),
    }
