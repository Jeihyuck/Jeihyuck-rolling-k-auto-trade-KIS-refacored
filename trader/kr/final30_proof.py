"""Preserve PREP's completed-daily family evidence in PB1 runtime candidates.

The only trusted source for the four-family entry proof is the immutable,
validated Final30 scored PREP row for that exact code. Do NOT reconstruct
missing proof from runtime scores, current price or the trading date.
Do not transfer stale exit policy from PREP: final arbitration selects its owner.
"""
from __future__ import annotations

from typing import Any

PROOF_KEYS = (
    "as_of",
    "candidate_family_proof_as_of",
    "candidate_family_screens",
    "candidate_breakout_evidence",
    "candidate_family_quality_percentiles",
    "selected_family_quality_percentile",
    "breakout_completed_proof_valid",
    "breakout_score_source",
    "breakout_pivot_price",
    "vcp_pass",
    "minervini_pass",
    "vcp_evidence_as_of",
)


def copy_final30_completed_daily_proof(
    runtime_features: dict[str, Any],
    scored_final30_row: dict[str, Any] | None,
    *,
    expected_code: str,
) -> dict[str, Any]:
    """Attach verbatim PREP evidence only if source row identifies the code.

    Missing and mismatched proof stays missing: downstream four-family gates
    remain fail-closed. Never coerce a missing screen/proof into True.

    Do not import entry/exit policy fields (such as exit_policy_family):
    four-family arbitration may have changed the chosen entry style after
    the PREP row acquired those policy fields. build_entry_exit_plan must
    derive the position's exit contract from the final winning style.
    """
    result = dict(runtime_features)
    source = scored_final30_row if isinstance(scored_final30_row, dict) else {}
    actual = str(source.get("code") or source.get("symbol") or "").strip().lstrip("A").zfill(6)
    expected = str(expected_code or "").strip().lstrip("A").zfill(6)
    if not actual or actual == "000000" or actual != expected:
        return result

    for key in PROOF_KEYS:
        if key not in source or source[key] is None:
            continue
        value = source[key]
        if key in {"candidate_family_screens", "candidate_breakout_evidence",
                   "candidate_family_quality_percentiles"}:
            if not isinstance(value, dict):
                continue
            result[key] = dict(value)
        else:
            result[key] = value
    return result
