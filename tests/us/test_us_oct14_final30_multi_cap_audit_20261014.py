"""Multiple-cap replacement and explainable underfill acceptance for PR #220."""
from __future__ import annotations

from collections import Counter

from trader.us.watchlist_builder import (
    effective_final30_cap_violations,
    repair_final30_effective_caps,
)


def _rows():
    counts = {
        "HEALTHCARE": 6, "AI_SEMI": 3, "AI_SOFTWARE": 3,
        "OTHER": 4, "FINANCIAL": 3, "INDUSTRIAL": 3,
        "ENERGY_MATERIALS": 3,
    }
    return [
        {"symbol": f"{cluster}_{index}", "theme_cluster": cluster,
         "score_final": 0.9 - idx * 0.02 - index * 0.001}
        for idx, (cluster, count) in enumerate(counts.items())
        for index in range(count)
    ]


def test_distinct_ai_and_non_ai_caps_require_two_swaps_without_shrinking():
    rows = _rows()
    assert len(rows) == 25
    caps = {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.20}
    before = effective_final30_cap_violations(rows, caps, "NEUTRAL")
    assert {"AI_TECH_COMBINED", "SINGLE_CLUSTER"} <= set(before)
    replacement = [
        {"symbol": "SAFE_STAPLE", "theme_cluster": "CONSUMER_STAPLES", "score_final": 0.5},
        {"symbol": "SAFE_OTHER", "theme_cluster": "OTHER", "score_final": 0.4},
    ]
    result, audit = repair_final30_effective_caps(
        rows, rows + replacement, caps, 25, "NEUTRAL",
    )
    assert len(result) == 25
    assert effective_final30_cap_violations(result, caps, "NEUTRAL") == []
    assert audit["original_count"] == 25
    assert set(audit["original_cap_violations"]) >= {"AI_TECH_COMBINED", "SINGLE_CLUSTER"}
    assert audit["replacement_search"]["accepted_swaps"] >= 2
    assert audit["replacement_search"]["forced_deletions"] == 0
    assert audit["repair_failure_reason"] == ""
    counts = Counter(item["theme_cluster"] for item in result)
    assert counts["HEALTHCARE"] <= 5
    assert sum(counts[x] for x in ("AI_SEMI", "AI_SOFTWARE")) <= 5


def test_invalid_replacements_report_exact_rejection_reasons_and_underfill():
    rows = _rows()
    caps = {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.20}
    rejected = [
        {"symbol": "ZERO", "theme_cluster": "OTHER", "score_final": 0},
        {"symbol": "BLOCKED", "theme_cluster": "OTHER", "score_final": 0.8,
         "blocked_by_cluster_cap": True},
        {"symbol": "NONCORE", "theme_cluster": "OTHER", "score_final": 0.8,
         "asset_type": "etf"},
    ]
    result, audit = repair_final30_effective_caps(
        rows, rows + rejected, caps, 25, "NEUTRAL",
    )
    assert len(result) < len(rows)
    assert audit["original_count"] == 25
    assert audit["actual_final30_count"] == len(result)
    assert audit["repair_failure_reason"]
    assert audit["replacement_rejection_reasons"]["invalid_or_nonpositive_score"] == 1
    assert audit["replacement_rejection_reasons"]["blocked_cluster"] == 1
    assert audit["replacement_rejection_reasons"]["not_permitted_core_etf"] == 1
    assert audit["replacement_search"]["forced_deletions"] > 0
    assert effective_final30_cap_violations(result, caps, "NEUTRAL") == []


def test_pool_iteration_order_cannot_force_unnecessary_deletion():
    import random

    rows = _rows()
    caps = {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.20}
    replacement = [
        {"symbol": "SAFE_STAPLE", "theme_cluster": "CONSUMER_STAPLES", "score_final": 0.5},
        {"symbol": "SAFE_OTHER", "theme_cluster": "OTHER", "score_final": 0.4},
    ]
    for seed in range(20):
        pool = rows + replacement
        random.Random(seed).shuffle(pool)
        result, audit = repair_final30_effective_caps(rows, pool, caps, 25, "NEUTRAL")
        assert len(result) == 25, (seed, audit)
        assert audit["cap_violations"] == [], (seed, audit)
        assert audit["replacement_search"]["forced_deletions"] == 0, (seed, audit)


def test_twenty_eight_ai_six_two_safe_additions_fill_thirty_without_swap():
    """The 6/28 -> 6/30 denominator bridge must not delete an AI candidate."""
    rows = [
        {"symbol": f"AI_{i}", "theme_cluster": ("AI_SEMI" if i < 3 else "AI_SOFTWARE"),
         "score_final": 0.99 - i * 0.01}
        for i in range(6)
    ]
    clusters = ["HEALTHCARE", "FINANCIAL", "INDUSTRIAL", "OTHER", "ENERGY_MATERIALS", "CONSUMER_STAPLES"]
    rows += [
        {"symbol": f"OTHER_{i}", "theme_cluster": clusters[i % len(clusters)],
         "score_final": 0.8 - i * 0.002}
        for i in range(22)
    ]
    assert len(rows) == 28
    replacements = [
        {"symbol": "NON_AI_B", "theme_cluster": "FINANCIAL", "score_final": 0.25},
        {"symbol": "NON_AI_A", "theme_cluster": "OTHER", "score_final": 0.26},
    ]
    caps = {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.20}
    assert "AI_TECH_COMBINED" in effective_final30_cap_violations(rows, caps, "NEUTRAL")
    for seed in range(20):
        import random
        pool = rows + replacements
        random.Random(seed).shuffle(pool)
        new_rows, audit = repair_final30_effective_caps(rows, pool, caps, 30, "NEUTRAL")
        assert len(new_rows) == 30, (seed, audit)
        assert {r["symbol"] for r in rows} <= {r["symbol"] for r in new_rows}
        assert {r["symbol"] for r in replacements} <= {r["symbol"] for r in new_rows}
        assert audit["replacement_search"]["forced_deletions"] == 0
        assert audit["cap_violations"] == []
        assert audit["repair_failure_reason"] == ""
        assert audit["replacement_search"]["joint_refill_count"] >= 1


def test_target_underfill_is_reported_even_when_count_increased():
    # Use a genuine >=25 candidate normal-tier roster; the tiny 4-name
    # synthetic roster is subject to unrelated Final30 per-cluster minima.
    clusters = ["HEALTHCARE", "FINANCIAL", "INDUSTRIAL",
                "OTHER", "ENERGY_MATERIALS", "CONSUMER_STAPLES"]
    rows = [
        {"symbol": f"SAFE_{i}", "theme_cluster": clusters[i % len(clusters)],
         "score_final": 0.7}
        for i in range(23)
    ] + [
        {"symbol": f"AI_{i}", "theme_cluster": ["AI_SOFTWARE", "MEGA_TECH", "AI_SEMI", "AI_SOFTWARE", "MEGA_TECH"][i], "score_final": 0.75}
        for i in range(5)
    ]
    pool = rows + [{"symbol": "SAFE_NEW", "theme_cluster": "INDUSTRIAL",
                    "score_final": 0.6}]
    caps = {"max_ai_tech_ratio": 0.2, "max_single_cluster_ratio": 0.2}
    assert effective_final30_cap_violations(rows, caps, "NEUTRAL") == []
    repaired, audit = repair_final30_effective_caps(
        rows, pool, caps, 30, "NEUTRAL",
    )
    assert len(repaired) == 29
    assert audit["cap_violations"] == []
    assert audit["repair_failure_reason"] == "qualified_replacement_pool_exhausted"
    assert audit["actual_final30_count"] == 29


def test_five_clusters_five_each_require_all_five_joint_additions():
    """25->30: every individual/intermediate append breaches a 20% sector cap."""
    clusters = ["HEALTHCARE", "FINANCIAL", "INDUSTRIAL", "OTHER", "CONSUMER_STAPLES"]
    base = [
        {"symbol": f"{name}_{i}", "theme_cluster": name, "score_final": 0.95 - i * 0.01}
        for name in clusters for i in range(5)
    ]
    increments = [
        {"symbol": f"EXTRA_{name}", "theme_cluster": name, "score_final": 0.45}
        for name in clusters
    ]
    caps = {"max_single_cluster_ratio": 0.20, "max_ai_tech_ratio": 0.20}
    assert effective_final30_cap_violations(base, caps, "NEUTRAL") == []
    import random
    for seed in range(20):
        candidates = base + increments
        random.Random(seed).shuffle(candidates)
        repaired, audit = repair_final30_effective_caps(base, candidates, caps, 30, "NEUTRAL")
        assert len(repaired) == 30, (seed, audit)
        assert {r["symbol"] for r in repaired} == {r["symbol"] for r in candidates}
        assert audit["replacement_search"]["joint_refill_slots"] == 5
        assert audit["replacement_search"]["joint_search_complete"] is True
        assert audit["replacement_search"]["forced_deletions"] == 0
        assert audit["cap_violations"] == []
        assert audit["repair_failure_reason"] == ""


def test_cap_infeasibility_is_not_mislabeled_as_candidate_exhaustion():
    clusters = ["HEALTHCARE", "FINANCIAL", "INDUSTRIAL", "OTHER", "CONSUMER_STAPLES"]
    base = [
        {"symbol": f"{name}_{i}", "theme_cluster": name, "score_final": 0.90}
        for name in clusters for i in range(5)
    ]
    # Five legitimate scored candidates, but all in one already capped sector.
    options = [
        {"symbol": f"HEALTH_NEW_{i}", "theme_cluster": "HEALTHCARE", "score_final": 0.60}
        for i in range(5)
    ]
    repaired, audit = repair_final30_effective_caps(
        base, base + options,
        {"max_single_cluster_ratio": 0.20, "max_ai_tech_ratio": 0.20},
        30, "NEUTRAL",
    )
    assert len(repaired) == 25, audit
    assert audit["remaining_eligible_replacements"] == 5
    assert audit["missing_target_slots"] == 5
    assert audit["repair_failure_reason"] == "cap_constrained_no_feasible_expansion"
    assert audit["replacement_search"]["joint_search_complete"] is True
