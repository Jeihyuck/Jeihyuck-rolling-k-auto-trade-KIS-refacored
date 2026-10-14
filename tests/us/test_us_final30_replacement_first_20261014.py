from collections import Counter

import pytest

from trader.us.watchlist_builder import effective_final30_cap_violations, repair_final30_effective_caps


def roster(n):
    ai = n // 5 + 1
    ai_clusters = ["AI_SEMI", "AI_SOFTWARE", "DATA_CENTER_POWER", "MEGA_TECH"]
    other = ["HEALTHCARE", "ENERGY_MATERIALS", "OTHER", "INDUSTRIAL", "FINANCIAL", "CONSUMER_STAPLES"]
    return [{"symbol": f"S{i}", "theme_cluster": (ai_clusters[i % 4] if i < ai else other[i % 6]),
             "score_final": 1 - i / 1000} for i in range(n)]


@pytest.mark.parametrize("n", [25, 26, 28, 30])
def test_replacement_preserves_roster_at_normal_tier_boundary(n):
    rows = roster(n)
    extra = {"symbol": "REPLACEMENT", "theme_cluster": "CONSUMER_STAPLES", "score_final": 0.4}
    result, audit = repair_final30_effective_caps(rows, rows + [extra],
                                                {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.20}, n, "NEUTRAL")
    assert len(result) == n
    assert len(audit["dropped"]) == len(audit["added"]) == 1
    assert audit["cap_violations"] == []
    assert not audit["repair_failure_reason"]
    assert {r["symbol"] for r in rows} - {r["symbol"] for r in result} == {f"S{n // 5}"}


def test_two_constraints_are_repaired_without_shrinking_or_mutating_inputs():
    rows = roster(25)
    for row in rows[:6]:
        row["theme_cluster"] = "AI_SOFTWARE"
    original = [dict(r) for r in rows]
    pool = rows + [{"symbol": "NEW", "theme_cluster": "CONSUMER_STAPLES", "score_final": 0.3}]
    result, audit = repair_final30_effective_caps(rows, pool,
                                                {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.20}, 25, "NEUTRAL")
    assert len(result) == 25
    assert audit["cap_violations"] == []
    assert rows == original


@pytest.mark.parametrize("override", [
    {"score_final": 0}, {"score_final": float("nan")}, {"blocked_by_cluster_cap": True},
    {"asset_type": "etf"},
])
def test_ineligible_replacement_never_relaxes_admission(override):
    rows = roster(25)
    bad = {"symbol": "BAD", "theme_cluster": "CONSUMER_STAPLES", "score_final": 0.5, **override}
    result, audit = repair_final30_effective_caps(rows, rows + [bad], {"max_ai_tech_ratio": 0.20}, 25, "NEUTRAL")
    assert all(r["symbol"] != "BAD" for r in result)
    assert len(result) < 25
    assert audit["repair_failure_reason"]
    assert effective_final30_cap_violations(result, {"max_ai_tech_ratio": 0.20}, "NEUTRAL") == []


def test_no_replacement_keeps_failure_reason_and_existing_underfill_policy():
    rows = roster(25)
    result, audit = repair_final30_effective_caps(rows, rows, {"max_ai_tech_ratio": 0.20}, 25, "NEUTRAL")
    assert len(result) == 23
    assert audit["repair_failure_reason"] == "qualified_replacement_pool_exhausted"
    assert audit["cap_violations"] == []
