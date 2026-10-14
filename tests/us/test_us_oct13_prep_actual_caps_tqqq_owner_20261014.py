"""Oct 13 production regression: underfilled Final30 and TQQQ owner identity."""
from __future__ import annotations

from collections import Counter

from trader.us.watchlist_builder import (
    effective_final30_cap_violations, repair_final30_effective_caps,
)
from trader.us.runner.tick_entry_permission import (
    resolve_shared_tick_entry_evaluation_permission,
    resolve_tqqq_policy_entry_override_permission,
)


def _oct13_rows():
    # Exact sector counts from the 2026-10-13 KIS practice PREP artifact.
    counts = {
        "ETF_INDEX": 4, "HEALTHCARE": 4, "ENERGY_MATERIALS": 4,
        "OTHER": 3, "INDUSTRIAL": 3, "FINANCIAL": 3,
        "AI_SOFTWARE": 2, "MEGA_TECH": 2, "AI_SEMI": 1,
        "DATA_CENTER_POWER": 1, "CONSUMER_STAPLES": 1,
    }
    return [
        {"symbol": f"{cluster}_{i}", "theme_cluster": cluster,
         "score_final": 0.8 - 0.001 * i - 0.01 * idx}
        for idx, (cluster, n) in enumerate(counts.items())
        for i in range(n)
    ]


def test_oct13_actual_28_name_preparation_rebalances_instead_of_blocking():
    rows = _oct13_rows()
    assert len(rows) == 28
    constraints = {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.15}
    assert "AI_TECH_COMBINED" in effective_final30_cap_violations(rows, constraints, "AI_OFF_ROTATION")
    repaired, audit = repair_final30_effective_caps(
        rows,
        rows + [{
            "symbol": "ADDITIONAL_STAPLE", "theme_cluster": "CONSUMER_STAPLES",
            "score_final": 0.42,
        }],
        constraints, 30, "AI_OFF_ROTATION",
    )
    assert audit["dropped"]
    assert audit["cap_violations"] == []
    assert len(repaired) >= 27
    assert len(repaired) <= 30
    assert effective_final30_cap_violations(repaired, constraints, "AI_OFF_ROTATION") == []
    counts = Counter(row["theme_cluster"] for row in repaired)
    assert (counts["AI_SOFTWARE"] + counts["MEGA_TECH"] + counts["AI_SEMI"] + counts["DATA_CENTER_POWER"]) / len(repaired) <= 0.20
    assert max(counts.values()) / len(repaired) <= 0.15


def test_no_safe_replacement_can_keep_cap_clean_underfilled_set():
    rows = _oct13_rows()
    constraints = {"max_ai_tech_ratio": 0.20, "max_single_cluster_ratio": 0.15}
    repaired, audit = repair_final30_effective_caps(
        rows, rows, constraints, 30, "AI_OFF_ROTATION",
    )
    assert len(repaired) == 27
    assert audit["cap_violations"] == []


def test_runtime_prep_artifact_provenance_allows_only_tqqq_owner(monkeypatch, tmp_path):
    from trader.us import prep_contract

    monkeypatch.chdir(tmp_path)
    revision = "dba6da71acfb0ecad0d618f0da8834eaed814db0"
    contract = {
        "trade_date": "2026-10-13",
        "status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
        "contract_version": "us_sector_rotation_v3",
        "final30_hash": "verified-final30-sha256",
        "run_revision": revision,
        "trade_can_proceed": 1,
        "entry_can_proceed": 0, "exit_can_proceed": 1,
        "close_can_proceed": 1, "contract_ok": True,
        "final30_trade_ready": True, "final30_scored_count": 28,
        "score_nonzero_count": 28,
        "trade_block_reason": "sector_cap_violation_block",
    }
    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda *_: contract)
    monkeypatch.setattr(prep_contract, "_current_git_commit_sha", lambda: revision)
    guard = prep_contract.check_us_prep_guard("2026-10-13", session="afternoon")

    assert guard["prep_run_id"].startswith("artifact:2026-10-13:")
    assert guard["prep_artifact_identity_verified"] is True
    assert resolve_shared_tick_entry_evaluation_permission(
        guard, timeout_entry_block=False, session_execution_mode="NORMAL",
    ) is False  # US_STANDARD remains blocked
    assert resolve_tqqq_policy_entry_override_permission(
        guard, timeout_entry_block=False, session_execution_mode="NORMAL",
    ) is True   # TQQQ owner can independently assess its policy
    assert resolve_tqqq_policy_entry_override_permission(
        guard, timeout_entry_block=True, session_execution_mode="NORMAL",
    ) is False


def test_tqqq_owner_override_still_denied_on_revision_mismatch(monkeypatch, tmp_path):
    from trader.us import prep_contract

    monkeypatch.chdir(tmp_path)
    contract = {
        "trade_date": "2026-10-13",
        "status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
        "contract_version": "us_sector_rotation_v3",
        "final30_hash": "verified-final30-sha256",
        "run_revision": "old-revision",
        "trade_can_proceed": 1, "entry_can_proceed": 0,
        "exit_can_proceed": 1, "close_can_proceed": 1,
        "contract_ok": True, "final30_trade_ready": True,
        "final30_scored_count": 28, "score_nonzero_count": 28,
        "trade_block_reason": "sector_cap_violation_block",
    }
    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda *_: contract)
    monkeypatch.setattr(prep_contract, "_current_git_commit_sha", lambda: "other-revision")
    guard = prep_contract.check_us_prep_guard("2026-10-13", session="am")

    assert guard["version_mismatch"] is True
    assert not resolve_tqqq_policy_entry_override_permission(
        guard, timeout_entry_block=False, session_execution_mode="NORMAL",
    )
