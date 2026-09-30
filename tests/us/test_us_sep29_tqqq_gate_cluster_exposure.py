from __future__ import annotations

import pytest

from trader.us.infinite.risk_adapter import (
    apply_tqqq_runtime_entry_override,
    effective_regime,
)
from trader.us.rotation import apply_cap_flags, compute_cluster_exposure


def _tqqq_overlay(**overrides):
    base = {
        "market_state": "DEFENSE_RISK_OFF",
        "market_regime": "DEFENSIVE",
        "entry_can_proceed": False,
        "exit_can_proceed": True,
        "trade_block_reason": "sector_cap_violation_block",
        "tqqq_context_quality": "ok",
        "tqqq_quote_stale": False,
    }
    base.update(overrides)
    return base


def test_tqqq_releases_pb1_sector_cap_entry_block_only():
    overlay = _tqqq_overlay()

    regime, multiplier, _reserve, entry_allowed, reason = effective_regime(overlay)

    assert regime in {"RISK_OFF", "DEFENSIVE"}
    assert multiplier == pytest.approx(0.5)
    assert entry_allowed is True
    # run_sleeve reads this shared field after effective_regime(); it must no
    # longer re-block a TQQQ-owned Fast-Dip decision for a PB1-only cap reason.
    assert overlay["entry_can_proceed"] is True
    assert overlay["tqqq_runtime_entry_override"] is True
    assert overlay["tqqq_runtime_entry_override_reason"] == "sector_cap_violation_block"
    assert reason == "defensive_sparse_evaluation"


def test_tqqq_does_not_release_unknown_operational_entry_block():
    overlay = _tqqq_overlay(
        trade_block_reason="prep_missing_after_preflight_recovery",
    )

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "prep_missing_after_preflight_recovery"
    assert overlay["entry_can_proceed"] is False
    assert "tqqq_runtime_entry_override" not in overlay


def test_tqqq_does_not_release_pb1_block_without_healthy_own_context():
    overlay = _tqqq_overlay(tqqq_quote_stale=True)

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "tqqq_operational_safety_not_proven"
    assert overlay["entry_can_proceed"] is False


def test_cluster_exposure_resolves_persisted_db_qty_current_px_shape():
    positions = [
        {"symbol": "NVDA", "qty": 10, "current_px": 100.0, "unrealized_pnl_usd": 25.0},
        {"symbol": "JNJ", "qty": 5, "current_px": 200.0, "unrealized_pnl_usd": -5.0},
    ]

    exposure = compute_cluster_exposure(positions, equity=2_000.0)

    assert exposure["AI_SEMI"]["cluster_market_value"] == pytest.approx(1_000.0)
    assert exposure["HEALTHCARE"]["cluster_market_value"] == pytest.approx(1_000.0)
    assert exposure["AI_SEMI"]["cluster_weight"] == pytest.approx(0.5)
    assert exposure["HEALTHCARE"]["cluster_weight"] == pytest.approx(0.5)


def test_cluster_exposure_live_and_db_shapes_are_equivalent():
    live = compute_cluster_exposure(
        [{"symbol": "NVDA", "qty": 10, "market_value_usd": 1_000.0}],
        equity=2_000.0,
    )
    persisted = compute_cluster_exposure(
        [{"symbol": "NVDA", "qty": 10, "current_px": 100.0}],
        equity=2_000.0,
    )

    assert persisted["AI_SEMI"]["cluster_market_value"] == pytest.approx(
        live["AI_SEMI"]["cluster_market_value"]
    )
    assert persisted["AI_SEMI"]["cluster_weight"] == pytest.approx(
        live["AI_SEMI"]["cluster_weight"]
    )


def test_db_shape_can_trigger_risk_off_ai_cap_instead_of_false_zero():
    exposure = compute_cluster_exposure(
        [{"symbol": "NVDA", "qty": 10, "current_px": 100.0}],
        equity=2_000.0,
    )
    flagged = apply_cap_flags(exposure, "RISK_OFF")

    assert flagged["AI_SEMI"]["cluster_weight"] == pytest.approx(0.5)
    assert flagged["AI_SEMI"]["over_cap"] is True
