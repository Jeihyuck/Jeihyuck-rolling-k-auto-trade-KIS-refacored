from __future__ import annotations

from trader.final30_quality import build_canonical_prep_verdict


def test_flow_total_failure_forces_fail_and_blocks_trade() -> None:
    quality = {
        "hard_fail_reasons": [],
        "soft_fail_reasons": ["entry_style_monoculture"],
    }
    verdict = build_canonical_prep_verdict(
        quality=quality,
        flow_failed_ratio=1.0,
        flow_fail_reason_counts={"kis:init_failed": 30},
    )

    assert verdict["quality_ok"] == 0
    assert verdict["status"] == "FAIL"
    assert verdict["trade_can_proceed"] == 0
    assert "flow_failed_ratio_hard_fail" in verdict["hard_fail_reasons"]
    assert "flow_provider_total_failure" in verdict["hard_fail_reasons"]
    assert verdict["soft_fail_reasons"] == ["entry_style_monoculture"]


def test_soft_fail_only_keeps_trade_proceed_true() -> None:
    quality = {
        "hard_fail_reasons": [],
        "soft_fail_reasons": ["entry_style_monoculture"],
    }
    verdict = build_canonical_prep_verdict(
        quality=quality,
        flow_failed_ratio=0.2,
        flow_fail_reason_counts={},
    )

    assert verdict["quality_ok"] == 1
    assert verdict["status"] == "WARN"
    assert verdict["trade_can_proceed"] == 1
    assert "entry_style_monoculture" in verdict["soft_fail_reasons"]
