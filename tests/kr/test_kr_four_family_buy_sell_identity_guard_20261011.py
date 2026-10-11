"""Prevent historical Final30 Momentum/Breakout -> Pullback BUY/SELL drift.

This tests the exact KR PB1 production identity resolver and the entry/exit
plan boundary, not a reimplemented strategy.  Real KIS orders are prohibited.
The flag-off branch must retain the existing strategy policy.
"""
from types import SimpleNamespace

import pytest

from trader.pb1_engine import PB1Engine


@pytest.mark.parametrize(
    ("style", "expected", "exit_policy"),
    [
        ("PULLBACK", "ENTRY_PULLBACK", "PULLBACK_EXIT"),
        ("MOMENTUM", "ENTRY_MOMENTUM", "MOMENTUM_EXIT"),
        ("BREAKOUT", "ENTRY_BREAKOUT", "BREAKOUT_EXIT"),
        ("VCP", "ENTRY_VCP", "SWING_STAGED_EXIT"),
    ],
)
def test_proven_final30_winner_owns_buy_reason_and_exit_plan(
    monkeypatch, style, expected, exit_policy,
):
    """A stale Pullback reason must never hijack a proven independent winner."""
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    row = {
        "entry_style_selected": style,
        "entry_reason": "ENTRY_PULLBACK" if style != "PULLBACK" else "ENTRY_MOMENTUM",
        "as_of": "2026-10-08",
        "candidate_family_proof_as_of": "2026-10-08",
        "candidate_family_screens": {style: True},
        "vcp_pass": True,
        "minervini_pass": True,
        "vcp_evidence_as_of": "2026-10-08",
        "close": 100.0, "atr_pct": .035,
        "rs_percentile": .9, "score_final": 88,
    }
    engine = PB1Engine.__new__(PB1Engine)
    identity = engine._resolve_entry_identity_from_mapping(row)
    assert identity["entry_reason"] == expected
    assert identity["entry_style_selected"] == expected
    assert identity["exit_policy_family"] == exit_policy

    # The real caller must feed the same winning style AND canonical reason to
    # build_entry_exit_plan. A stale feature entry_reason used to override it.
    received = {}

    def spy_build_entry_exit_plan(**kwargs):
        received.update(kwargs)
        class Plan:
            def to_dict(self):
                return {
                    "entry_style_selected": kwargs["entry_style_selected"],
                    "entry_reason": kwargs["entry_reason"],
                    "entry_thesis": "VERIFIED_FAMILY",
                    "trade_horizon": "SWING_CARRY",
                    "exit_policy_family": exit_policy,
                    "eod_action": "CARRY",
                    "force_eod_close": False,
                    "risk_plan": {"initial_stop": 92., "risk_R": 8.},
                    "time_plan": {"max_trading_days": 20},
                    "policy_source": "verified_family",
                    "policy_version": "test_boundary",
                }
        return Plan()

    monkeypatch.setattr("trader.pb1_engine.build_entry_exit_plan", spy_build_entry_exit_plan)
    candidate = SimpleNamespace(code="083450", market="KOSPI", features=row)
    plan, entry_meta = engine._prepare_entry_exit_plan(candidate, entry_price_for_plan=100.)
    assert received["entry_style_selected"] == style
    assert received["entry_reason"] == expected
    assert entry_meta["entry_reason"] == expected
    assert entry_meta["entry_style_selected"] == style
    assert entry_meta["exit_policy_family"] == exit_policy
    assert plan["risk_plan"]["initial_stop"] == 92.


@pytest.mark.parametrize("style", ["PULLBACK", "MOMENTUM", "BREAKOUT", "VCP"])
def test_no_unverified_style_can_override_a_legacy_frozen_buy(monkeypatch, style):
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    engine = PB1Engine.__new__(PB1Engine)
    row = {
        "entry_style_selected": style, "entry_reason": "ENTRY_PULLBACK",
        "as_of": "2026-10-08", "candidate_family_proof_as_of": "2026-10-07",
        "candidate_family_screens": {style: True},
        "vcp_pass": True, "minervini_pass": True,
        "vcp_evidence_as_of": "2026-10-07",
    }
    resolved = engine._resolve_entry_identity_from_mapping(row)
    assert resolved["entry_reason"] == "ENTRY_PULLBACK"
    assert resolved["exit_policy_family"] == "PULLBACK_EXIT"


def test_flag_off_keeps_the_legacy_buy_and_sell_contract(monkeypatch):
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "0")
    engine = PB1Engine.__new__(PB1Engine)
    row = {
        "entry_style_selected": "BREAKOUT", "entry_reason": "ENTRY_PULLBACK",
        "as_of": "2026-10-08", "candidate_family_proof_as_of": "2026-10-08",
        "candidate_family_screens": {"BREAKOUT": True},
    }
    identity = engine._resolve_entry_identity_from_mapping(row)
    assert identity["entry_reason"] == "ENTRY_PULLBACK"
    assert identity["exit_policy_family"] == "PULLBACK_EXIT"
