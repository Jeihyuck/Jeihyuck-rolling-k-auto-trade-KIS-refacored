"""US_STANDARD 2026-10-27 KIS order metadata replay (read-only DB extract).

Source: us_orders metadata entry_signal_evidence and selected entry style,
cross-checked with KIS cumulative actual fill rows. This proves *setup
evidence consistency* under the current code, NOT intraday quote causality,
volume proof, or that all 8 buys were optimal.
"""
from __future__ import annotations

import pytest

from trader.us.pb1.us_entry_engine import _validate_us_independent_new_buy_proof
from trader.us.pb1.entry_identity_proof import build_entry_identity_proof


# Original eight trade-date DB rows, no fabricated volume or timestamps:
# (symbol, qty, selected style, pullback pass, momentum pass,
#  completed prior close, standalone momentum score).
US_STANDARD_BUY_EVIDENCE_20261027 = (
    ("ABBV", 11, "ENTRY_PULLBACK", True, True, 286.30, 0.403),
    ("CL", 37, "ENTRY_PULLBACK", True, False, 94.02, None),
    ("DHR", 14, "ENTRY_PULLBACK", True, False, 231.94, None),
    ("GILD", 22, "ENTRY_PULLBACK", True, True, 158.58, 0.404),
    ("KO", 36, "ENTRY_PULLBACK", True, True, 93.0068, 0.402),
    ("KR", 52, "ENTRY_MOMENTUM", False, True, 66.58, 0.369),
    ("SYY", 41, "ENTRY_PULLBACK", True, False, 81.65, None),
    ("TMO", 5, "ENTRY_PULLBACK", True, False, 677.12, None),
)


@pytest.mark.parametrize("symbol,qty,style,pullback,momentum,close,score",
                         US_STANDARD_BUY_EVIDENCE_20261027)
def test_20261027_eight_buy_style_evidence_matches_owner_policy(
    monkeypatch, symbol, qty, style, pullback, momentum, close, score,
):
    monkeypatch.setenv("US_FOUR_FAMILY_FAIR_ARBITRATION_ENABLED", "1")
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    metadata = {
        "entry_style_selected": style,
        "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
        "pullback_pass": pullback,
        "momentum_pass": momentum,
        "pullback_completed_close": close,
        "standalone_momentum_score": score,
    }
    valid, reason = _validate_us_independent_new_buy_proof(metadata, style)
    assert valid, (symbol, qty, reason)
    proof = build_entry_identity_proof(
        metadata,
        intent={"strategy_owner": "US_STANDARD", "entry_strategy": "us_pb1",
                "entry_style_selected": style, "position_action": "NEW_POSITION_BUY",
                "trade_date": "2026-10-27"},
    )
    assert proof["strategy_owner"] == "US_STANDARD"
    assert proof["selected_style"] == style
    assert proof["volume_proof_status"] == "UNVERIFIED"
    assert proof["trigger_timestamp"] is None


def test_20261027_kr_momentum_pass_is_independent_of_pullback_score(monkeypatch):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    info = {
        "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
        "pullback_pass": False,
        "momentum_pass": True,
        "standalone_momentum_score": .369,
    }
    assert _validate_us_independent_new_buy_proof(info, "ENTRY_MOMENTUM") == (True, "")
    info["momentum_pass"] = False
    assert _validate_us_independent_new_buy_proof(info, "ENTRY_MOMENTUM")[0] is False


def test_20261027_unproven_pullback_cannot_replay_as_eligible(monkeypatch):
    monkeypatch.setenv("US_FOUR_FAMILY_FAIR_ARBITRATION_ENABLED", "1")
    info = {
        "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
        "pullback_pass": True,
        "pullback_completed_close": None,
    }
    assert _validate_us_independent_new_buy_proof(info, "ENTRY_PULLBACK")[0] is False
