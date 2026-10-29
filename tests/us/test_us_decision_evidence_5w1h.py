import copy
import math

import pytest

from trader.us.decision_evidence_5w1h import (
    build_decision_evidence, verify_decision_evidence,
)

BASE = dict(
    decision_id="d1", side="BUY", strategy_owner="US_STANDARD", symbol="ELV",
    decision_at="2026-10-28T14:00:00Z", policy_version="policy-v1", code_sha="abc123",
    position_lifecycle_id="cycle-1", decision_reason="ENTRY_PULLBACK",
    facts={"pullback_pct": 0.06, "intraday_reversal_proven": None},
    thresholds={"pullback_range": [0.03, 0.18]}, market_context={"regime": "DEFENSIVE"},
    sizing={"qty": 8, "limit_price": 422.0},
)

def test_buy_evidence_is_stable_and_tamper_evident():
    first = build_decision_evidence(**BASE)
    assert verify_decision_evidence(first)
    assert first == build_decision_evidence(**BASE)
    tampered = copy.deepcopy(first)
    tampered["why"]["facts"]["pullback_pct"] = 0.07
    assert not verify_decision_evidence(tampered)

def test_sell_must_link_exact_entry_not_latest_symbol():
    buy = build_decision_evidence(**BASE)
    params = {**BASE, "side": "SELL", "decision_id": "d2",
              "decision_reason": "EXIT_HARD_STOP",
              "entry_decision_id": "d1", "entry_evidence_hash": buy["sha256"],
              "changed_since_entry": {"price_change_pct": -0.065}}
    sell = build_decision_evidence(**params)
    assert verify_decision_evidence(sell)
    assert sell["entry_link"]["evidence_hash"] == buy["sha256"]
    with pytest.raises(ValueError, match="sell_requires_entry"):
        build_decision_evidence(**{k: v for k, v in params.items()
                                  if k not in ("entry_decision_id", "entry_evidence_hash")})

def test_nonfinite_and_wrong_owner_fail_closed():
    with pytest.raises(ValueError, match="nonfinite"):
        build_decision_evidence(**{**BASE, "facts": {"rvol": math.nan}})
    with pytest.raises(ValueError, match="owner"):
        build_decision_evidence(**{**BASE, "symbol": "TQQQ"})
