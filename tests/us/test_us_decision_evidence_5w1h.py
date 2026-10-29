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

def test_exit_links_specific_position_cycle_and_detects_other_entry():
    from trader.us.decision_evidence_5w1h import (
        build_exit_evidence_from_entry, verify_exit_entry_link,
    )
    first = build_decision_evidence(**BASE)
    second = build_decision_evidence(**{**BASE, "decision_id": "d-new",
                                        "position_lifecycle_id": "cycle-2"})
    sell = build_exit_evidence_from_entry(
        entry_evidence=first, decision_id="exit-1", decision_at="2026-10-29T14:00:00Z",
        decision_reason="EXIT_STOP", current_facts={"last_price": 395.0},
        changed_since_entry={"price_change_pct": -0.064},
        sizing={"qty": 8}, market_context={"regime": "DEFENSIVE"},
    )
    assert verify_exit_entry_link(sell, first)
    assert not verify_exit_entry_link(sell, second)
    with pytest.raises(ValueError, match="integrity"):
        build_exit_evidence_from_entry(
            entry_evidence={**first, "sha256": "0" * 64}, decision_id="exit-2",
            decision_at="2026-10-29T14:00:00Z", decision_reason="EXIT_STOP",
            current_facts={}, changed_since_entry={}, sizing={}, market_context={},
        )

def test_read_only_report_marks_verified_missing_and_tampered():
    from trader.us.decision_evidence_5w1h import summarize_order_evidence
    original = build_decision_evidence(**BASE)
    report = summarize_order_evidence(
        {"entry_decision_evidence_5w1h": original,
         "entry_decision_evidence_sha256": original["sha256"]}, side="BUY"
    )
    assert report["status"] == "VERIFIED"
    assert report["decision_reason"] == "ENTRY_PULLBACK"
    assert report["facts"]["intraday_reversal_proven"] is None
    assert summarize_order_evidence({}, side="BUY")["status"] == "MISSING"
    invalid = copy.deepcopy(original)
    invalid["why"]["facts"]["pullback_pct"] = 0.12
    assert summarize_order_evidence(
        {"entry_decision_evidence_5w1h": invalid}, side="BUY"
    )["status"] == "INVALID"
    assert summarize_order_evidence(
        {"entry_decision_evidence_5w1h": original,
         "entry_decision_evidence_sha256": "other"}, side="BUY"
    )["status"] == "INVALID_DECLARED_DIGEST"

def test_report_legacy_missing_sell_evidence_not_fabricated():
    from trader.us.decision_evidence_5w1h import summarize_order_evidence
    assert summarize_order_evidence({"exit_reason": "hard_stop"}, side="SELL")["status"] == "MISSING"

def test_all_standard_sell_families_receive_the_exact_buy_contract_when_available():
    from trader.us.decision_evidence_5w1h import attach_standard_sell_evidence
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    buy = build_decision_evidence(**BASE)
    contract = build_us_entry_exit_contract({
        "symbol": "ELV", "strategy_owner": "US_STANDARD",
        "meta": {"entry_decision_evidence_5w1h": buy,
                 "entry_decision_evidence_sha256": buy["sha256"]},
    })
    position = {
        "symbol": "ELV", "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "cycle-1", "meta": {"entry_exit_contract": contract},
    }
    for kind in ("hard_stop", "TAKE_PROFIT_TP1", "defense_trim"):
        intent = {
            "symbol": "ELV", "side": "SELL", "qty": 2, "limit_price": 390.0,
            "client_order_key": "sell-" + kind, "exit_type": kind,
            "position_lifecycle_id": "cycle-1", "meta": {"decision_price": 391.0},
        }
        result = attach_standard_sell_evidence(
            intent, position, decision_at="2026-10-29T14:00:00Z",
            code_sha="exit-execution-pin",
        )
        assert result == "VERIFIED"
        assert intent["meta"]["exit_decision_evidence_5w1h"]["entry_link"]["evidence_hash"] == buy["sha256"]

def test_attach_rejects_wrong_lifecycle_without_blocking_sell():
    from trader.us.decision_evidence_5w1h import attach_standard_sell_evidence
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    buy = build_decision_evidence(**BASE)
    contract = build_us_entry_exit_contract({
        "symbol": "ELV", "strategy_owner": "US_STANDARD",
        "meta": {"entry_decision_evidence_5w1h": buy,
                 "entry_decision_evidence_sha256": buy["sha256"]},
    })
    intent = {"symbol": "ELV", "side": "SELL", "qty": 2,
              "client_order_key": "sell-other", "meta": {}}
    pos = {"symbol": "ELV", "position_lifecycle_id": "other-cycle",
           "meta": {"entry_exit_contract": contract}}
    assert attach_standard_sell_evidence(
        intent, pos, decision_at="2026-10-29T14:00:00Z",
        code_sha="sha-exit",
    ) == "LIFECYCLE_MISMATCH"
    assert "exit_decision_evidence_5w1h" not in intent["meta"]

def test_prior_sell_proof_must_match_current_entry_even_if_hash_valid():
    from trader.us.decision_evidence_5w1h import (
        attach_standard_sell_evidence, build_exit_evidence_from_entry, verify_exit_entry_link,
    )
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    buy = build_decision_evidence(**BASE)
    old_buy = build_decision_evidence(**{
        **BASE, "decision_id": "old-buy", "position_lifecycle_id": "old-cycle"
    })
    stale = build_exit_evidence_from_entry(
        entry_evidence=old_buy, decision_id="stale-sell",
        decision_at="2026-10-29T14:00:00Z", decision_reason="EXIT_STOP",
        current_facts={}, changed_since_entry={}, sizing={}, market_context={},
    )
    contract = build_us_entry_exit_contract({
        "symbol": "ELV", "strategy_owner": "US_STANDARD",
        "meta": {"entry_decision_evidence_5w1h": buy,
                 "entry_decision_evidence_sha256": buy["sha256"]},
    })
    pos = {"symbol": "ELV", "position_lifecycle_id": "cycle-1",
           "meta": {"entry_exit_contract": contract}}
    intent = {"symbol": "ELV", "side": "SELL", "qty": 8,
              "client_order_key": "current-sell", "meta": {
                  "exit_decision_evidence_5w1h": stale,
              }}
    assert attach_standard_sell_evidence(
        intent, pos, decision_at="2026-10-29T15:00:00Z", code_sha="current",
    ) == "VERIFIED"
    assert verify_exit_entry_link(intent["meta"]["exit_decision_evidence_5w1h"], buy)
