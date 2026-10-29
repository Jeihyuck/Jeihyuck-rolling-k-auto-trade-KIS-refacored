from trader.us.decision_evidence_5w1h import (
    build_decision_evidence, build_exit_evidence_from_entry, verify_exit_entry_link,
)
from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract, verify_us_entry_exit_contract,
)

def test_buy_evidence_is_frozen_and_accessible_to_sell():
    record = build_decision_evidence(
        decision_id="decision-1", side="BUY", strategy_owner="US_STANDARD",
        symbol="ELV", decision_at="2026-10-28T14:01:00Z",
        policy_version="us_entry_exit_contract_v2", code_sha="test-pin",
        position_lifecycle_id="cycle-123", decision_reason="ENTRY_PULLBACK",
        facts={"pullback": 0.06, "vwap_above": None}, thresholds={},
        market_context={"regime": "DEFENSIVE"}, sizing={"qty": 8},
    )
    contract = build_us_entry_exit_contract({
        "symbol": "ELV", "strategy_owner": "US_STANDARD",
        "entry_style_selected": "ENTRY_PULLBACK",
        "meta": {
            "entry_decision_evidence_5w1h": record,
            "entry_decision_evidence_sha256": record["sha256"],
        },
    })
    assert verify_us_entry_exit_contract(contract)
    frozen = contract["entry_provenance"]["entry_decision_evidence_5w1h"]
    assert frozen == record
    sell = build_exit_evidence_from_entry(
        entry_evidence=frozen, decision_id="sell-1",
        decision_at="2026-10-29T14:01:00Z", decision_reason="EXIT_HARD_STOP",
        current_facts={"price": 395.0}, changed_since_entry={"pnl": -0.06},
        sizing={"qty": 8}, market_context={"regime": "DEFENSIVE"},
    )
    assert verify_exit_entry_link(sell, frozen)

def test_tqqq_never_inherits_standard_frozen_evidence():
    contract = build_us_entry_exit_contract({
        "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE",
        "meta": {"entry_decision_evidence_sha256": "fake"},
    })
    assert contract == {}
