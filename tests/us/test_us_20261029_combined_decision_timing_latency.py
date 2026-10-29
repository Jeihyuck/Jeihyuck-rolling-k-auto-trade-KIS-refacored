"""End-to-end *pure* integration proof; no KIS or DB side effects."""
from trader.us.decision_evidence_5w1h import (
    build_decision_evidence, build_exit_evidence_from_entry,
    verify_exit_entry_link, summarize_order_evidence,
)
from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract, verify_us_entry_exit_contract,
)
from trader.us.entry_timing_shadow import (
    derive_quote_freshness, evaluate_entry_timing_shadow,
)
from trader.us.execution.latency_evidence import (
    observed_tick_sample, latency_summary,
)
from trader.us.validation.integration_acceptance import evaluate_operational_readiness


def test_pb1_buy_to_signed_contract_to_sell_and_report_is_auditable():
    quote = {"source": "KIS_LIVE", "asof_epoch": 1000.0, "last": 420.0}
    freshness = derive_quote_freshness(quote, now_epoch=1002.0)
    timing = evaluate_entry_timing_shadow(
        owner="US_STANDARD", style="ENTRY_PULLBACK", prep_eligible=True,
        live_facts={**freshness, "support_reaction_confirmed": None},
    )
    assert timing["timing_state"] == "WAIT"
    assert timing["may_authorize_order"] is False
    buy = build_decision_evidence(
        decision_id="buy-1", side="BUY", strategy_owner="US_STANDARD",
        symbol="ELV", decision_at="2026-10-28T14:40:00Z",
        policy_version="us_entry_exit_contract_v2", code_sha="pin-abc",
        position_lifecycle_id="ENTRY:US_STANDARD:ELV", decision_reason="ENTRY_PULLBACK",
        facts={"score": 0.9, "timing": timing}, thresholds={"min_score": 0.05},
        market_context={"regime": "DEFENSIVE"}, sizing={"qty": 8, "limit": 422.0},
    )
    contract = build_us_entry_exit_contract({
        "symbol": "ELV", "strategy_owner": "US_STANDARD",
        "entry_style_selected": "ENTRY_PULLBACK",
        "meta": {"entry_decision_evidence_5w1h": buy,
                 "entry_decision_evidence_sha256": buy["sha256"]},
    })
    assert verify_us_entry_exit_contract(contract)
    signed_buy = contract["entry_provenance"]["entry_decision_evidence_5w1h"]
    sell = build_exit_evidence_from_entry(
        entry_evidence=signed_buy, decision_id="sell-1",
        decision_at="2026-10-29T14:40:00Z", decision_reason="EXIT_STOP",
        current_facts={"current_price": 390.0},
        changed_since_entry={"pnl_pct": -0.07},
        sizing={"qty": 8}, market_context={"regime": "DEFENSIVE"},
    )
    assert verify_exit_entry_link(sell, signed_buy)
    assert summarize_order_evidence(
        {"exit_decision_evidence_5w1h": sell}, side="SELL"
    )["status"] == "VERIFIED"


def test_diagnostics_cannot_claim_operation_pass_without_real_evidence():
    measured = observed_tick_sample({
        "tick_total_ms": 5600, "exit_engine_ms": 2200,
        "broker_http_by_attempt_ms": {"attempt-1": 390},
    })
    assert latency_summary([measured])["phases"]["broker_http"]["count"] == 1
    assert latency_summary([measured])["phases"]["ack_to_fill"]["count"] == 0
    readiness = evaluate_operational_readiness({
        "combined_ci_pass": True, "entry_evidence_hash_link_pass": True,
        "shadow_never_authorizes_orders": True,
    })
    assert readiness["status"] == "NOT_PROVEN"
    assert "kis_original_fill_reconcile_pass" in readiness["not_passed"]


def test_5w1h_and_price_opportunity_are_frozen_together_without_authorizing_buy():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from trader.us.entry_timing.opportunity import (
        attach_opportunity_proof, evaluate_price_opportunity,
        pre_submit_timing_check,
    )

    now = datetime(2026, 10, 28, 9, 45, tzinfo=ZoneInfo("America/New_York"))
    observed = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date="2026-10-28", now_et=now,
        verified_bars=[], quote={},
    )
    assert observed["state"] == "WATCH"
    buy = build_decision_evidence(
        decision_id="buy-1", side="BUY", strategy_owner="US_STANDARD",
        symbol="ELV", decision_at=now.isoformat(),
        policy_version="us_entry_exit_contract_v2", code_sha="pin-abc",
        position_lifecycle_id="ENTRY:US_STANDARD:ELV",
        decision_reason="ENTRY_PULLBACK", facts={"score": 0.9},
        thresholds={"min_score": 0.05}, market_context={},
        sizing={"qty": 8},
    )
    intent = {
        "symbol": "ELV", "side": "BUY", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "position_lifecycle_id": "ENTRY:US_STANDARD:ELV",
        "client_order_key": "buy-key-1",
        "meta": {
            "entry_decision_evidence_5w1h": buy,
            "entry_decision_evidence_sha256": buy["sha256"],
        },
    }
    attach_opportunity_proof(
        intent, observed, mode="SHADOW", decision_epoch=now.timestamp(),
    )
    contract = build_us_entry_exit_contract(intent)
    assert verify_us_entry_exit_contract(contract)
    provenance = contract["entry_provenance"]
    assert provenance["entry_decision_evidence_sha256"] == buy["sha256"]
    assert (provenance["us_entry_opportunity_v1"]["evidence_sha256"]
            == observed["evidence_sha256"])
    # A SHADOW WATCH is a recorded observation, never KIS BUY authority.
    assert pre_submit_timing_check(
        intent, now_epoch=now.timestamp(), fresh_ws_quote=None,
    ) == (False, "TIMING_PROOF_MISSING")
