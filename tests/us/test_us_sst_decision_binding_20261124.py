"""SST audit regression for 11/23 production placeholder shape (offline).
No KIS client, network, trading policy, or production DB writes.
"""
from copy import deepcopy

import pytest

from trader.us.decision_evidence_5w1h import (
    attach_original_buy_bound_sell_audit, build_decision_evidence,
    verified_original_buy_decision_binding, verify_decision_evidence,
    verify_exit_entry_link,
)
from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract, verify_us_entry_exit_contract,
)
from trader.us.position_contract_provenance import merge_original_order_contract_metadata
from trader.us.original_buy_lineage import select_verified_us_original_buy_scope

import hashlib
ACCOUNT = "practice:offline-test:01"
HASH = hashlib.sha256(ACCOUNT.encode()).hexdigest()
SYMBOLS = ["MDT","ICE","SHW","COST","ED","MCK","ABBV","LLY","U","CME"]
SELL_FAMILIES = ["soft_stop_loss","hard_stop_loss","persistent_soft_stop_full_exit",
                 "TAKE_PROFIT_TP1","TAKE_PROFIT_TP2","TAKE_PROFIT_TP3",
                 "trailing_stop","PROFIT_DEFENSE_PARTIAL","defense_trim","portfolio_profit_guard"]


def source(symbol, monkeypatch):
    monkeypatch.setenv("KIS_ENV","practice")
    monkeypatch.setenv("US_SWING_EXIT_MODE","V3_ACTIVE_PRACTICE")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED","1")
    reason = "ENTRY_MOMENTUM" if symbol == "LLY" else "ENTRY_PULLBACK"
    entry = build_decision_evidence(
        decision_id="BUY:"+symbol, side="BUY", strategy_owner="US_STANDARD",
        symbol=symbol, decision_at="2026-11-23T16:00:00Z", policy_version="V3",
        code_sha="fixture", position_lifecycle_id="ENTRY:US_STANDARD:"+symbol,
        decision_reason=reason, facts={"pullback_pct":0.06,"reversal":None},
        thresholds={"pullback_range":[0.03,0.18]}, market_context={"regime":"RISK_OFF"},
        sizing={"qty":6,"limit_price":100.0},
    )
    contract = build_us_entry_exit_contract({
        "symbol":symbol,"strategy_owner":"US_STANDARD",
        "entry_reason":reason,"entry_style_selected":reason,
        "position_action":"NEW_POSITION_BUY","book":"SWING_BOOK","horizon":"SWING_CARRY",
        "meta":{"entry_decision_evidence_5w1h":entry,
                "entry_decision_evidence_sha256":entry["sha256"]},
    })
    assert verify_us_entry_exit_contract(contract)
    assert contract['version'] == 'us_entry_exit_contract_v3'
    cycle = "actual-cycle-"+symbol
    meta = {
        "entry_exit_contract":contract,"entry_exit_contract_sha256":contract["sha256"],
        "entry_decision_evidence_5w1h":entry,
        "entry_decision_evidence_sha256":entry["sha256"],
        "position_lifecycle_id":cycle,"position_cycle_id":cycle,
        "portfolio_epoch_id":"portfolio","broker_account_hash":HASH,
        "strategy_owner":"US_STANDARD","position_action":"NEW_POSITION_BUY",
        "pre_order_holding_qty":0,"cumulative_filled_qty":6,
        "fill_evidence_type":"KIS_ORDER_CUMULATIVE_ACTUAL","is_synthetic":False,
    }
    row = {
        "symbol":symbol,"trading_epoch_id":"epoch","account_id":HASH,
        "portfolio_epoch_id":"portfolio","position_cycle_id":cycle,
        "order_no":"order-"+symbol,"client_order_key":"original-"+symbol,
        "filled_at":"2026-11-23T16:00:01Z","fill_qty":6,"order_qty":6,
        "intent_qty":6,"order_filled":6,"order_status":"FILLED",
        "intent_meta":deepcopy(meta),"order_meta":deepcopy(meta),"fill_meta":deepcopy(meta),
    }
    identity = {"env":"practice","account_id":ACCOUNT,"trading_epoch_id":"epoch",
                "strategy_owner":"US_STANDARD","portfolio_epoch_id":"portfolio",
                "position_cycle_id":cycle,"position_lifecycle_id":cycle}
    return entry, contract, row, identity


@pytest.mark.parametrize("symbol", SYMBOLS)
@pytest.mark.parametrize("family", SELL_FAMILIES)
def test_original_placeholder_to_scoped_sell_audit(symbol, family, monkeypatch):
    entry, contract, row, identity = source(symbol, monkeypatch)
    original = deepcopy(row)
    # Historical original BUY: actual original six shares, not remaining qty.
    proof = select_verified_us_original_buy_scope([row],symbol=symbol,qty=None,identity=identity)
    binding = proof["original_buy_decision_binding"]
    assert binding["source_entry_decision_sha256"] == entry["sha256"]
    assert binding["source_entry_lifecycle_id"] == "ENTRY:US_STANDARD:"+symbol
    assert binding["position_cycle_id"] == identity["position_cycle_id"]
    sell = {"symbol":symbol,"side":"SELL","strategy_owner":"US_STANDARD",
            "position_lifecycle_id":identity["position_cycle_id"],
            "client_order_key":"sell-"+family,"reason":family,"qty":2,
            "limit_price":95.0,"meta":{}}
    args = {"contract":contract,"binding":binding,
            "decision_at":"2026-11-24T16:00:00Z","code_sha":"test"}
    assert attach_original_buy_bound_sell_audit(sell,**args) == "VERIFIED"
    evidence = sell["meta"]["exit_decision_evidence_5w1h"]
    assert evidence["what"]["position_lifecycle_id"] == identity["position_cycle_id"]
    assert evidence["why"]["decision_reason"] == family
    assert evidence["entry_link"]["evidence_hash"] == entry["sha256"]
    assert verify_exit_entry_link(evidence,entry,original_buy_binding=binding)
    assert not verify_exit_entry_link(evidence,entry)
    assert sell["meta"]["source_entry_reason"] == entry["why"]["decision_reason"]
    assert attach_original_buy_bound_sell_audit(sell,**args) == "ALREADY_VERIFIED"
    # Fill replay must retain original audit even on competing diagnostic copy.
    persisted = merge_original_order_contract_metadata(sell["meta"], {
        "source_entry_decision_sha256":"foreign", "source_entry_reason":"foreign",
        "exit_decision_evidence_5w1h":None,
    })
    assert persisted["exit_decision_evidence_5w1h"] == evidence
    assert persisted["source_entry_decision_sha256"] == entry["sha256"]
    assert persisted["source_entry_reason"] == entry["why"]["decision_reason"]
    assert persisted["original_buy_decision_replay_conflicts"]
    assert row == original and verify_decision_evidence(entry)


@pytest.mark.parametrize("fault", ["intent_contract","fill_contract","foreign_cycle",
                                   "entry_copy","entry_hash","wrong_action","wrong_timing",
                                   "duplicate_source","different_account"])
def test_no_false_bridge_even_when_exit_audit_is_optional(fault, monkeypatch):
    entry, contract, row, identity = source("MDT",monkeypatch)
    rows = [row]
    if fault == "intent_contract":
        row["intent_meta"]["entry_exit_contract"] = {"sha256":contract["sha256"]}
    elif fault == "fill_contract":
        row["fill_meta"]["entry_exit_contract"] = {"sha256":contract["sha256"]}
    elif fault == "foreign_cycle":
        row["fill_meta"]["position_cycle_id"] = "wrong-cycle"
    elif fault == "entry_copy":
        row["intent_meta"]["entry_decision_evidence_5w1h"]["why"]["facts"] = {}
    elif fault == "entry_hash":
        row["intent_meta"]["entry_decision_evidence_sha256"] = "wrong"
    elif fault == "wrong_action":
        row["intent_meta"]["position_action"] = "ADD_TO_EXISTING_BUY"
    elif fault == "wrong_timing":
        row["intent_meta"]["us_entry_opportunity_v1"] = {"submission_binding_version":1}
    elif fault == "duplicate_source":
        rows += [deepcopy(row)]
    elif fault == "different_account":
        row["account_id"] = "foreign"
    proof = select_verified_us_original_buy_scope(rows,symbol="MDT",qty=None,identity=identity)
    assert not proof or not proof.get("original_buy_decision_binding")


def test_wrong_signed_sell_binding_cannot_self_authenticate(monkeypatch):
    entry,contract,row,identity = source("MDT",monkeypatch)
    proof=select_verified_us_original_buy_scope([row],symbol="MDT",qty=None,identity=identity)
    binding=proof["original_buy_decision_binding"]
    sell={"symbol":"MDT","side":"SELL","strategy_owner":"US_STANDARD",
          "position_lifecycle_id":identity["position_cycle_id"],"qty":1,
          "client_order_key":"sell-1","reason":"hard_stop_loss","meta":{}}
    assert attach_original_buy_bound_sell_audit(
        sell,contract=contract,binding=binding,decision_at="2026-11-24T16:00:00Z",
        code_sha="test")=="VERIFIED"
    ev=sell["meta"]["exit_decision_evidence_5w1h"]
    wrong={**binding,"source_buy_client_order_key":"other-order"}
    assert not verify_exit_entry_link(ev,entry,original_buy_binding=wrong)
