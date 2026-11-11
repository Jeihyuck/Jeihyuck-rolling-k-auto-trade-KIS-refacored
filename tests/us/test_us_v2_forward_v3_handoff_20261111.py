"""Forward-only US_STANDARD V2->V3: no synthetic BUY, no live KIS/DB writes."""
from copy import deepcopy
import hashlib

import pytest

from trader.us.entry_exit_contract import (
    US_ENTRY_EXIT_CONTRACT_VERSION, US_SWING_V3_CONTRACT_VERSION,
    build_us_entry_exit_contract, extract_us_entry_exit_contract,
    verify_us_entry_exit_contract,
)
from trader.us.pb1.us_forward_v3_handoff import (
    PROOF_SOURCE, issue_forward_v3_handoff,
    verified_forward_v3_management,
    verify_forward_v3_order_authorization,
)
from trader.us.execution.order_router import _semantic_action_instance


def evidence_fixture(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SWING_EXIT_MODE", "LEGACY_V2")
    monkeypatch.setenv("US_V2_V3_FORWARD_ACTIVE", "1")
    monkeypatch.setenv("US_V2_V3_HANDOFF_SIGNING_KEY", "fixture-" + "A" * 40)
    contract = build_us_entry_exit_contract({
        "symbol":"AMGN", "strategy_owner":"US_STANDARD",
        "position_action":"NEW_POSITION_BUY",
        "book":"SWING_BOOK","horizon":"SWING_CARRY",
    })
    assert verify_us_entry_exit_contract(contract)
    assert contract["version"] == US_ENTRY_EXIT_CONTRACT_VERSION
    raw_acct = "practice:fixture:01"
    account_hash = hashlib.sha256(raw_acct.encode()).hexdigest()
    pos = {
        "symbol":"AMGN", "strategy_owner":"US_STANDARD", "qty":6,
        "orderable_qty":6,"as_of":"2026-11-11",
        "trading_epoch_id":"active-epoch", "account_id":account_hash,
        "position_cycle_id":"original-cycle",
        "portfolio_epoch_id":"original-portfolio",
        "meta":{
            "strategy_owner":"US_STANDARD",
            "entry_exit_contract":contract,
            "entry_exit_contract_sha256":contract["sha256"],
            "entry_exit_contract_version":contract["version"],
            "position_lifecycle_id":"original-cycle",
            "position_cycle_id":"original-cycle",
            "portfolio_epoch_id":"original-portfolio",
            "broker_account_hash":account_hash,
            "trading_epoch_id":"active-epoch",
        },
        "risk_state":{"state":{"lifecycle":{
            "is_open":True,"lifecycle_id":"original-cycle",
            "position_cycle_id":"original-cycle",
            "portfolio_epoch_id":"original-portfolio",
            "trading_epoch_id":"active-epoch",
            "account_id":raw_acct,
            "strategy_owner":"US_STANDARD",
            "entry_policy":{"entry_exit_contract_sha256":contract["sha256"]},
        }}},
    }
    proof = {
        "source":PROOF_SOURCE,"symbol":"AMGN",
        "broker_account_hash":account_hash,
        "trading_epoch_id":"active-epoch",
        "position_cycle_id":"original-cycle",
        "portfolio_epoch_id":"original-portfolio",
        "as_of":"2026-11-11",
        "holding_qty":6,"orderable_qty":6,
        "unique_original_buy_verified":True,
        "tp_stage_journal_verified":True,
        "unresolved_order_count":0,
        "competing_sell_count":0,
    }
    return pos,proof


def activate(monkeypatch):
    pos,proof = evidence_fixture(monkeypatch)
    auth = issue_forward_v3_handoff(
        pos,broker_evidence=proof,
        signing_key="fixture-"+"A"*40,
    )
    pos["meta"]["us_v2_v3_forward_handoff"] = deepcopy(auth)
    pos["risk_state"]["state"]["us_v2_v3_forward_handoff"] = deepcopy(auth)
    return pos,auth


def test_verified_forward_handoff_preserves_original_buy_and_routing_identity(monkeypatch):
    pos,auth=activate(monkeypatch)
    original = deepcopy(pos["meta"]["entry_exit_contract"])
    overlay=verified_forward_v3_management(pos)
    assert overlay is not None
    v3, proof = overlay
    assert v3["version"] == US_SWING_V3_CONTRACT_VERSION
    assert v3["forward_management_from_v2"]["source_buy_contract_sha256"] == original["sha256"]
    assert pos["meta"]["entry_exit_contract"] == original
    assert extract_us_entry_exit_contract(pos)["version"] == US_ENTRY_EXIT_CONTRACT_VERSION
    assert v3["management"]["swing"] == original["management"]["swing"]
    meta = {
        "strategy_owner":"US_STANDARD",
        "semantic_action":"PROFIT_DEFENSE_PARTIAL",
        "source_entry_contract_version":US_ENTRY_EXIT_CONTRACT_VERSION,
        "source_entry_contract_sha256":original["sha256"],
        "forward_management_contract_sha256":v3["sha256"],
        "us_v2_v3_forward_handoff":proof,
    }
    intent = {
        "symbol":"AMGN", "side":"SELL","qty":2,
        "strategy_owner":"US_STANDARD",
        "trade_date":"2026-11-11",
        "position_lifecycle_id":"original-cycle",
        "meta":meta,
    }
    assert verify_forward_v3_order_authorization(intent,auth)
    assert _semantic_action_instance(
        intent,meta,"PROFIT_DEFENSE_PARTIAL",None,
    ) == "V2_FORWARD_V3:"+v3["sha256"]


@pytest.mark.parametrize("change",[
    "wrong_broker_qty","open_order","missing_stage_journal",
    "wrong_owner","wrong_cycle","missing_risk_state",
])
def test_unverified_legacy_fails_closed(monkeypatch,change):
    pos,proof=evidence_fixture(monkeypatch)
    if change=="wrong_broker_qty":
        proof["holding_qty"]=5
    elif change=="open_order":
        proof["unresolved_order_count"]=1
    elif change=="missing_stage_journal":
        proof["tp_stage_journal_verified"]=False
    elif change=="wrong_owner":
        pos["strategy_owner"]="TQQQ_INFINITE"
    elif change=="wrong_cycle":
        pos["position_cycle_id"]="foreign-cycle"
    elif change=="missing_risk_state":
        pos["risk_state"]={"state":{}}
    with pytest.raises(ValueError):
        issue_forward_v3_handoff(
            pos,broker_evidence=proof,signing_key="fixture-"+"A"*40,
        )


@pytest.mark.parametrize("change",[
    "quantity","asof","signature","risk_copy","v2_sha","account","no_key",
])
def test_tampered_or_stale_forward_authorization_cannot_trigger_v3(monkeypatch,change):
    pos,auth=activate(monkeypatch)
    if change=="quantity":
        pos["qty"]=5
    elif change=="asof":
        pos["as_of"]="2026-11-12"
    elif change=="signature":
        pos["meta"]["us_v2_v3_forward_handoff"]["approval_hmac_sha256"]="0"*64
    elif change=="risk_copy":
        del pos["risk_state"]["state"]["us_v2_v3_forward_handoff"]
    elif change=="v2_sha":
        pos["meta"]["entry_exit_contract_sha256"]="0"*64
    elif change=="account":
        pos["account_id"]="0"*64
    elif change=="no_key":
        monkeypatch.delenv("US_V2_V3_HANDOFF_SIGNING_KEY")
    assert verified_forward_v3_management(pos) is None


def test_foreign_owner_and_legacy_missing_cycles_never_adopt(monkeypatch):
    pos,_=evidence_fixture(monkeypatch)
    pos["symbol"]="TQQQ"
    assert verified_forward_v3_management(pos) is None
    pos,_=evidence_fixture(monkeypatch)
    pos["position_cycle_id"]=None
    pos["meta"].pop("position_cycle_id")
    pos["meta"].pop("position_lifecycle_id")
    pos["risk_state"]["state"]["lifecycle"]={}
    assert verified_forward_v3_management(pos) is None
