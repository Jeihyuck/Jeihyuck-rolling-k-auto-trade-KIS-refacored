"""Legacy v2 carry-in is a review proposal, not a synthetic original epoch."""
import hashlib
from trader.us.execution.us_legacy_carry_in_review import (
    SYMBOLS,build_forward_only_v2_carry_review,
)


def sample():
    plan={"entries":[{
        "symbol":symbol,"trade_date":"2026-10-27","db_qty":2,
        "requested_qty":3,"strategy_owner":"US_STANDARD",
        "lifecycle_present":True,"frozen_contract_match":True,
        "original_lifecycle_id":f"legacy-{symbol}",
        "original_contract_sha256":hashlib.sha256(symbol.encode()).hexdigest(),
        "epoch_db_net_matches":True,"original_portfolio_proven":False,
    } for symbol in sorted(SYMBOLS)]}
    cert={"status":"BROKER_SOURCE_PROVEN_LEGACY_POLICY_REQUIRED",
          "broker_originals_and_balance_verified":True,
          "tmo_original_sell":"PROVEN_CANCELLED"}
    return plan,cert


def evaluate(plan=None,cert=None):
    default_plan,default_cert=sample()
    return build_forward_only_v2_carry_review(
        plan or default_plan,cert or default_cert,
        trading_epoch_started="2026-10-25",
        active_portfolio_started="2026-11-04",
    )


def test_forward_only_review_cannot_authorize_order_or_retroactively_backfill():
    output=evaluate()
    assert output["status"]=="READY_FOR_POLICY_REVIEW_NOT_APPLY"
    assert output["candidate_count"]==9
    assert output["candidate_symbols"]==sorted(SYMBOLS)
    assert output["original_buys_before_new_portfolio"] is True
    assert output["historical_account_or_portfolio_backfilled"] is False
    assert output["original_v2_contract_must_remain_frozen"] is True
    assert len(output["proposal_digest_sha256"])==64
    assert output["order_capable_go"] is False
    assert output["approval_granted"] is False
    assert output["db_rows_modified"]==0
    assert output["claims_released"]==0
    assert output["broker_orders_submitted"]==0


def test_duplicate_symbol_or_tqqq_owner_never_generates_plan():
    plan,cert=sample()
    plan["entries"][0]["symbol"]="TQQQ"
    assert evaluate(plan,cert)["status"]=="BLOCKED"
    plan,cert=sample()
    plan["entries"][1]["symbol"]=plan["entries"][0]["symbol"]
    assert evaluate(plan,cert)["status"]=="BLOCKED"


def test_new_portfolio_date_is_not_original_entry_date():
    plan,cert=sample()
    plan["entries"][0]["trade_date"]="2026-11-04"
    assert evaluate(plan,cert)["status"]=="BLOCKED"
    plan,cert=sample()
    result=build_forward_only_v2_carry_review(
        plan,cert,trading_epoch_started="2026-11-05",
        active_portfolio_started="2026-11-04",
    )
    assert result["status"]=="BLOCKED"


def test_missing_frozen_contract_or_owner_never_adopts():
    plan,cert=sample()
    plan["entries"][0]["frozen_contract_match"]=False
    assert evaluate(plan,cert)["status"]=="BLOCKED"
    plan,cert=sample()
    plan["entries"][0]["strategy_owner"]="TQQQ_INFINITE"
    assert evaluate(plan,cert)["status"]=="BLOCKED"


def test_nonterminal_tmo_or_missing_original_kis_proof_blocks():
    plan,cert=sample()
    cert["tmo_original_sell"]="PROVEN_OPEN"
    assert evaluate(plan,cert)["status"]=="BLOCKED"
    plan,cert=sample()
    cert["broker_originals_and_balance_verified"]=False
    assert evaluate(plan,cert)["status"]=="BLOCKED"


def test_wrong_owner_argument_never_builds_forward_contract():
    plan,cert=sample()
    output=build_forward_only_v2_carry_review(
        plan,cert,trading_epoch_started="2026-10-25",
        active_portfolio_started="2026-11-04",
        active_owner="TQQQ_INFINITE",
    )
    assert output["status"]=="BLOCKED"
    assert output["order_capable_go"] is False


def test_rejected_fractional_boolean_and_nonfinite_shares():
    for bad in (2.5, True, float("nan"), float("inf"), "-1"):
        plan,cert=sample()
        plan["entries"][0]["db_qty"]=bad
        assert evaluate(plan,cert)["status"]=="BLOCKED"


def test_same_quantity_changed_contract_or_lifecycle_changes_review_digest():
    plan,cert=sample()
    base=evaluate(plan,cert)
    assert base["status"]=="READY_FOR_POLICY_REVIEW_NOT_APPLY"
    plan["entries"][0]["original_lifecycle_id"]="different-real-original-cycle"
    modified=evaluate(plan,cert)
    assert modified["proposal_digest_sha256"] != base["proposal_digest_sha256"]
    plan,cert=sample()
    plan["entries"][0]["original_contract_sha256"]="f"*64
    modified=evaluate(plan,cert)
    assert modified["proposal_digest_sha256"] != base["proposal_digest_sha256"]


def test_cannot_reuse_lifecycle_across_two_legacy_holdings():
    plan,cert=sample()
    plan["entries"][1]["original_lifecycle_id"]=plan["entries"][0]["original_lifecycle_id"]
    assert evaluate(plan,cert)["status"]=="BLOCKED"
