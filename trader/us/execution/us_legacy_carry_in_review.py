"""Policy-review-only US_STANDARD v2 legacy carry-in proposal.

Original US BUY occurred BEFORE the canonical US_STANDARD portfolio epoch was
created. Backdating that epoch would falsify original order provenance. This
module produces a deterministic proposed forward-only management handoff,
never an original scope assertion, database migration, BUY/SELL, or approval.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

SYMBOLS = frozenset({"ABBV","DHR","HCA","KO","KR","MCK","PFE","PG","TMO"})
OWNER = "US_STANDARD"


def build_forward_only_v2_carry_review(
    original_plan: dict[str, Any],
    kis_certification: dict[str, Any],
    *, trading_epoch_started: str,
    active_portfolio_started: str,
    active_owner: str = OWNER,
) -> dict[str, Any]:
    """Build a reviewable, non-authorizing carry-in plan from verified facts.

    This deliberately accepts no approval argument. Even a perfect read-only
    KIS match cannot itself authorize future SELL orders or rewrite an
    original portfolio/position cycle. An independently reviewed forward-only
    management contract and WSL native broker proof are still necessary.
    """
    def blocked(reason: str) -> dict[str, Any]:
        return {"status":"BLOCKED", "reason":reason,
                "order_capable_go":False, "approval_granted":False,
                "db_rows_modified":0, "claims_released":0,
                "broker_orders_submitted":0}

    if active_owner != OWNER:
        return blocked("US_LEGACY_CARRY_OWNER_MISMATCH")
    try:
        started = date.fromisoformat(trading_epoch_started)
        portfolio_start = date.fromisoformat(active_portfolio_started)
    except (TypeError, ValueError):
        return blocked("US_LEGACY_CARRY_EPOCH_DATES_UNVERIFIED")
    if started >= portfolio_start:
        return blocked("US_LEGACY_CARRY_EPOCH_CHRONOLOGY_INVALID")
    if (kis_certification.get("broker_originals_and_balance_verified") is not True
            or kis_certification.get("status")
               != "BROKER_SOURCE_PROVEN_LEGACY_POLICY_REQUIRED"
            or kis_certification.get("tmo_original_sell")
               not in {"PROVEN_FILLED","PROVEN_CANCELLED"}):
        return blocked("US_LEGACY_CARRY_AUTHENTICATED_KIS_EVIDENCE_MISSING")
    entries=original_plan.get("entries")
    if not isinstance(entries,list) or len(entries)!=len(SYMBOLS):
        return blocked("US_LEGACY_CARRY_NINE_POSITION_SET_INCOMPLETE")
    canonical=[]
    seen=set()
    seen_lifecycles=set()
    for item in entries:
        if not isinstance(item,dict):
            return blocked("US_LEGACY_CARRY_ENTRY_INVALID")
        symbol=str(item.get("symbol") or "").upper()
        if symbol not in SYMBOLS or symbol in seen:
            return blocked("US_LEGACY_CARRY_OWNER_SET_NOT_UNIQUE")
        seen.add(symbol)
        try:
            original_day=date.fromisoformat(str(item.get("trade_date") or ""))
            raw_qty=item.get("db_qty")
            raw_requested=item.get("requested_qty")
            if isinstance(raw_qty,bool) or isinstance(raw_requested,bool):
                raise ValueError("BOOL_QUANTITY")
            qty_decimal=Decimal(str(raw_qty))
            requested_decimal=Decimal(str(raw_requested))
            if (not qty_decimal.is_finite()
                    or not requested_decimal.is_finite()
                    or qty_decimal != qty_decimal.to_integral_value()
                    or requested_decimal != requested_decimal.to_integral_value()):
                raise ValueError("FRACTIONAL_QUANTITY")
            qty=int(qty_decimal)
            requested=int(requested_decimal)
        except (ValueError,TypeError,InvalidOperation):
            return blocked("US_LEGACY_CARRY_SOURCE_QUANTITY_INVALID")
        life=str(item.get("original_lifecycle_id") or "").strip()
        contract_hash=str(item.get("original_contract_sha256") or "").strip().lower()
        if (not life or life in seen_lifecycles
                or not re.fullmatch(r"[0-9a-f]{64}",contract_hash)):
            return blocked("US_LEGACY_CARRY_IMMUTABLE_CONTRACT_UNVERIFIED")
        seen_lifecycles.add(life)
        if (not (started <= original_day < portfolio_start)
                or qty <= 0 or requested < qty
                or item.get("strategy_owner") != OWNER
                or item.get("lifecycle_present") is not True
                or item.get("frozen_contract_match") is not True
                or item.get("epoch_db_net_matches") is not True
                or item.get("original_portfolio_proven") is not False):
            return blocked("US_LEGACY_CARRY_FROZEN_V2_SOURCE_UNVERIFIED")
        canonical.append({
            "symbol":symbol,"original_buy_date":str(original_day),
            "original_lifecycle_id":life,
            "frozen_contract_sha256":contract_hash,
            "carry_quantity":qty, "original_order_quantity":requested,
            "current_portfolio_started_on":str(portfolio_start),
            "future_management_effective_at_requires_new_approval":True,
        })
    if seen != SYMBOLS:
        return blocked("US_LEGACY_CARRY_NINE_POSITION_SET_INCOMPLETE")
    canonical.sort(key=lambda x:x["symbol"])
    digest=hashlib.sha256(json.dumps(
        canonical,sort_keys=True,separators=(",",":"),
    ).encode("utf-8")).hexdigest()
    return {
        "status":"READY_FOR_POLICY_REVIEW_NOT_APPLY",
        "policy":"FORWARD_ONLY_US_STANDARD_V2_CARRY_IN",
        "strategy_owner":OWNER,
        "original_buys_before_new_portfolio":True,
        "historical_account_or_portfolio_backfilled":False,
        "original_v2_contract_must_remain_frozen":True,
        "candidate_count":len(canonical),
        "candidate_symbols":[x["symbol"] for x in canonical],
        "proposal_digest_sha256":digest,
        "effect":"Future management contract only, not original BUY attribution",
        "risk":"Incorrect adoption can cause duplicate SELL/claim collisions",
        "requires_new_reviewed_runtime_implementation":True,
        "requires_original_KIS_and_WSL_evidence":True,
        "approval_granted":False,
        "order_capable_go":False,
        "db_rows_modified":0,
        "claims_released":0,
        "broker_orders_submitted":0,
    }
