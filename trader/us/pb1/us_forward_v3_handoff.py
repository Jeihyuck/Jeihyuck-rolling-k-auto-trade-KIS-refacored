"""Prospective V3 management of a verified, originally V2 US_STANDARD swing.

The original BUY-time signed V2 contract and original portfolio/epoch identity
MUST NOT be rewritten. An independently HMAC-authorized forward management
contract is effective only for one exact KIS practice balance snapshot, owner,
open lifecycle, stage-journal state, and matching account/epoch/quantity.

No broker I/O or DB writes occur here. A separate trusted operator running
read-only KIS/DB evidence collection must issue and atomically persist both
copies of the authorization. Absent that proof V2 remains authoritative.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from copy import deepcopy
from datetime import date
from typing import Any

from trader.us.entry_exit_contract import (
    US_ENTRY_EXIT_CONTRACT_VERSION, US_SWING_V3_CONTRACT_VERSION,
    extract_us_entry_exit_contract, verify_us_entry_exit_contract,
)
from trader.us.pb1.swing_runner_exit_v3 import V3_RUNNER_POLICY

VERSION = "us_standard_v2_forward_v3_management_v1"
PROOF_SOURCE = "KIS_DIRECT_BALANCE_ORDER_AND_TP_JOURNAL"


def _exact_positive(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        val = int(value)
        return val if val > 0 else None
    return None


def _canon(payload: dict) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, default=str,
    ).encode("utf-8")


def _sign(payload: dict, key: str) -> str:
    if len(key.encode("utf-8")) < 32:
        raise ValueError("V3_FORWARD_SIGNING_KEY_UNVERIFIED")
    return hmac.new(key.encode("utf-8"), _canon(payload), hashlib.sha256).hexdigest()


def _scope(position: dict) -> dict | None:
    meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    risk = position.get("risk_state") if isinstance(position.get("risk_state"), dict) else {}
    state = risk.get("state") if isinstance(risk.get("state"), dict) else {}
    lifecycle = state.get("lifecycle") if isinstance(state.get("lifecycle"), dict) else {}
    contract = extract_us_entry_exit_contract(position, meta)
    symbol = str(position.get("symbol") or "").upper()
    qty = _exact_positive(position.get("qty"))
    orderable = _exact_positive(position.get("orderable_qty"))
    cycle = str(position.get("position_cycle_id") or meta.get("position_cycle_id") or "")
    portfolio = str(position.get("portfolio_epoch_id") or meta.get("portfolio_epoch_id") or "")
    epoch = str(position.get("trading_epoch_id") or meta.get("trading_epoch_id") or "")
    acct_hash = str(position.get("account_id") or meta.get("broker_account_hash") or "")
    as_of = str(position.get("as_of") or position.get("trade_date") or "")
    try:
        date.fromisoformat(as_of)
    except (ValueError, TypeError):
        return None
    if not (
        symbol and symbol != "TQQQ"
        and str(position.get("strategy_owner") or meta.get("strategy_owner") or "") == "US_STANDARD"
        and contract.get("version") == US_ENTRY_EXIT_CONTRACT_VERSION
        and verify_us_entry_exit_contract(contract)
        and meta.get("entry_exit_contract_sha256") == contract.get("sha256")
        and (contract.get("management") or {}).get("book") == "SWING_BOOK"
        and (contract.get("management") or {}).get("horizon") == "SWING_CARRY"
        and qty is not None and orderable is not None and orderable <= qty
        and cycle and portfolio and epoch
        and re.fullmatch("[a-f0-9]{64}", acct_hash)
        and str(meta.get("position_lifecycle_id") or "") == cycle
        and str(meta.get("portfolio_epoch_id") or "") == portfolio
        and str(meta.get("broker_account_hash") or "") == acct_hash
        and lifecycle.get("is_open") is True
        and str(lifecycle.get("lifecycle_id") or "") == cycle
        and str(lifecycle.get("position_cycle_id") or "") == cycle
        and str(lifecycle.get("portfolio_epoch_id") or "") == portfolio
        and str(lifecycle.get("trading_epoch_id") or "") == epoch
        and str(lifecycle.get("strategy_owner") or "") == "US_STANDARD"
        and hashlib.sha256(
            str(lifecycle.get("account_id") or "").encode("utf-8")
        ).hexdigest() == acct_hash
        and (lifecycle.get("entry_policy") or {}).get("entry_exit_contract_sha256")
            == contract.get("sha256")
    ):
        return None
    return {
        "symbol":symbol, "account_hash":acct_hash,
        "cycle":cycle, "portfolio":portfolio, "epoch":epoch,
        "as_of":as_of, "qty":qty, "orderable_qty":orderable,
        "original_v2_sha256":contract["sha256"],
        "original_contract":contract, "state":state,
    }


def _v3_forward_contract(source: dict) -> dict:
    old = source["original_contract"]
    new = deepcopy({k:v for k,v in old.items() if k != "sha256"})
    new["version"] = US_SWING_V3_CONTRACT_VERSION
    management = new["management"]
    management["runner_exit"] = deepcopy(V3_RUNNER_POLICY)
    capture = management.setdefault("profit_capture", {})
    capture.update({
        "enabled": True, "partial_exit_allowed": True,
        "runner_min_remain_pct": 0.40,
        "stages": [
            {"flag":"tp1_done","reason":"TAKE_PROFIT_TP1",
             "threshold_fraction":0.03,"sell_fraction":0.25},
            {"flag":"tp2_done","reason":"TAKE_PROFIT_TP2",
             "threshold_fraction":0.05,"sell_fraction":0.25},
            {"flag":"tp3_done","reason":"TAKE_PROFIT_TP3",
             "threshold_fraction":0.08,"sell_fraction":0.20},
        ],
    })
    # Source V2 contract is NOT retroactively a V3 BUY contract.
    new["forward_management_from_v2"] = {
        "source_buy_contract_sha256":source["original_v2_sha256"],
        "original_cycle":source["cycle"],
        "effective_from":source["as_of"],
    }
    new["sha256"] = hashlib.sha256(_canon(new)).hexdigest()
    if not verify_us_entry_exit_contract(new):
        raise ValueError("V3_FORWARD_CONTRACT_INVALID")
    return new


def issue_forward_v3_handoff(
    position: dict, *, broker_evidence: dict, signing_key: str,
) -> dict:
    """Issue only from independently certified KIS+TP facts.

    The caller must obtain broker_evidence by read-only KIS and DB queries;
    hand-written or projected data is not original broker proof. Never write
    either copy of the result without an atomic, scoped DB transaction.
    """
    if os.getenv("KIS_ENV") != "practice":
        raise ValueError("V3_FORWARD_PRACTICE_ONLY")
    scope = _scope(position)
    if scope is None:
        raise ValueError("V3_FORWARD_ORIGINAL_V2_SCOPE_UNVERIFIED")
    required = {
        "source":PROOF_SOURCE,
        "symbol":scope["symbol"],
        "broker_account_hash":scope["account_hash"],
        "trading_epoch_id":scope["epoch"],
        "position_cycle_id":scope["cycle"],
        "portfolio_epoch_id":scope["portfolio"],
        "as_of":scope["as_of"],
        "holding_qty":scope["qty"],
        "orderable_qty":scope["orderable_qty"],
        "unique_original_buy_verified":True,
        "tp_stage_journal_verified":True,
        "unresolved_order_count":0,
        "competing_sell_count":0,
    }
    if (not isinstance(broker_evidence,dict)
            or any(type(broker_evidence.get(k)) is not type(v)
                   or broker_evidence.get(k) != v for k,v in required.items())):
        raise ValueError("V3_FORWARD_BROKER_OR_STAGE_PROOF_MISSING")
    forward = _v3_forward_contract(scope)
    body = {
        "version":VERSION,"source":PROOF_SOURCE,
        **{k:scope[k] for k in (
            "symbol","account_hash","cycle","portfolio","epoch","as_of",
            "qty","orderable_qty","original_v2_sha256",
        )},
        "forward_v3_contract":forward,
        "broker_truth_digest_sha256":hashlib.sha256(_canon(broker_evidence)).hexdigest(),
        "broker_order_states_terminal":True,
        "tp_journal_reconciled":True,
    }
    return {**body,"approval_hmac_sha256":_sign(body,signing_key)}


def verify_forward_v3_order_authorization(intent: dict, auth: Any) -> bool:
    """Final canonical-router check; never treat a timestamp as a BUY rebind."""
    key = os.getenv("US_V2_V3_HANDOFF_SIGNING_KEY", "")
    meta = intent.get("meta") if isinstance(intent.get("meta"),dict) else {}
    if not key or not isinstance(auth,dict):
        return False
    body = {k:v for k,v in auth.items() if k != "approval_hmac_sha256"}
    try:
        signed = _sign(body,key)
    except ValueError:
        return False
    signed_contract=auth.get("forward_v3_contract")
    return bool(
        os.getenv("KIS_ENV")=="practice"
        and os.getenv("US_V2_V3_FORWARD_ACTIVE","1") == "1"
        and auth.get("version")==VERSION and auth.get("source")==PROOF_SOURCE
        and hmac.compare_digest(str(auth.get("approval_hmac_sha256") or ""),signed)
        and isinstance(signed_contract,dict)
        and verify_us_entry_exit_contract(signed_contract)
        and signed_contract.get("version")==US_SWING_V3_CONTRACT_VERSION
        and (signed_contract.get("forward_management_from_v2") or {}).get(
            "source_buy_contract_sha256") == auth.get("original_v2_sha256")
        and str(intent.get("symbol") or "").upper()==auth.get("symbol")
        and str(intent.get("strategy_owner") or meta.get("strategy_owner") or "")=="US_STANDARD"
        and str(intent.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or "")==auth.get("cycle")
        and str(intent.get("trade_date") or "")==auth.get("as_of")
        and _exact_positive(intent.get("qty")) is not None
        and _exact_positive(intent.get("qty")) <= auth.get("orderable_qty",0)
        and meta.get("source_entry_contract_version")==US_ENTRY_EXIT_CONTRACT_VERSION
        and meta.get("source_entry_contract_sha256")==auth.get("original_v2_sha256")
        and meta.get("forward_management_contract_sha256")==signed_contract.get("sha256")
        and auth.get("broker_order_states_terminal") is True
        and auth.get("tp_journal_reconciled") is True
    )


def verified_forward_v3_management(position: dict) -> tuple[dict, dict] | None:
    """Return an approved V3 management overlay without changing V2 provenance."""
    scope = _scope(position)
    if scope is None:
        return None
    meta = position.get("meta") or {}
    auth = meta.get("us_v2_v3_forward_handoff")
    state_auth = scope["state"].get("us_v2_v3_forward_handoff")
    if not isinstance(auth,dict) or auth != state_auth:
        return None
    if any(auth.get(k)!=scope[k] for k in (
        "symbol","account_hash","cycle","portfolio","epoch","as_of",
        "qty","orderable_qty","original_v2_sha256",
    )):
        return None
    synthetic_intent = {
        "side":"SELL","symbol":scope["symbol"],
        "strategy_owner":"US_STANDARD",
        "position_lifecycle_id":scope["cycle"],
        "trade_date":scope["as_of"],
        "qty":1,"meta":{
            "source_entry_contract_version":US_ENTRY_EXIT_CONTRACT_VERSION,
            "source_entry_contract_sha256":scope["original_v2_sha256"],
            "forward_management_contract_sha256":(
                auth.get("forward_v3_contract") or {}
            ).get("sha256"),
        },
    }
    if not verify_forward_v3_order_authorization(synthetic_intent,auth):
        return None
    return deepcopy(auth["forward_v3_contract"]), deepcopy(auth)
