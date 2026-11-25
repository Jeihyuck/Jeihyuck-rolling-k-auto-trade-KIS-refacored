"""Immutable, audit-only US decision evidence. Never authorizes or blocks broker orders."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any

VERSION = "us_decision_evidence_5w1h_v1"
ALLOWED_SIDES = {"BUY", "SELL"}

def _clean(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("nonfinite_decision_evidence")
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("nonfinite_decision_evidence")
        return value
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_clean(v) for v in value]
    raise TypeError("unsupported_decision_evidence_type")

def canonical_bytes(record: dict) -> bytes:
    return json.dumps(_clean(record), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")

def evidence_hash(record: dict) -> str:
    return hashlib.sha256(canonical_bytes(record)).hexdigest()

def build_decision_evidence(
    *, decision_id: str, side: str, strategy_owner: str, symbol: str,
    decision_at: str, policy_version: str, code_sha: str,
    position_lifecycle_id: str, decision_reason: str,
    facts: dict, thresholds: dict, market_context: dict, sizing: dict,
    entry_decision_id: str | None = None,
    entry_evidence_hash: str | None = None,
    changed_since_entry: dict | None = None,
) -> dict:
    """Capture observed facts only: never invent missing price/volume or trigger proof.

    A SELL links to the original entry by stable IDs, not by latest symbol snapshot.
    The returned digest covers the full immutable payload; narrative rendering is
    deliberately not part of the broker-critical path.
    """
    if side not in ALLOWED_SIDES or not all([
        decision_id, strategy_owner, symbol, decision_at, policy_version,
        code_sha, position_lifecycle_id, decision_reason,
    ]):
        raise ValueError("incomplete_decision_identity")
    if strategy_owner == "TQQQ_INFINITE" and symbol.upper() != "TQQQ":
        raise ValueError("strategy_owner_symbol_mismatch")
    if symbol.upper() == "TQQQ" and strategy_owner != "TQQQ_INFINITE":
        raise ValueError("strategy_owner_symbol_mismatch")
    if side == "SELL" and not (entry_decision_id and entry_evidence_hash):
        raise ValueError("sell_requires_entry_evidence_link")
    payload = {
        "version": VERSION,
        "who": {"strategy_owner": strategy_owner, "policy_version": policy_version, "code_sha": code_sha},
        "what": {"decision_id": decision_id, "side": side, "symbol": symbol.upper(),
                 "position_lifecycle_id": position_lifecycle_id},
        "when": {"decision_at": decision_at},
        "where": {"market_context": market_context},
        "why": {"decision_reason": decision_reason, "facts": facts,
                "thresholds": thresholds, "changed_since_entry": changed_since_entry},
        "how": {"sizing": sizing},
        "entry_link": {"decision_id": entry_decision_id, "evidence_hash": entry_evidence_hash},
    }
    result = _clean(payload)
    result["sha256"] = evidence_hash(result)
    return result

def verify_decision_evidence(record: dict) -> bool:
    if not isinstance(record, dict) or record.get("version") != VERSION:
        return False
    digest = record.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        return False
    try:
        return digest == evidence_hash({k: v for k, v in record.items() if k != "sha256"})
    except (TypeError, ValueError):
        return False

def verified_original_buy_decision_binding(
    *, symbol: str, cycle: str, contract: dict, intent_meta: dict,
    order_meta: dict, fill_meta: dict, original_order_key: str,
    account_hash: str, trading_epoch_id: str, portfolio_epoch_id: str,
) -> dict | None:
    """Audit-only bridge after exact native first-BUY lineage was proven.

    A placeholder is not an opened cycle. Require the *same immutable full
    contract* in original Intent, Order and native Fill, original decision
    digest, owner, symbol, first-BUY action, and the already-proven scope.
    Never re-sign the original signal or accept a ticker-only match.
    """
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract
    if (not all((cycle, original_order_key, account_hash, trading_epoch_id, portfolio_epoch_id))
            or not verify_us_entry_exit_contract(contract)):
        return None
    provenance = contract.get("entry_provenance") or {}
    entry = provenance.get("entry_decision_evidence_5w1h")
    if not isinstance(entry, dict) or not verify_decision_evidence(entry):
        return None
    what, who = entry.get("what") or {}, entry.get("who") or {}
    if (what.get("side") != "BUY" or what.get("symbol") != symbol
            or who.get("strategy_owner") != "US_STANDARD" or symbol == "TQQQ"
            or not what.get("decision_id")
            or provenance.get("entry_decision_evidence_sha256") != entry["sha256"]):
        return None
    source_scope = what.get("position_lifecycle_id")
    if source_scope not in (cycle, "ENTRY:US_STANDARD:" + symbol):
        return None
    for meta in (intent_meta, order_meta, fill_meta):
        if (meta.get("entry_exit_contract") != contract
                or meta.get("entry_exit_contract_sha256") != contract["sha256"]
                or meta.get("position_lifecycle_id") != cycle
                or meta.get("position_cycle_id") != cycle
                or meta.get("portfolio_epoch_id") != portfolio_epoch_id
                or meta.get("broker_account_hash") != account_hash
                or meta.get("strategy_owner") != "US_STANDARD"):
            return None
    if source_scope != cycle:
        # A scoped, native first BUY is the authority for the transition;
        # require the original producer action and exact original 5W1H copy.
        if (intent_meta.get("position_action") != "NEW_POSITION_BUY"
                or intent_meta.get("entry_decision_evidence_5w1h") != entry
                or intent_meta.get("entry_decision_evidence_sha256") != entry["sha256"]):
            return None
        # If the timing proof is present, require the same final V2 binding
        # across all three original records. It is additional provenance,
        # not a replacement for the exact native BUY chain.
        timing = intent_meta.get("us_entry_opportunity_v1")
        if timing is not None:
            from trader.us.entry_timing.opportunity import _verify_timing_signal_and_binding
            attempt = intent_meta.get("submit_attempt_id")
            scoped_intent = {
                "symbol": symbol, "side": "BUY", "strategy_owner": "US_STANDARD",
                "position_action": "NEW_POSITION_BUY",
                "client_order_key": original_order_key,
                "position_lifecycle_id": cycle, "position_cycle_id": cycle,
                "env": "practice", "broker_account_hash": account_hash,
                "trading_epoch_id": trading_epoch_id,
                "portfolio_epoch_id": portfolio_epoch_id,
                "submit_attempt_id": attempt,
            }
            if (not isinstance(timing, dict)
                    or timing.get("submission_binding_version") != 2
                    or not attempt
                    or any(m.get("submit_attempt_id") != attempt
                           for m in (order_meta, fill_meta))
                    or order_meta.get("us_entry_opportunity_v1") != timing
                    or fill_meta.get("us_entry_opportunity_v1") != timing
                    or not _verify_timing_signal_and_binding(scoped_intent, timing)):
                return None
    return {
        "source_buy_client_order_key": original_order_key,
        "source_entry_contract_sha256": contract["sha256"],
        "source_entry_decision_sha256": entry["sha256"],
        "source_entry_decision_id": what["decision_id"],
        "source_entry_lifecycle_id": source_scope,
        "position_cycle_id": cycle,
        "symbol": symbol,
        "strategy_owner": "US_STANDARD",
    }


def attach_original_buy_bound_sell_audit(
    intent: dict, *, contract: dict, binding: dict, decision_at: str,
    code_sha: str,
) -> str:
    """Attach separate signed SELL evidence without changing the BUY or policy.

    Caller supplies the independently verified DB binding, not a position flag.
    This is report-only and does not authorize or block a protective order.
    """
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract
    if not verify_us_entry_exit_contract(contract):
        return "ENTRY_PROOF_MISSING_OR_INVALID"
    provenance = contract.get("entry_provenance") or {}
    entry = provenance.get("entry_decision_evidence_5w1h")
    if not isinstance(entry, dict) or not verify_decision_evidence(entry):
        return "ENTRY_PROOF_MISSING_OR_INVALID"
    meta = intent.setdefault("meta", {})
    cycle = str(intent.get("position_lifecycle_id") or "")
    if (intent.get("side") != "SELL" or not cycle
            or intent.get("symbol") != binding.get("symbol")
            or intent.get("strategy_owner") != "US_STANDARD"
            or binding.get("strategy_owner") != "US_STANDARD"
            or binding.get("position_cycle_id") != cycle
            or binding.get("source_entry_contract_sha256") != contract["sha256"]
            or binding.get("source_entry_decision_sha256") != entry["sha256"]
            or binding.get("source_entry_decision_id") != entry["what"]["decision_id"]
            or binding.get("source_entry_lifecycle_id") != entry["what"]["position_lifecycle_id"]
            or provenance.get("entry_decision_evidence_sha256") != entry["sha256"]
            or not binding.get("source_buy_client_order_key")):
        return "LIFECYCLE_MISMATCH"
    prior = meta.get("exit_decision_evidence_5w1h")
    if (isinstance(prior, dict) and prior.get("original_buy_binding") == binding
            and verify_exit_entry_link(prior, entry, original_buy_binding=binding)
            and prior["what"]["decision_id"] == "US_STANDARD:SELL:" + str(intent.get("client_order_key") or "")):
        meta.update(exit_decision_evidence_status="VERIFIED",
                    source_entry_decision_sha256=entry["sha256"],
                    source_entry_reason=provenance.get("entry_reason"),
                    us_original_buy_decision_binding_v1=dict(binding))
        intent.update(exit_decision_id=prior["what"]["decision_id"],
                      source_entry_decision_sha256=entry["sha256"],
                      source_entry_reason=provenance.get("entry_reason"))
        return "ALREADY_VERIFIED"
    ev = build_exit_evidence_from_entry(
        entry_evidence=entry,
        decision_id="US_STANDARD:SELL:" + str(intent.get("client_order_key") or ""),
        decision_at=decision_at,
        decision_reason=str(intent.get("exit_reason_detail") or intent.get("exit_reason")
                            or intent.get("exit_type") or intent.get("reason") or "UNKNOWN"),
        current_facts={"entry_price": meta.get("entry_price"),
                       "decision_price": meta.get("decision_price"),
                       "exit_type": intent.get("exit_type")},
        changed_since_entry={"pnl_pct": meta.get("return_rate_at_decision")},
        sizing={"qty": intent.get("qty"), "limit_price": intent.get("limit_price"),
                "holding_qty": meta.get("holding_qty")},
        market_context={"market_regime": intent.get("market_regime") or meta.get("market_state")},
        execution_code_sha=code_sha,
    )
    # Only the new SELL record is signed with the real cycle and separate
    # bridge. The original BUY payload and its digest remain byte-for-byte.
    ev["what"]["position_lifecycle_id"] = cycle
    ev["original_buy_binding"] = dict(binding)
    ev["sha256"] = evidence_hash({k: v for k, v in ev.items() if k != "sha256"})
    if not verify_exit_entry_link(ev, entry, original_buy_binding=binding):
        return "LIFECYCLE_MISMATCH"
    meta.update(exit_decision_evidence_5w1h=ev, exit_decision_evidence_status="VERIFIED",
                source_entry_decision_sha256=entry["sha256"],
                source_entry_reason=provenance.get("entry_reason"),
                us_original_buy_decision_binding_v1=dict(binding))
    intent.update(exit_decision_id=ev["what"]["decision_id"],
                  source_entry_decision_sha256=entry["sha256"],
                  source_entry_reason=provenance.get("entry_reason"))
    return "VERIFIED"


def build_exit_evidence_from_entry(
    *, entry_evidence: dict, decision_id: str, decision_at: str,
    decision_reason: str, current_facts: dict, changed_since_entry: dict,
    sizing: dict, market_context: dict, execution_code_sha: str | None = None,
) -> dict:
    """Bind SELL evidence to an exactly verified BUY lifecycle and original digest.

    Do not look up the latest symbol snapshot. The caller must supply the
    position-specific frozen entry record from durable storage.
    """
    if not verify_decision_evidence(entry_evidence):
        raise ValueError("entry_evidence_integrity_invalid")
    what = entry_evidence.get("what") or {}
    who = entry_evidence.get("who") or {}
    if what.get("side") != "BUY":
        raise ValueError("entry_evidence_not_buy")
    return build_decision_evidence(
        decision_id=decision_id, side="SELL",
        strategy_owner=who["strategy_owner"], symbol=what["symbol"],
        decision_at=decision_at, policy_version=who["policy_version"],
        code_sha=execution_code_sha or who["code_sha"],
        position_lifecycle_id=what["position_lifecycle_id"],
        decision_reason=decision_reason,
        facts=current_facts, thresholds={},
        market_context=market_context, sizing=sizing,
        entry_decision_id=what["decision_id"],
        entry_evidence_hash=entry_evidence["sha256"],
        changed_since_entry=changed_since_entry,
    )

def verify_exit_entry_link(exit_evidence: dict, entry_evidence: dict, *,
                           original_buy_binding: dict | None = None) -> bool:
    if not verify_decision_evidence(exit_evidence) or not verify_decision_evidence(entry_evidence):
        return False
    if (exit_evidence.get("what") or {}).get("side") != "SELL":
        return False
    left = exit_evidence["what"]
    right = entry_evidence["what"]
    same_cycle = left["position_lifecycle_id"] == right["position_lifecycle_id"]
    if exit_evidence.get("original_buy_binding") is not None:
        # The receipt inside a SELL is not self-authenticating. A verifier
        # must independently supply the original native BUY binding.
        binding = original_buy_binding
        same_cycle = bool(
            isinstance(binding, dict)
            and exit_evidence["original_buy_binding"] == binding
            and binding.get("source_entry_decision_sha256") == entry_evidence["sha256"]
            and binding.get("source_entry_decision_id") == right["decision_id"]
            and binding.get("source_entry_lifecycle_id") == right["position_lifecycle_id"]
            and binding.get("position_cycle_id") == left["position_lifecycle_id"]
            and binding.get("symbol") == right["symbol"]
            and binding.get("strategy_owner") == entry_evidence["who"]["strategy_owner"]
            and binding.get("source_buy_client_order_key")
            and binding.get("source_entry_contract_sha256")
        )
    return bool(
        exit_evidence["entry_link"] == {
            "decision_id": right["decision_id"],
            "evidence_hash": entry_evidence["sha256"],
        }
        and left["symbol"] == right["symbol"]
        and same_cycle
        and exit_evidence["who"]["strategy_owner"] == entry_evidence["who"]["strategy_owner"]
    )

def summarize_order_evidence(meta: dict, *, side: str) -> dict:
    """Report-only verified evidence. Never turn missing fields into proof.

    SELL additionally requires the exact matched original BUY link when
    available. Legacy orders remain visibly UNVERIFIED, not silently green.
    """
    if not isinstance(meta, dict):
        meta = {}
    side = str(side or "").upper()
    if side not in ALLOWED_SIDES:
        return {"status": "INVALID_SIDE", "evidence_hash": None}
    key = "entry_decision_evidence_5w1h" if side == "BUY" else "exit_decision_evidence_5w1h"
    record = meta.get(key)
    if side == "BUY" and not isinstance(record, dict):
        contract = meta.get("entry_exit_contract") or {}
        if isinstance(contract, dict):
            record = (contract.get("entry_provenance") or {}).get(key)
    if not isinstance(record, dict):
        if side == "SELL":
            from trader.us.sell_decision_observation import summarize_sell_observation
            observation = summarize_sell_observation(meta)
            if observation is not None:
                return observation
        return {"status": "MISSING", "evidence_hash": None}
    if not verify_decision_evidence(record) or (record.get("what") or {}).get("side") != side:
        return {"status": "INVALID", "evidence_hash": None}
    result = {
        "status": "VERIFIED",
        "evidence_hash": record["sha256"],
        "decision_id": record["what"]["decision_id"],
        "strategy_owner": record["who"]["strategy_owner"],
        "decision_reason": record["why"]["decision_reason"],
        "when": record["when"]["decision_at"],
        "market_context": record["where"]["market_context"],
        "facts": record["why"]["facts"],
        "thresholds": record["why"]["thresholds"],
        "changed_since_entry": record["why"]["changed_since_entry"],
        "sizing": record["how"]["sizing"],
        "entry_link": record["entry_link"],
    }
    if side == "BUY":
        declared = meta.get("entry_decision_evidence_sha256")
        if declared and declared != record["sha256"]:
            result["status"] = "INVALID_DECLARED_DIGEST"
    return result

def attach_standard_sell_evidence(
    intent: dict, position: dict, *, decision_at: str, code_sha: str,
) -> str:
    """Attach proof from the *exact* signed position; never authorize a SELL.

    Missing/invalid historical evidence returns a status and does not interfere
    with protected exit routing. No DB queries, HTTP calls, or retries.
    """
    from trader.us.entry_exit_contract import extract_us_entry_exit_contract
    symbol = str(intent.get("symbol") or "").strip().upper()
    if str(intent.get("side") or "").upper() != "SELL" or symbol == "TQQQ":
        return "NOT_APPLICABLE"
    if not isinstance(position, dict) or str(position.get("symbol") or "").upper() != symbol:
        return "POSITION_NOT_MATCHED"
    pos_meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    owner = str(position.get("strategy_owner") or pos_meta.get("strategy_owner") or "US_STANDARD").upper()
    if owner != "US_STANDARD":
        return "OWNER_NOT_STANDARD"
    intent_meta = intent.setdefault("meta", {})
    prior = intent_meta.get("exit_decision_evidence_5w1h")
    contract = extract_us_entry_exit_contract(position, pos_meta)
    provenance = contract.get("entry_provenance") or {}
    entry = provenance.get("entry_decision_evidence_5w1h")
    expected = provenance.get("entry_decision_evidence_sha256")
    if not isinstance(entry, dict) or not verify_decision_evidence(entry) or expected != entry.get("sha256"):
        # Record today's decision even when the historical BUY predates 5W1H.
        # Invalid existing BUY evidence is never replaced by a legacy proof.
        if entry is None and expected is None:
            from trader.us.sell_decision_observation import attach_sell_observation
            if attach_sell_observation(intent, position, decision_at=decision_at, code_sha=code_sha):
                intent_meta["exit_decision_evidence_status"] = "CURRENT_DECISION_VERIFIED_LEGACY_ENTRY"
                return "CURRENT_DECISION_VERIFIED_LEGACY_ENTRY"
        intent_meta["exit_decision_evidence_status"] = "ENTRY_PROOF_MISSING_OR_INVALID"
        return "ENTRY_PROOF_MISSING_OR_INVALID"
    cycle = str(position.get("position_lifecycle_id") or pos_meta.get("position_lifecycle_id") or "")
    intent_cycle = str(intent.get("position_lifecycle_id") or intent_meta.get("position_lifecycle_id") or "")
    if (not cycle or entry["what"]["position_lifecycle_id"] != cycle
            or (intent_cycle and intent_cycle != cycle)):
        intent_meta["exit_decision_evidence_status"] = "LIFECYCLE_MISMATCH"
        return "LIFECYCLE_MISMATCH"
    # A self-consistent signed SELL from a different lifecycle is still wrong.
    if isinstance(prior, dict) and verify_decision_evidence(prior):
        if verify_exit_entry_link(prior, entry):
            return "ALREADY_VERIFIED"
        intent_meta.pop("exit_decision_evidence_5w1h", None)
        intent_meta["exit_decision_evidence_status"] = "REPLACED_STALE_SELL_PROOF"
    reason = str(intent.get("exit_reason_detail") or intent.get("exit_reason") or
                 intent.get("exit_type") or intent.get("reason") or "UNKNOWN")
    ev = build_exit_evidence_from_entry(
        entry_evidence=entry,
        decision_id="US_STANDARD:SELL:" + str(intent.get("client_order_key") or
                                             intent.get("submit_attempt_id") or "UNKNOWN"),
        decision_at=decision_at, decision_reason=reason,
        current_facts={
            "entry_price": intent_meta.get("entry_price"),
            "decision_price": intent_meta.get("decision_price"),
            "exit_type": intent.get("exit_type"),
        },
        changed_since_entry={"pnl_pct": intent_meta.get("return_rate_at_decision")},
        sizing={"qty": intent.get("qty"), "limit_price": intent.get("limit_price"),
                "holding_qty": intent_meta.get("holding_qty")},
        market_context={"market_regime": position.get("market_regime")},
        execution_code_sha=code_sha,
    )
    intent_meta["exit_decision_evidence_5w1h"] = ev
    intent_meta["exit_decision_evidence_status"] = "VERIFIED"
    intent["exit_decision_id"] = ev["what"]["decision_id"]
    intent["source_entry_decision_sha256"] = entry["sha256"]
    return "VERIFIED"
