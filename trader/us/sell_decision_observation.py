"""Current SELL observations, separate from unavailable historical BUY decisions.

A valid original contract is a source reference, never a fabricated BUY 5W1H.
This module only records observations; it cannot authorize a broker submission.
"""
from __future__ import annotations

from trader.us.decision_evidence_5w1h import _clean, evidence_hash
from trader.us.entry_exit_contract import extract_us_entry_exit_contract, verify_us_entry_exit_contract

VERSION = "us_sell_decision_observation_v1"


def attach_sell_observation(intent: dict, position: dict, *, decision_at: str, code_sha: str) -> bool:
    symbol = str(intent.get("symbol") or "").upper()
    pm = position.get("meta") or {}
    owner = str(position.get("strategy_owner") or pm.get("strategy_owner") or "US_STANDARD").upper()
    if (intent.get("side") != "SELL" or not symbol or symbol == "TQQQ"
            or owner != "US_STANDARD" or str(position.get("symbol") or "").upper() != symbol):
        return False
    meta = intent.setdefault("meta", {})
    cycle = str(position.get("position_lifecycle_id") or pm.get("position_lifecycle_id") or "")
    intent_cycle = str(intent.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or "")
    if not cycle or (intent_cycle and intent_cycle != cycle):
        return False
    contract = extract_us_entry_exit_contract(position, pm)
    digest = contract.get("sha256")
    declared = (intent.get("source_entry_contract_sha256") or meta.get("source_entry_contract_sha256"))
    if not verify_us_entry_exit_contract(contract) or (declared and declared != digest):
        return False
    record = _clean({
        "version": VERSION,
        "who": {"strategy_owner": owner, "code_sha": code_sha,
                "entry_strategy": contract.get("entry_strategy")},
        "what": {"decision_id": "US_STANDARD:SELL:" + str(intent.get("client_order_key") or ""),
                 "symbol": symbol, "side": "SELL", "position_lifecycle_id": cycle},
        "when": {"decision_at": decision_at},
        "where": {"market_context": {"market_regime": position.get("market_regime"),
                                      "market_state": meta.get("market_state")}},
        "why": {"decision_reason": intent.get("exit_reason_detail") or intent.get("exit_reason")
                                   or intent.get("exit_type") or intent.get("reason"),
                "facts": {k: meta.get(k) for k in (
                    "entry_price", "broker_avg_price", "decision_price", "return_rate_at_decision",
                    "profit_capture_stage", "tp_threshold_fraction", "pre_order_holding_qty")},
                "thresholds": contract.get("management") or {}},
        "how": {"sizing": {"qty": intent.get("qty"), "limit_price": intent.get("limit_price")}},
        "entry_link": {"kind": "ORIGINAL_ENTRY_CONTRACT", "sha256": digest,
                       "position_lifecycle_id": cycle,
                       "historical_buy_decision_status": "NOT_AVAILABLE_IN_ORIGINAL_CONTRACT"},
    })
    if not record["why"]["decision_reason"] or not record["when"]["decision_at"]:
        return False
    record["sha256"] = evidence_hash(record)
    meta["exit_decision_observation_5w1h"] = record
    # Preserve the original contract verbatim for independent audit verification.
    meta["exit_observation_source_contract"] = contract
    return True


def summarize_sell_observation(meta: dict) -> dict | None:
    record = meta.get("exit_decision_observation_5w1h")
    if not isinstance(record, dict):
        return None
    contract = meta.get("exit_observation_source_contract")
    try:
        link = record["entry_link"]
        if (record.get("version") != VERSION
                or record.get("sha256") != evidence_hash({k: v for k, v in record.items() if k != "sha256"})
                or not verify_us_entry_exit_contract(contract)
                or link.get("sha256") != contract.get("sha256")
                or link.get("kind") != "ORIGINAL_ENTRY_CONTRACT"
                or record["who"].get("strategy_owner") != "US_STANDARD"
                or record["what"].get("side") != "SELL"
                or record["what"].get("symbol") == "TQQQ"
                or not record["what"].get("position_lifecycle_id")
                or link.get("position_lifecycle_id") != record["what"].get("position_lifecycle_id")):
            return {"status": "INVALID", "evidence_hash": None}
        return {"status": "CURRENT_DECISION_VERIFIED_LEGACY_ENTRY",
                "evidence_hash": record["sha256"], "decision_id": record["what"]["decision_id"],
                "strategy_owner": record["who"]["strategy_owner"],
                "decision_reason": record["why"]["decision_reason"],
                "when": record["when"]["decision_at"],
                "market_context": record["where"]["market_context"],
                "facts": record["why"]["facts"], "thresholds": record["why"]["thresholds"],
                "sizing": record["how"]["sizing"], "entry_link": link}
    except (KeyError, TypeError, ValueError):
        return {"status": "INVALID", "evidence_hash": None}
