"""Immutable, audit-only US decision evidence. Never authorizes or blocks broker orders."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any

VERSION = "us_decision_evidence_5w1h_v1"
ALLOWED_SIDES = {"BUY", "SELL"}

def _clean(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str, int)):
        return value
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

def build_exit_evidence_from_entry(
    *, entry_evidence: dict, decision_id: str, decision_at: str,
    decision_reason: str, current_facts: dict, changed_since_entry: dict,
    sizing: dict, market_context: dict,
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
        code_sha=who["code_sha"],
        position_lifecycle_id=what["position_lifecycle_id"],
        decision_reason=decision_reason,
        facts=current_facts, thresholds={},
        market_context=market_context, sizing=sizing,
        entry_decision_id=what["decision_id"],
        entry_evidence_hash=entry_evidence["sha256"],
        changed_since_entry=changed_since_entry,
    )

def verify_exit_entry_link(exit_evidence: dict, entry_evidence: dict) -> bool:
    if not verify_decision_evidence(exit_evidence) or not verify_decision_evidence(entry_evidence):
        return False
    if (exit_evidence.get("what") or {}).get("side") != "SELL":
        return False
    left = exit_evidence["what"]
    right = entry_evidence["what"]
    return bool(
        exit_evidence["entry_link"] == {
            "decision_id": right["decision_id"],
            "evidence_hash": entry_evidence["sha256"],
        }
        and left["symbol"] == right["symbol"]
        and left["position_lifecycle_id"] == right["position_lifecycle_id"]
        and exit_evidence["who"]["strategy_owner"] == entry_evidence["who"]["strategy_owner"]
    )
