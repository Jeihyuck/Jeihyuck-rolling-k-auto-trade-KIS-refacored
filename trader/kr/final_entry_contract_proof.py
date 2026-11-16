"""Immutable KR PB1 final entry contract provenance, separate from setup signal.

ENTRY_MOMENTUM may legitimately become ENTRY_MOMENTUM_SWING for a newly
approved paper entry. Original evidence documents the signal; the final
plan owns the eventual exit. Never rewrite already-held position contracts.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from trader.trade_plan import ENTRY_STYLE_ALIAS, STYLE_PLAN_MAPPING, parse_plan_bool

VERSION = "kr_pb1_final_entry_contract_proof_v1"
CONTRACT_FIELDS = (
    "code", "entry_style_selected", "trade_horizon",
    "exit_policy_family", "eod_action", "force_eod_close",
    "policy_version",
)


def _canonical(data: Mapping[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str)


def build_final_entry_contract_proof(
    *, code: str, owner: str, plan: Mapping[str, Any],
    original_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Sign an already selected plan, NOT an earlier setup style."""
    if str(owner or "").upper() != "PB1":
        raise ValueError("KR_FINAL_CONTRACT_OWNER_NOT_PB1")
    final_style = str(plan.get("entry_style_selected") or "").upper()
    style = ENTRY_STYLE_ALIAS.get(final_style, final_style)
    mapped = STYLE_PLAN_MAPPING.get(style)
    if mapped is None:
        raise ValueError("KR_FINAL_CONTRACT_STYLE_UNKNOWN")
    code_norm = str(code or "").removeprefix("A").zfill(6)
    if not code_norm.isdigit() or str(plan.get("code") or "").zfill(6) != code_norm:
        raise ValueError("KR_FINAL_CONTRACT_CODE_MISMATCH")
    for key in ("trade_horizon", "exit_policy_family", "eod_action"):
        if str(plan.get(key) or "").upper() != str(mapped[key]).upper():
            raise ValueError("KR_FINAL_CONTRACT_POLICY_MISMATCH:" + key)
    if parse_plan_bool(plan.get("force_eod_close")) != bool(mapped["force_eod_close"]):
        raise ValueError("KR_FINAL_CONTRACT_POLICY_MISMATCH:force_eod_close")
    if not str(plan.get("policy_version") or ""):
        raise ValueError("KR_FINAL_CONTRACT_POLICY_VERSION_MISSING")
    original = dict(original_identity or {})
    # Retain the historical signal; it is intentionally not the exit owner.
    contract = {
        "version": VERSION, "strategy_owner": "PB1",
        "original_selected_style": str(original.get("selected_style") or ""),
        "original_trade_horizon": str(original.get("contract_trade_horizon") or ""),
        "final_style": style,
        "code": code_norm, "trade_horizon": plan["trade_horizon"],
        "exit_policy_family": plan["exit_policy_family"],
        "eod_action": plan["eod_action"],
        "force_eod_close": bool(mapped["force_eod_close"]),
        "policy_version": plan["policy_version"],
        "plan_sha256": hashlib.sha256(_canonical(plan).encode("utf-8")).hexdigest(),
    }
    contract["sha256"] = hashlib.sha256(
        _canonical(contract).encode("utf-8")
    ).hexdigest()
    return contract


def verify_final_entry_contract_proof(
    *, plan: Mapping[str, Any], proof: Mapping[str, Any],
) -> bool:
    try:
        expected = build_final_entry_contract_proof(
            code=str(proof["code"]), owner=str(proof["strategy_owner"]),
            plan=plan, original_identity={
                "selected_style": proof.get("original_selected_style"),
                "contract_trade_horizon": proof.get("original_trade_horizon"),
            },
        )
        return dict(proof) == expected
    except (KeyError, TypeError, ValueError):
        return False
