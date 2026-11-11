"""KR PB1 new-SWING US_STANDARD TP parity, immutable practice policy.

No broker I/O and no DB stage writes. The execution router MUST NOT submit
an intent from this module without a separately approved, durable KR stage
claim + broker recheck path. Existing v1 and non-PB1 contracts stay frozen.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from typing import Any, Mapping

from trader.us.market_state_overlay import resolve_profit_capture_stage_quantity

POLICY_VERSION = "kr_pb1_us_standard_profit_parity_v2"
STAGES = (
    ("TP1", 0.03, 0.25, "TAKE_PROFIT_TP1"),
    ("TP2", 0.05, 0.25, "TAKE_PROFIT_TP2"),
    ("TP3", 0.08, 0.20, "TAKE_PROFIT_TP3"),
)
RUNNER_MIN_REMAIN_PCT = 0.40
VALID_RUNNER_VERSIONS = frozenset({"US_V2", "US_V3"})
BLOCKING_STATES = frozenset({
    "PENDING", "INTENT", "CLAIMED", "SUBMITTED", "ACK", "ACKED",
    "ACCEPTED", "PARTIAL_FILLED", "PARTIALLY_FILLED", "UNRESOLVED",
    "UNRESOLVED_ACK", "RECONCILE_PENDING",
})


def _canonical_sha(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")).hexdigest()


def verified_contract(value: Mapping[str, Any] | None) -> bool:
    if not isinstance(value, Mapping):
        return False
    data = dict(value)
    old_sha = data.pop("sha256", None)
    if (data.get("version") != POLICY_VERSION
            or data.get("strategy_owner") != "PB1"
            or data.get("exit_policy_family") != "SWING_STAGED_EXIT"
            or data.get("trade_horizon") != "SWING"
            or data.get("position_action") not in {"NEW_POSITION_BUY", "EXPLICIT_POSITION_AMENDMENT"}
            or data.get("activation_mode", "SHADOW") not in {"SHADOW", "ACTIVE_PRACTICE"}
            or not str(data.get("account_sha256") or "").strip()
            or not str(data.get("trading_epoch_id") or "").strip()
            or not str(data.get("position_cycle_id") or "").strip()
            or not (str(data.get("code") or "").isdigit()
                    and len(str(data.get("code") or "")) == 6)
            or data.get("runner_version") not in VALID_RUNNER_VERSIONS
            or data.get("stages") != [dict(stage=s[0], threshold_fraction=s[1],
                                           sell_fraction=s[2], reason=s[3]) for s in STAGES]):
        return False
    proof = data.get("amendment_proof")
    if data.get("position_action") == "EXPLICIT_POSITION_AMENDMENT":
        if (
            not isinstance(proof, Mapping)
            or proof.get("version") != "kr_pb1_position_amendment_v1"
            or proof.get("source") != "KIS_DAILY_CCLD_AND_FRESH_BALANCE"
            or not str(proof.get("original_buy_odno") or "").strip()
            or not str(proof.get("original_buy_trade_date") or "").strip()
            or not str(proof.get("original_entry_contract_sha256") or "").strip()
            or not isinstance(proof.get("verified_qty"), int)
            or isinstance(proof.get("verified_qty"), bool)
            or proof.get("verified_qty", 0) <= 0
            or not isinstance(proof.get("verified_avg_price"), (int, float))
            or float(proof.get("verified_avg_price") or 0) <= 0
        ):
            return False
    elif proof is not None:
        return False
    return bool(old_sha) and old_sha == _canonical_sha(data)


def _mode() -> str:
    # Approved KR practice policy is ON by default; real accounts stay LEGACY.
    default = "ACTIVE_PRACTICE" if os.getenv("KIS_ENV", "").lower() == "practice" else "LEGACY"
    value = str(os.getenv("KR_SWING_US_PROFIT_MODE", default)).strip().upper()
    if value not in {"LEGACY", "SHADOW", "ACTIVE_PRACTICE"}:
        raise ValueError("KR_SWING_PROFIT_MODE_INVALID")
    return value


def build_new_swing_parity_contract(
    *,
    strategy_owner: str, exit_policy_family: str, trade_horizon: str,
    position_action: str, account_id: str, trading_epoch_id: str,
    position_cycle_id: str, runner_version: str, code: str,
    existing_contract: Mapping[str, Any] | None = None,
    amendment_proof: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Only a positively identified NEW PB1 SWING can receive a new contract.

    Returning None for LEGACY preserves all previous BUY-time v1 contracts.
    SHADOW returns an independently signed *candidate*, never an order permit.
    ACTIVE_PRACTICE still requires a separate, explicit env and broker gate.
    """
    mode = _mode()
    if mode == "LEGACY":
        return None
    if existing_contract:
        raise ValueError("KR_FROZEN_CONTRACT_ALREADY_EXISTS")
    if (
        str(strategy_owner).upper() != "PB1"
        or str(exit_policy_family).upper() != "SWING_STAGED_EXIT"
        or str(trade_horizon).upper() != "SWING"
        or str(position_action).upper() not in {"NEW_POSITION_BUY", "EXPLICIT_POSITION_AMENDMENT"}
    ):
        raise ValueError("KR_PROFIT_PARITY_OWNER_OR_HORIZON_UNVERIFIED")
    if str(position_action).upper() == "EXPLICIT_POSITION_AMENDMENT":
        if mode != "ACTIVE_PRACTICE" or not isinstance(amendment_proof, Mapping):
            raise ValueError("KR_PROFIT_PARITY_AMENDMENT_EVIDENCE_REQUIRED")
    elif amendment_proof is not None:
        raise ValueError("KR_PROFIT_PARITY_NEW_BUY_MUST_NOT_INHERIT_AMENDMENT")
    if not all(str(x or "").strip() for x in
               (account_id, trading_epoch_id, position_cycle_id)):
        raise ValueError("KR_PROFIT_PARITY_LIFECYCLE_UNVERIFIED")
    if runner_version not in VALID_RUNNER_VERSIONS:
        raise ValueError("KR_PROFIT_PARITY_RUNNER_VERSION_UNVERIFIED")
    if not (str(code).isdigit() and len(str(code)) == 6):
        raise ValueError("KR_PROFIT_PARITY_SYMBOL_UNVERIFIED")
    if mode == "ACTIVE_PRACTICE" and os.getenv("KIS_ENV", "").lower() != "practice":
        raise ValueError("KR_PROFIT_PARITY_DEPLOY_NOT_APPROVED")
    # A signed contract is NOT broker permission. The live PB1 exit owner
    # must pass the existing execution claim and a durable TP stage journal.
    payload: dict[str, Any] = {
        "version": POLICY_VERSION,
        "strategy_owner": "PB1",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "trade_horizon": "SWING",
        "position_action": str(position_action).upper(),
        "activation_mode": mode,
        "account_sha256": hashlib.sha256(account_id.encode("utf-8")).hexdigest(),
        "trading_epoch_id": trading_epoch_id,
        "position_cycle_id": position_cycle_id,
        "code": str(code),
        "runner_version": runner_version,
        "runner_min_remain_pct": RUNNER_MIN_REMAIN_PCT,
        "stages": [dict(stage=s[0], threshold_fraction=s[1],
                        sell_fraction=s[2], reason=s[3]) for s in STAGES],
    }
    if amendment_proof is not None:
        payload["amendment_proof"] = dict(amendment_proof)
    if runner_version == "US_V3":
        # Snapshot the approved US V3 policy, not today's ENV at sell time.
        from trader.us.pb1.swing_runner_exit_v3 import V3_RUNNER_POLICY
        payload["runner_policy"] = dict(V3_RUNNER_POLICY)
    else:
        # V2 must use its own separately verified source policy at execution.
        payload["runner_policy"] = {"version": "US_V2", "execution_approved": False}
    payload["sha256"] = _canonical_sha(payload)
    return payload


@dataclass(frozen=True)
class StageDecision:
    action: str
    stage: str | None = None
    target_qty: int = 0
    reason: str = ""
    order_authorized: bool = False


def calculate_next_tp_stage(
    *,
    contract: Mapping[str, Any], stage_states: Mapping[str, Mapping[str, Any]],
    holding_qty: int, orderable_qty: int,
    verified_entry_price: float, executable_price: float,
    unresolved_sell: bool = False,
) -> StageDecision:
    """Calculate next TP candidate, *never* authorize placing a broker order.

    US resolver defines integer rounding and per-step remaining-holding floor.
    Durable stage state, real broker provenance and pre-submit claims remain
    mandatory responsibilities of the KR execution router.
    """
    if not verified_contract(contract):
        return StageDecision("BLOCK", reason="FROZEN_CONTRACT_INVALID")
    def _exact_qty(value: Any) -> int:
        # Match US V3 strict broker qty semantics: canonical nonnegative int
        # or ASCII decimal string, never a rounded float/bool/"2.0".
        if isinstance(value, bool):
            raise ValueError("boolean quantity")
        if isinstance(value, int) and value >= 0:
            return value
        if isinstance(value, str) and value.isascii() and value.isdecimal():
            return int(value)
        raise ValueError("quantity evidence not integral")

    try:
        q, orderable = _exact_qty(holding_qty), _exact_qty(orderable_qty)
        basis, px = float(verified_entry_price), float(executable_price)
        if q < 0 or orderable < 0 or orderable > q:
            raise ValueError("qty out of range")
        if not all(math.isfinite(x) and x > 0 for x in (basis, px)):
            raise ValueError("price invalid")
    except (ValueError, TypeError, OverflowError):
        return StageDecision("BLOCK", reason="QUANTITY_OR_PRICE_NOT_VERIFIED")
    if unresolved_sell:
        return StageDecision("BLOCK", reason="UNRESOLVED_SELL_OR_ACK")
    if not isinstance(stage_states, Mapping):
        return StageDecision("BLOCK", reason="STAGE_STORAGE_UNAVAILABLE")
    gain = (px / basis) - 1.0
    for name, threshold, frac, _ in STAGES:
        state = dict(stage_states.get(name) or {})
        status = str(state.get("status") or "NOT_TRIGGERED").upper()
        if status == "FILLED":
            # A flag alone cannot prove broker fills: need an exact stage match.
            if state.get("broker_fill_proven") is not True:
                return StageDecision("BLOCK", name, reason="STAGE_FILL_PROOF_MISSING")
            continue
        if status in BLOCKING_STATES:
            return StageDecision("BLOCK", name, reason="PREVIOUS_STAGE_NOT_FILLED")
        if status in {"REJECTED", "CANCELLED"}:
            # A rejected/cancelled broker ACK is not proof that a previously
            # accepted or partially-filled TP is absent. Require a separate
            # exact broker cancellation + cumulative fill observation first.
            return StageDecision("BLOCK", name, reason="RETRY_REQUIRES_BROKER_PROOF")
        if status != "NOT_TRIGGERED":
            return StageDecision("BLOCK", name, reason="STAGE_STATE_UNKNOWN")
        if gain + 1e-12 < threshold:
            return StageDecision("HOLD", name, reason="TP_THRESHOLD_NOT_MET")
        # Do not reuse yesterday's target qty across an unproven new lifecycle.
        target = state.get("target_qty") or 0
        filled = state.get("filled_qty") or 0
        try:
            target_qty = _exact_qty(target)
            filled_qty = _exact_qty(filled)
            if filled_qty > target_qty and target_qty > 0:
                raise ValueError("stage filled > target")
            quantity = resolve_profit_capture_stage_quantity(
                holding_qty=q, sell_fraction=frac,
                runner_min_remain_pct=RUNNER_MIN_REMAIN_PCT,
                stage_target_qty=target_qty, stage_filled_qty=filled_qty,
            )
        except (TypeError, ValueError, OverflowError):
            return StageDecision("BLOCK", name, reason="STAGE_QUANTITY_INVALID")
        quantity = min(quantity, orderable)
        if quantity <= 0:
            return StageDecision("HOLD", name, reason="NOT_ENOUGH_ORDERABLE_OR_RUNNER")
        active = contract.get("activation_mode") == "ACTIVE_PRACTICE"
        return StageDecision("ACTIVE_TP" if active else "SHADOW_TP", name, quantity,
                             reason="REQUIRES_KR_DURABLE_BROKER_STAGE_GATE",
                             order_authorized=False)
    return StageDecision("RUNNER", reason="ALL_TP_STAGES_PROVEN_FILLED")


def evaluate_v3_runner_shadow(
    *,
    contract: Mapping[str, Any],
    stage_states: Mapping[str, Mapping[str, Any]],
    verified_entry_price: float,
    verified_high: float,
    executable_price: float,
    holding_qty: int,
    orderable_qty: int,
    high_verified: bool,
    trend_state: str,
    trend_verified: bool,
    atr_evidence: Mapping[str, Any] | None,
    partial_stage_status: str,
    final_stage_status: str,
    unresolved_sell: bool = False,
) -> dict[str, Any]:
    """KR shadow uses the *exact* US V3 pure policy, not a reimplementation.

    Only a trusted, account/epoch/cycle-frozen contract and complete TP fills
    may reach Runner. This function cannot create a KR broker order or advance
    a durable stage.
    """
    if not verified_contract(contract):
        return {"action": "BLOCK", "reason": "FROZEN_CONTRACT_INVALID",
                "order_authorized": False}
    if contract.get("runner_version") != "US_V3":
        return {"action": "BLOCK", "reason": "V2_RUNNER_REQUIRES_VERIFIED_POLICY",
                "order_authorized": False}
    if unresolved_sell:
        return {"action": "BLOCK", "reason": "UNRESOLVED_SELL_OR_ACK",
                "order_authorized": False}
    for stage, *_ in STAGES:
        evidence = dict(stage_states.get(stage) or {})
        if evidence.get("status") != "FILLED" or evidence.get("broker_fill_proven") is not True:
            return {"action": "BLOCK", "reason": "TP_STAGES_NOT_BROKER_PROVEN",
                    "stage": stage, "order_authorized": False}
    from trader.us.pb1.swing_runner_exit_v3 import evaluate_v3_runner
    raw = evaluate_v3_runner(
        entry_price=verified_entry_price,
        verified_high=verified_high,
        current_price=executable_price,
        holding_qty=holding_qty,
        orderable_qty=orderable_qty,
        trend_state=trend_state, trend_verified=trend_verified,
        high_verified=high_verified,
        atr_evidence=dict(atr_evidence or {}),
        partial_stage_status=partial_stage_status,
        final_stage_status=final_stage_status,
        policy=dict(contract.get("runner_policy") or {}),
    )
    # A proposal from US policy is never the permission to submit in KR.
    out = dict(raw)
    out["source_policy_action"] = str(raw.get("action") or "")
    out["action"] = "SHADOW_RUNNER" if raw.get("action") == "SELL" else "HOLD"
    out["order_authorized"] = False
    return out
