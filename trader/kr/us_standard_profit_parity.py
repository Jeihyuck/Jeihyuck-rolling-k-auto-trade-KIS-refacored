"""KR PB1 new-SWING US_STANDARD TP parity, pure shadow-safe policy.

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
            or data.get("runner_version") not in VALID_RUNNER_VERSIONS
            or data.get("stages") != [dict(stage=s[0], threshold_fraction=s[1],
                                           sell_fraction=s[2], reason=s[3]) for s in STAGES]):
        return False
    return bool(old_sha) and old_sha == _canonical_sha(data)


def _mode() -> str:
    value = str(os.getenv("KR_SWING_US_PROFIT_MODE", "LEGACY")).strip().upper()
    if value not in {"LEGACY", "SHADOW", "ACTIVE_PRACTICE"}:
        raise ValueError("KR_SWING_PROFIT_MODE_INVALID")
    return value


def build_new_swing_parity_contract(
    *,
    strategy_owner: str, exit_policy_family: str, trade_horizon: str,
    position_action: str, account_id: str, trading_epoch_id: str,
    position_cycle_id: str, runner_version: str,
    existing_contract: Mapping[str, Any] | None = None,
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
        or str(position_action).upper() != "NEW_POSITION_BUY"
    ):
        raise ValueError("KR_PROFIT_PARITY_OWNER_OR_HORIZON_UNVERIFIED")
    if not all(str(x or "").strip() for x in
               (account_id, trading_epoch_id, position_cycle_id)):
        raise ValueError("KR_PROFIT_PARITY_LIFECYCLE_UNVERIFIED")
    if runner_version not in VALID_RUNNER_VERSIONS:
        raise ValueError("KR_PROFIT_PARITY_RUNNER_VERSION_UNVERIFIED")
    if mode == "ACTIVE_PRACTICE" and (
        os.getenv("KR_SWING_US_PROFIT_DEPLOY_APPROVED", "0") != "1"
        or os.getenv("KIS_ENV", "").lower() != "practice"
    ):
        raise ValueError("KR_PROFIT_PARITY_DEPLOY_NOT_APPROVED")
    payload: dict[str, Any] = {
        "version": POLICY_VERSION,
        "strategy_owner": "PB1",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "trade_horizon": "SWING",
        "position_action": "NEW_POSITION_BUY",
        "account_sha256": hashlib.sha256(account_id.encode("utf-8")).hexdigest(),
        "trading_epoch_id": trading_epoch_id,
        "position_cycle_id": position_cycle_id,
        "runner_version": runner_version,
        "runner_min_remain_pct": RUNNER_MIN_REMAIN_PCT,
        "stages": [dict(stage=s[0], threshold_fraction=s[1],
                        sell_fraction=s[2], reason=s[3]) for s in STAGES],
    }
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
    try:
        if any(isinstance(x, bool) or int(x) != x for x in (holding_qty, orderable_qty)):
            raise ValueError("fractional qty")
        q, orderable = int(holding_qty), int(orderable_qty)
        basis, px = float(verified_entry_price), float(executable_price)
        if q < 0 or orderable < 0 or orderable > q:
            raise ValueError("qty out of range")
        if not all(math.isfinite(x) and x > 0 for x in (basis, px)):
            raise ValueError("price invalid")
    except (ValueError, TypeError, OverflowError):
        return StageDecision("BLOCK", reason="QUANTITY_OR_PRICE_NOT_VERIFIED")
    if unresolved_sell:
        return StageDecision("BLOCK", reason="UNRESOLVED_SELL_OR_ACK")
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
        if status not in {"NOT_TRIGGERED", "REJECTED", "CANCELLED"}:
            return StageDecision("BLOCK", name, reason="STAGE_STATE_UNKNOWN")
        if gain + 1e-12 < threshold:
            return StageDecision("HOLD", name, reason="TP_THRESHOLD_NOT_MET")
        # Do not reuse yesterday's target qty across an unproven new lifecycle.
        target = state.get("target_qty") or 0
        filled = state.get("filled_qty") or 0
        try:
            quantity = resolve_profit_capture_stage_quantity(
                holding_qty=q, sell_fraction=frac,
                runner_min_remain_pct=RUNNER_MIN_REMAIN_PCT,
                stage_target_qty=int(target), stage_filled_qty=int(filled),
            )
        except (TypeError, ValueError, OverflowError):
            return StageDecision("BLOCK", name, reason="STAGE_QUANTITY_INVALID")
        quantity = min(quantity, orderable)
        if quantity <= 0:
            return StageDecision("HOLD", name, reason="NOT_ENOUGH_ORDERABLE_OR_RUNNER")
        return StageDecision("SHADOW_TP", name, quantity,
                             reason="REQUIRES_KR_DURABLE_BROKER_STAGE_GATE",
                             order_authorized=False)
    return StageDecision("RUNNER", reason="ALL_TP_STAGES_PROVEN_FILLED")
