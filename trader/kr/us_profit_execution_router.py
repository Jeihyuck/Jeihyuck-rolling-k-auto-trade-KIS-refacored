"""KR PB1 practice US-standard TP activation through existing SELL owner.

The result is a policy candidate, never an order permit. Durable stage claim,
semantic SELL dedupe, execution claim, live KIS submit and exact original KIS
fill reconciliation all remain mandatory in pb1_engine / reconcile_kis.
"""
from __future__ import annotations

import hashlib
import math
from typing import Any, Mapping

from trader.account_state import get_account_key
from trader.db.repos import active_trading_epoch_id
from trader.kr.us_profit_stage_journal import load_frozen_stage_states
from trader.kr.us_standard_profit_parity import calculate_next_tp_stage, verified_contract


def _meta(value: Any) -> dict:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        import json
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def frozen_kr_profit_contract(position: Mapping[str, Any]) -> dict | None:
    """Never adopt an unrelated historical contract by symbol-only lookup."""
    for source in ("position_meta", "entry_meta_json"):
        contract = _meta(position.get(source)).get("kr_us_profit_contract")
        if contract is not None:
            return _meta(contract)
    return None


def evaluate_active_kr_tp(
    *, position: Mapping[str, Any], engine: Any, kis: Any, env: str, code: str,
) -> dict | None:
    """Return None only for a position with no ACTIVE frozen parity policy.

    Return BLOCK for any bad lifecycle, missing KIS proof or DB failure.
    A broker error never turns into an empty/clean stage list.
    """
    contract = frozen_kr_profit_contract(position)
    if contract is None or contract.get("activation_mode") != "ACTIVE_PRACTICE":
        return None

    def block(reason: str) -> dict:
        return {"action": "BLOCK", "reason": reason, "contract": contract}

    if not verified_contract(contract):
        return block("KR_TP_INVALID_FROZEN_CONTRACT")
    if env != "practice" or str(getattr(kis, "env", "")).lower() != "practice":
        return block("KR_TP_NOT_PRACTICE")
    owner = str(position.get("owner_strategy") or position.get("strategy") or "").lower()
    if owner not in {"pb1", "kr_standard", "pb1_pullback_close"}:
        return block("KR_TP_OWNER_NOT_PB1")
    if (
        str(position.get("status") or "OPEN").upper() != "OPEN"
        or position.get("lifecycle_blocked")
        or str(position.get("exit_policy_family") or "") != "SWING_STAGED_EXIT"
        or str(position.get("trade_horizon") or "").upper() not in {"SWING", "SWING_CARRY"}
        or str(position.get("policy_source") or "") == "kr_policy_missing_profit_capture_adoption"
        or str(code).zfill(6) != contract.get("code")
        or str(code).zfill(6) == "122630"
    ):
        return block("KR_TP_POLICY_OWNER_OR_CYCLE_CONFLICT")
    cycle = str(position.get("position_cycle_id") or "")
    if not cycle or cycle != contract.get("position_cycle_id"):
        return block("KR_TP_POSITION_CYCLE_UNVERIFIED")
    if position.get("holding_source") != "kis_balance":
        return block("KR_TP_KIS_HOLDING_SOURCE_UNVERIFIED")
    try:
        qty = position.get("kis_qty")
        orderable = position.get("orderable_qty")
        if isinstance(qty, bool) or isinstance(orderable, bool):
            return block("KR_TP_QUANTITY_INVALID")
        if not (
            (isinstance(qty, int) or isinstance(qty, str) and qty.isascii() and qty.isdecimal())
            and (isinstance(orderable, int) or isinstance(orderable, str) and orderable.isascii() and orderable.isdecimal())
        ):
            return block("KR_TP_QUANTITY_INVALID")
        qty, orderable = int(qty), int(orderable)
        if qty <= 0 or orderable <= 0 or orderable > qty or qty != int(position.get("qty") or -1):
            return block("KR_TP_KIS_DB_QTY_MISMATCH")
        account = get_account_key(env=env, kis=kis)
        if hashlib.sha256(str(account).encode("utf-8")).hexdigest() != contract["account_sha256"]:
            return block("KR_TP_ACCOUNT_MISMATCH")
        epoch = active_trading_epoch_id(engine, env=env, account_id=account, required=True)
        if str(epoch or "") != contract.get("trading_epoch_id"):
            return block("KR_TP_TRADING_EPOCH_MISMATCH")
        # Fresh native KIS quote. No OHLCV, PREP close or cached synthetic price.
        snapshot = kis.get_price_snapshot(code, market="J")
        if not isinstance(snapshot, dict):
            return block("KR_TP_FRESH_QUOTE_UNAVAILABLE")
        raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
        px = snapshot.get("bid") or snapshot.get("bidp") or snapshot.get("bidp1")
        px = px or snapshot.get("stck_prpr") or snapshot.get("prpr") or snapshot.get("last")
        px = px or raw.get("bidp1") or raw.get("stck_prpr")
        price = float(px or 0)
        basis = float(position.get("avg_buy_price") or 0)
        if not math.isfinite(price) or price <= 0 or not math.isfinite(basis) or basis <= 0:
            return block("KR_TP_PRICE_OR_BASIS_UNVERIFIED")
        states = load_frozen_stage_states(
            engine=engine, contract=contract, account_id=str(account),
            trading_epoch_id=str(epoch), position_cycle_id=cycle, code=str(code).zfill(6),
        )
        d = calculate_next_tp_stage(
            contract=contract, stage_states=states,
            holding_qty=qty, orderable_qty=orderable,
            verified_entry_price=basis, executable_price=price,
        )
        return {
            "action": d.action, "reason": d.reason, "stage": d.stage,
            "qty": d.target_qty, "contract": contract,
            "account_id": str(account), "epoch_id": str(epoch),
            "position_cycle_id": cycle, "code": str(code).zfill(6),
            "fresh_bid_or_last": price, "verified_entry_price": basis,
        }
    except Exception as exc:
        return block("KR_TP_EVIDENCE_UNAVAILABLE:" + type(exc).__name__)
