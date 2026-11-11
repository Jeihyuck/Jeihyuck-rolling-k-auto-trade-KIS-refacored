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


def active_tp_durable_intent_permitted(
    *, dry_run: bool, intended_live: bool, force_block_live: bool,
) -> bool:
    """No diagnostic/disabled PB1 TP SELL may reserve real DB inventory.

    Decision evaluation is allowed in dry-run; writing a CREATED/ACK-less
    order is not: it would block the next live cross-owner SELL reconciliation.
    """
    return not (dry_run or not intended_live or force_block_live)


def frozen_kr_profit_contract(position: Mapping[str, Any]) -> dict | None:
    """Never adopt an unrelated historical contract by symbol-only lookup."""
    for source in ("position_meta", "entry_meta_json"):
        contract = _meta(position.get(source)).get("kr_us_profit_contract")
        if contract is not None:
            return _meta(contract)
    return None


_PROVEN_PRE_HTTP_ERROR_CODES = frozenset({
    "EXECUTION_CLAIM_UNAVAILABLE",
    "EXECUTION_ACTION_ALREADY_CLAIMED",
    "KR_US_TP_STAGE_NOT_DURABLE",
    "KR_US_TP_PRETRADE_DENIED",
})


def _unresolved_sell_row(row: Mapping[str, Any]) -> bool:
    """A positive, broker-free pre-HTTP failure does not reserve KIS shares.

    ERROR alone is NOT proof of no submission. Missing response, ambiguous
    HTTP exception, broker number or submit timestamp remains unresolved.
    """
    status = str(row.get("status") or "").strip().upper()
    if status in {"FILLED", "RECONCILED", "CANCELLED", "REJECTED"}:
        return False
    response = _meta(row.get("response_json"))
    if (
        status == "ERROR"
        and response.get("broker_submit") is False
        and str(response.get("rt_cd") or "") in _PROVEN_PRE_HTTP_ERROR_CODES
        and not any(row.get(k) for k in (
            "kis_odno", "broker_order_id", "submitted_at", "acked_at"
        ))
    ):
        return False
    return True


def has_unresolved_kr_sell_orders(*, engine: Any, env: str, code: str,
                                  trading_epoch_id: str) -> bool:
    """Cross-owner SELL/ACK fence on the exact trading epoch and symbol.

    Only explicitly proven pre-KIS, error-status local claims can be ignored.
    Any unknown DB/KIS row, ambiguous submit, ACK or partial fill fails closed.
    """
    import sqlalchemy as sa
    from trader.db.schema import schema_for_engine

    orders = schema_for_engine(engine).orders
    with engine.connect() as conn:
        rows = conn.execute(sa.select(
            orders.c.status, orders.c.response_json,
            orders.c.kis_odno, orders.c.broker_order_id,
            orders.c.submitted_at, orders.c.acked_at,
        ).where(
            sa.and_(
                orders.c.env == env,
                orders.c.trading_epoch_id == trading_epoch_id,
                orders.c.code == str(code).zfill(6),
                sa.func.upper(orders.c.side).like("SELL%"),
                sa.or_(
                    orders.c.status.is_(None),
                    sa.func.upper(orders.c.status).not_in(
                        ["FILLED", "RECONCILED", "CANCELLED", "REJECTED"]
                    ),
                ),
            )
        )).mappings().all()
    return any(_unresolved_sell_row(row) for row in rows)


def evaluate_active_kr_tp(
    *, position: Mapping[str, Any], engine: Any, kis: Any, env: str, code: str,
    balance_snapshot: Any,
) -> dict | None:
    """Return None only for a position with no ACTIVE frozen parity policy.

    Return BLOCK for any bad lifecycle, missing KIS proof or DB failure.
    A broker error never turns into an empty/clean stage list.
    """
    contract = frozen_kr_profit_contract(position)
    if contract is None and str(code).zfill(6) == "028050" and env == "practice":
        # One explicitly scoped existing v1 holding: broker-proof amendment
        # may attach a NEW versioned overlay. A failure preserves v1 exactly
        # and does NOT claim that a new TP strategy is active.
        try:
            from trader.kr.us_profit_existing_amendment import (
                maybe_amend_samsung_ea_position,
            )
            amended = maybe_amend_samsung_ea_position(
                engine=engine, kis=kis, env=env,
                position_cycle_id=str(position.get("position_cycle_id") or ""),
            )
            if amended:
                contract = amended
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning(
                "[KR_PROFIT_PARITY][SAMSUNG_AMENDMENT_NOT_VERIFIED] code=%s reason=%s",
                code, type(exc).__name__,
            )
    if contract is None or contract.get("activation_mode") != "ACTIVE_PRACTICE":
        return None

    def block(reason: str) -> dict:
        return {"action": "BLOCK", "reason": reason, "contract": contract}

    if not verified_contract(contract):
        return block("KR_TP_INVALID_FROZEN_CONTRACT")
    if contract.get("position_action") == "EXPLICIT_POSITION_AMENDMENT":
        try:
            from trader.kr.us_profit_existing_amendment import (
                verified_amendment_readback,
            )
            if not verified_amendment_readback(engine=engine, contract=contract):
                return block("KR_TP_EXISTING_POSITION_AMENDMENT_NOT_DURABLE")
        except Exception:
            return block("KR_TP_EXISTING_POSITION_AMENDMENT_DB_UNAVAILABLE")
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
        if orderable != qty:
            # Partial sell reservation / unsettled broker order may consume
            # available inventory without being recorded in our DB yet.
            # Do not manufacture a smaller TP target from that inventory.
            return block("KR_TP_BROKER_RESERVED_INVENTORY_RECONCILE_REQUIRED")
        # The immutable BUY contract is not sufficient: compare the current
        # broker balance avg, qty and orderable shares to DB every exit tick.
        from trader.execution_state import BalanceFreshness
        from datetime import datetime, timezone
        if (
            balance_snapshot is None
            or getattr(balance_snapshot, "freshness", None) != BalanceFreshness.FRESH
            or not callable(getattr(balance_snapshot, "position", None))
        ):
            return block("KR_TP_KIS_BALANCE_NOT_FRESH")
        acquired_at = balance_snapshot.fetched_at
        if acquired_at.tzinfo is None or (
            datetime.now(timezone.utc) - acquired_at.astimezone(timezone.utc)
        ).total_seconds() > 180:
            return block("KR_TP_KIS_BALANCE_TOO_OLD")
        broker = balance_snapshot.position(code)
        broker_avg = float(broker.avg_price or 0)
        db_avg = float(position.get("avg_buy_price") or 0)
        if (
            broker.qty != qty or broker.orderable_qty != orderable
            or broker_avg <= 0 or not math.isfinite(broker_avg)
            or not math.isfinite(db_avg) or db_avg <= 0
            or abs(broker_avg - db_avg) / broker_avg > 0.005
        ):
            return block("KR_TP_KIS_AVG_OR_QTY_MISMATCH")
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
        basis = broker_avg
        if not math.isfinite(price) or price <= 0 or not math.isfinite(basis) or basis <= 0:
            return block("KR_TP_PRICE_OR_BASIS_UNVERIFIED")
        states = load_frozen_stage_states(
            engine=engine, contract=contract, account_id=str(account),
            trading_epoch_id=str(epoch), position_cycle_id=cycle, code=str(code).zfill(6),
        )
        unresolved_sell = has_unresolved_kr_sell_orders(
            engine=engine, env=env, code=code,
            trading_epoch_id=str(epoch),
        )
        d = calculate_next_tp_stage(
            contract=contract, stage_states=states,
            holding_qty=qty, orderable_qty=orderable,
            verified_entry_price=basis, executable_price=price,
            unresolved_sell=unresolved_sell,
        )
        if d.action == "RUNNER" and contract.get("runner_version") == "US_V3":
            from datetime import datetime
            from zoneinfo import ZoneInfo
            from trader.kr.us_v3_runner_active import (
                load_kr_kis_native_completed_daily, evaluate_kr_v3_runner,
            )
            today = datetime.now(ZoneInfo("Asia/Seoul")).date()
            native_daily, native_source = load_kr_kis_native_completed_daily(
                kis=kis, code=code, today=today,
            )
            runner = evaluate_kr_v3_runner(
                contract=contract, stage_states=states,
                position=position, verified_entry_price=basis,
                executable_price=price, holding_qty=qty,
                orderable_qty=orderable, completed_daily=native_daily,
                completed_daily_source=native_source,
                today=today, unresolved_sell=unresolved_sell,
            )
            return {
                **runner,
                "contract": contract,
                "account_id": str(account), "epoch_id": str(epoch),
                "position_cycle_id": cycle, "code": str(code).zfill(6),
                "fresh_bid_or_last": price, "verified_entry_price": basis,
                "balance_snapshot_id": balance_snapshot.snapshot_id,
            }
        return {
            "action": d.action, "reason": d.reason, "stage": d.stage,
            "qty": d.target_qty, "contract": contract,
            "account_id": str(account), "epoch_id": str(epoch),
            "position_cycle_id": cycle, "code": str(code).zfill(6),
            "fresh_bid_or_last": price, "verified_entry_price": basis,
            "balance_snapshot_id": balance_snapshot.snapshot_id,
        }
    except Exception as exc:
        return block("KR_TP_EVIDENCE_UNAVAILABLE:" + type(exc).__name__)
