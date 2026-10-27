"""Restore a BUY's immutable exit contract only with exact broker/claim proof.

No symbol-only adoption. No KR_INFINITE overlay. Ambiguity is a hard skip.
Called after authoritative KIS reconciliation and fresh holdings validation.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.repos import (
    _assert_kr_buy_entry_contract, _kr_entry_exit_plan_sha256,
)
from trader.db.schema import schema_for_engine

log = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")
_BROKER_TERMINAL = {"FILLED", "RECONCILED"}


def _as_json(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _on_trade_date(value, trade_date: date) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        try:
            from datetime import datetime
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
    if getattr(value, "tzinfo", None) is None:
        # PostgreSQL timestamps are UTC; SQLite offline fixtures may be naive.
        from datetime import timezone
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(_KST).date() == trade_date


def _verified_buy(order: dict, position: dict, claim: dict, trade_date: date) -> dict | None:
    """Validate frozen BUY evidence. Never infer a plan from a ticker alone."""
    if str(order.get("status") or "").upper() not in _BROKER_TERMINAL:
        return None
    if not _on_trade_date(order.get("created_at"), trade_date):
        return None
    if not order.get("kis_odno"):
        return None
    if any(str(order.get(k) or "") != str(position.get(k) or "") for k in (
        "env", "strategy", "sid", "mode", "code", "portfolio_epoch_id", "trading_epoch_id"
    )):
        return None
    if int(order.get("qty") or 0) != int(position.get("qty") or 0):
        return None
    request = _as_json(order.get("request_json"))
    response = _as_json(order.get("response_json"))
    try:
        _assert_kr_buy_entry_contract(request)
    except (RuntimeError, ValueError, TypeError):
        return None
    frozen_hash = request.get("entry_contract_sha256")
    ack_hash = _as_json(response.get("_kr_buy_entry_contract")).get("entry_contract_sha256")
    if not frozen_hash or frozen_hash != ack_hash:
        return None
    if str(request.get("position_cycle_id")) != str(order.get("position_cycle_id")):
        return None
    if str(order.get("position_cycle_id")) == str(position.get("position_cycle_id")):
        return None  # Already exact-cycle, not an imported-cycle recovery.
    if str(claim.get("trading_epoch_id")) != str(position.get("trading_epoch_id")):
        return None
    if (
        claim.get("strategy_owner") != "PB1"
        or claim.get("action") != "BUY_ENTRY:PB1-AM"
        or str(claim.get("action_state")) != "SATISFIED"
        or str(claim.get("lifecycle_id") or "").split(":")[-1] != str(position.get("code"))
        or int(claim.get("target_qty") or 0) != int(position.get("qty") or 0)
        or int(claim.get("cumulative_filled_qty") or 0) != int(position.get("qty") or 0)
        or claim.get("trade_date") != trade_date
    ):
        return None
    plan = request.get("entry_exit_plan")
    if not isinstance(plan, dict):
        return None
    if not all(plan.get(k) for k in (
        "exit_policy_family", "entry_reason", "trade_horizon", "policy_version"
    )):
        return None
    if plan.get("exit_policy_family") == "POLICY_MISSING":
        return None
    return request


def restore_verified_imported_buy_contracts(
    *, engine, env: str, strategy: str, trade_date: date,
    holdings_rows: list[dict], fresh_balance_proven: bool,
) -> int:
    """Link a proven PB1 BUY to its exact SYSTEM cycle, without fake fills.

    No import recovery without fresh KIS holdings, a unique same-day frozen BUY
    matching those holdings, and a fully satisfied same-epoch PB1 execution claim.
    Idempotent: only IMPORTED/POLICY_MISSING rows are eligible, and source-cycle
    collisions are rejected. Existing qty, cost and realized P&L are preserved.
    """
    if not fresh_balance_proven:
        return 0
    balances: dict[str, int] = {}
    for holding in holdings_rows or []:
        code = str(holding.get("pdno") or holding.get("code") or "").zfill(6)
        qty = int(float(holding.get("hldg_qty") or holding.get("qty") or 0))
        if qty <= 0:
            continue
        if code in balances:
            log.error("[KR_CONTRACT_RECOVERY][AMBIGUOUS_BROKER_HOLDINGS] code=%s", code)
            return 0
        balances[code] = qty

    schema = schema_for_engine(engine)
    restored = 0
    with engine.begin() as conn:
        rows = conn.execute(
            sa.select(schema.positions).where(sa.and_(
                schema.positions.c.env == env,
                schema.positions.c.strategy == strategy,
                schema.positions.c.status == "OPEN",
                schema.positions.c.position_origin == "IMPORTED",
                schema.positions.c.exit_policy_family == "POLICY_MISSING",
                schema.positions.c.qty > 0,
            )).with_for_update()
        ).mappings().all()
        for row in rows:
            pos = dict(row)
            code = str(pos["code"]).zfill(6)
            if code == "122630":  # Exclusive KR_INFINITE owner, not PB1.
                continue
            if balances.get(code) != int(pos["qty"]):
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=BROKER_QTY_MISMATCH", code)
                continue
            cycles = conn.execute(
                sa.select(schema.positions.c.position_id).where(sa.and_(
                    schema.positions.c.env == env,
                    schema.positions.c.strategy == strategy,
                    schema.positions.c.code == code,
                    schema.positions.c.trading_epoch_id == pos["trading_epoch_id"],
                    schema.positions.c.status == "OPEN",
                    schema.positions.c.qty > 0,
                ))
            ).all()
            if len(cycles) != 1:
                log.error("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=DUPLICATE_OPEN_CYCLE", code)
                continue
            buys = [dict(o) for o in conn.execute(
                sa.select(schema.orders).where(sa.and_(
                    schema.orders.c.env == pos["env"],
                    schema.orders.c.strategy == pos["strategy"],
                    schema.orders.c.sid == pos["sid"],
                    schema.orders.c.mode == pos["mode"],
                    schema.orders.c.code == code,
                    schema.orders.c.side == "BUY",
                    schema.orders.c.trading_epoch_id == pos["trading_epoch_id"],
                    schema.orders.c.portfolio_epoch_id == pos["portfolio_epoch_id"],
                ))
            ).mappings().all() if _on_trade_date(o["created_at"], trade_date)]
            if len(buys) != 1:
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=BUY_ORDER_AMBIGUOUS count=%s", code, len(buys))
                continue
            buy = buys[0]
            original = conn.execute(
                sa.select(schema.positions.c.position_id).where(
                    schema.positions.c.position_cycle_id == buy["position_cycle_id"]
                )
            ).first()
            if original:
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=SOURCE_CYCLE_ALREADY_EXISTS", code)
                continue
            claims = [dict(c) for c in conn.execute(
                sa.select(schema.execution_claims).where(sa.and_(
                    schema.execution_claims.c.env == env,
                    schema.execution_claims.c.strategy_owner == "PB1",
                    schema.execution_claims.c.trade_date == trade_date,
                    schema.execution_claims.c.trading_epoch_id == pos["trading_epoch_id"],
                    schema.execution_claims.c.action == "BUY_ENTRY:PB1-AM",
                ))
            ).mappings().all() if str(c["lifecycle_id"]).split(":")[-1] == code]
            if len(claims) != 1:
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=CLAIM_AMBIGUOUS count=%s", code, len(claims))
                continue
            request = _verified_buy(buy, pos, claims[0], trade_date)
            if request is None:
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=CONTRACT_OR_CLAIM_INVALID", code)
                continue
            plan = request["entry_exit_plan"]
            previous_meta = _as_json(pos.get("position_meta"))
            new_meta = dict(previous_meta)
            # Claim and KIS holdings jointly prove a current-session BUY
            # even if the exact fill timestamp has not been persisted.
            # Do not synthesize a broker fill time.
            new_meta["same_day_entry_proven"] = True
            new_meta["same_day_entry_provenance"] = "ORDER_FILL_CURRENT_DAY"
            new_meta["holding_age_unknown"] = False
            new_meta["frozen_contract_recovery"] = {
                "source_buy_order_id": str(buy["order_id"]),
                "old_imported_cycle": str(pos["position_cycle_id"]),
                "entry_contract_sha256": request["entry_contract_sha256"],
                "entry_exit_plan_sha256": _kr_entry_exit_plan_sha256(plan),
                "provenance": "KIS_HOLDING_PLUS_SATISFIED_PB1_CLAIM",
            }
            entry_meta = dict(_as_json(request.get("entry_meta")))
            entry_meta.update({
                "entry_contract_sha256": request["entry_contract_sha256"],
                "source_buy_order_id": str(buy["order_id"]),
                "source_buy_client_order_key": request.get("client_order_key"),
                "entry_exit_plan_sha256": _kr_entry_exit_plan_sha256(plan),
            })
            updates = {
                "position_origin": "SYSTEM",
                "position_cycle_id": buy["position_cycle_id"],
                "entry_meta_json": entry_meta,
                "entry_exit_plan_json": plan,
                "position_meta": new_meta,
                "entry_thesis": plan.get("entry_thesis"),
                "entry_reason": plan["entry_reason"],
                "entry_style_selected": plan.get("entry_style_selected"),
                "trade_horizon": plan["trade_horizon"],
                "exit_policy_family": plan["exit_policy_family"],
                "policy_source": plan.get("policy_source"),
                "policy_version": plan["policy_version"],
                "eod_action": plan.get("eod_action"),
                "force_eod_close": bool(plan.get("force_eod_close")),
                "max_trading_days": (plan.get("time_plan") or {}).get("max_trading_days"),
                "initial_stop_price": (plan.get("risk_plan") or {}).get("initial_stop"),
                "initial_risk_r": (plan.get("risk_plan") or {}).get("risk_R"),
                "updated_at": sa.func.now(),
            }
            result = conn.execute(
                sa.update(schema.positions).where(sa.and_(
                    schema.positions.c.position_id == pos["position_id"],
                    schema.positions.c.position_cycle_id == pos["position_cycle_id"],
                    schema.positions.c.exit_policy_family == "POLICY_MISSING",
                    schema.positions.c.position_origin == "IMPORTED",
                )).values(**updates)
            )
            if int(result.rowcount or 0) == 1:
                restored += 1
                log.warning("[KR_CONTRACT_RECOVERY][RESTORED] code=%s source_order=%s original_cycle=%s",
                            code, buy["order_id"], buy["position_cycle_id"])
    return restored
