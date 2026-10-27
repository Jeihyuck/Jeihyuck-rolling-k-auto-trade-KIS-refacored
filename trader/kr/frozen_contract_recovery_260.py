"""Restore a BUY's immutable exit contract only with exact broker/claim proof.

No symbol-only adoption. No KR_INFINITE overlay. Ambiguity is a hard skip.
Called after authoritative KIS reconciliation and fresh holdings validation.
"""
from __future__ import annotations

import json
import logging
import hashlib
from datetime import date, timedelta, datetime, time
from decimal import Decimal, InvalidOperation
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


def _broker_buy_cumulative_verified(
    response: dict, *, code: str, order_number: str, quantity: int,
    trade_date: date,
) -> bool:
    """Require an exact KIS cumulative execution, not ACK/RECONCILED alone.

    The order-level KIS inquiry response must match the original BUY order,
    show all requested shares executed, no remaining shares, and a priced
    execution. A missing/zero/ambiguous broker observation is never promoted.
    """
    try:
        if str(response.get("pdno") or "").removeprefix("A").zfill(6) != str(code).zfill(6):
            return False
        if str(response.get("odno") or "").zfill(10) != str(order_number).zfill(10):
            return False
        if str(response.get("ord_dt") or "").replace("-", "") != trade_date.strftime("%Y%m%d"):
            return False
        if str(response.get("sll_buy_dvsn_cd") or "") != "02":
            return False
        if str(response.get("cncl_yn") or "").upper() == "Y":
            return False
        requested = Decimal(str(response.get("ord_qty")))
        executed = Decimal(str(response.get("tot_ccld_qty")))
        remaining = Decimal(str(response.get("rmn_qty")))
        notional = Decimal(str(response.get("tot_ccld_amt")))
        average = Decimal(str(response.get("avg_prvs")))
        if any(not number.is_finite() for number in
               (requested, executed, remaining, notional, average)):
            return False
        if (requested != quantity or executed != quantity or remaining != 0
                or quantity <= 0 or notional <= 0 or average <= 0):
            return False
        # KIS rounds average prices to whole KRW; validate against notional.
        return abs(notional / executed - average) <= max(
            Decimal("1"), average * Decimal("0.001")
        )
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        return False


def _verified_buy(order: dict, position: dict, claim: dict, trade_date: date, account_hash: str) -> dict | None:
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
    if not _broker_buy_cumulative_verified(
        response, code=str(order["code"]), order_number=str(order["kis_odno"]),
        quantity=int(order["qty"]), trade_date=trade_date,
    ):
        return None
    if str(request.get("position_cycle_id")) != str(order.get("position_cycle_id")):
        return None
    if str(order.get("position_cycle_id")) == str(position.get("position_cycle_id")):
        return None  # Already exact-cycle, not an imported-cycle recovery.
    if str(claim.get("trading_epoch_id")) != str(position.get("trading_epoch_id")):
        return None
    if (
        str(claim.get("account_id") or "") != account_hash
        or claim.get("market") != "KR"
        or claim.get("strategy_owner") != "PB1"
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
    account_id: str, lookback_calendar_days: int = 7,
) -> int:
    """Recover a prior-day PB1 BUY from *frozen* KIS order evidence.

    The source order date, rather than the reconciliation run date, binds KIS
    execution evidence and the matching claim. Preserve the entry contract;
    never synthesize fills, quantities, P&L or a new SELL order.

    Cross-session recovery requires fresh current account holdings, exact
    portfolio/trading account+epoch, one unique original BUY, fully priced KIS
    cumulative execution, a satisfied same-date claim, no later economic
    activity, and a unique imported position. If any proof is absent it remains
    POLICY_MISSING with a review log. Normal per-tick trading is unaffected.
    """
    if not fresh_balance_proven or env != "practice":
        return 0
    if strategy != "pb1_pullback_close" or not account_id:
        return 0
    if not isinstance(trade_date, date) or not (1 <= lookback_calendar_days <= 7):
        return 0

    account_hash = hashlib.sha256(account_id.encode("utf-8")).hexdigest()
    earliest_day = trade_date - timedelta(days=lookback_calendar_days)
    balances: dict[str, int] = {}
    for holding in holdings_rows or []:
        if not isinstance(holding, dict):
            return 0
        code = str(holding.get("pdno") or holding.get("code") or "").removeprefix("A").zfill(6)
        try:
            from decimal import Decimal
            quantity = Decimal(str(holding.get("hldg_qty") or holding.get("qty") or "0"))
            if not quantity.is_finite() or quantity != int(quantity) or quantity < 0:
                return 0
            qty = int(quantity)
        except (TypeError, ValueError, InvalidOperation):
            return 0
        if not code.isdigit() or len(code) != 6 or code in balances:
            log.error("[KR_CONTRACT_RECOVERY][AMBIGUOUS_HOLDINGS] code=%s", code)
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
            if code == "122630":  # KR_INFINITE sleeve: never adopt PB1.
                continue
            if balances.get(code) != int(pos["qty"]):
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=BROKER_QTY_MISMATCH", code)
                continue

            portfolio_epoch = conn.execute(
                sa.select(schema.portfolio_epochs).where(
                    schema.portfolio_epochs.c.portfolio_epoch_id == pos["portfolio_epoch_id"]
                )
            ).mappings().first()
            trading_epoch = conn.execute(
                sa.select(schema.trading_epochs).where(
                    schema.trading_epochs.c.trading_epoch_id == pos["trading_epoch_id"]
                )
            ).mappings().first()
            if not portfolio_epoch or not trading_epoch or any([
                portfolio_epoch["account_id"] != account_id,
                trading_epoch["account_id"] != account_id,
                portfolio_epoch["env"] != env,
                trading_epoch["env"] != env,
                portfolio_epoch["strategy"] != strategy,
                portfolio_epoch["sid"] != pos["sid"],
                portfolio_epoch["mode"] != pos["mode"],
                str(portfolio_epoch["trading_epoch_id"]) != str(pos["trading_epoch_id"]),
            ]):
                log.error("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=ACCOUNT_OR_EPOCH_MISMATCH", code)
                continue

            # Account-wide uniqueness: otherwise a second sleeve or lifecycle
            # may own part of the broker's aggregate share balance.
            live_cycles = conn.execute(
                sa.select(schema.positions.c.position_cycle_id).where(sa.and_(
                    schema.positions.c.env == env,
                    schema.positions.c.trading_epoch_id == pos["trading_epoch_id"],
                    schema.positions.c.code == code,
                    schema.positions.c.status == "OPEN",
                    schema.positions.c.qty > 0,
                ))
            ).all()
            if len(live_cycles) != 1:
                log.error("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=DUPLICATE_OPEN_CYCLE", code)
                continue

            orders = [dict(o) for o in conn.execute(
                sa.select(schema.orders).where(sa.and_(
                    schema.orders.c.env == env,
                    schema.orders.c.strategy == strategy,
                    schema.orders.c.sid == pos["sid"],
                    schema.orders.c.mode == pos["mode"],
                    schema.orders.c.code == code,
                    schema.orders.c.trading_epoch_id == pos["trading_epoch_id"],
                    schema.orders.c.portfolio_epoch_id == pos["portfolio_epoch_id"],
                )).order_by(schema.orders.c.created_at.asc())
            ).mappings().all()]
            # Require exactly one BUY in the original lifecycle; later or
            # historical duplicate BUY/Sell attempts demand manual attribution.
            buys = [o for o in orders if o["side"] == "BUY"]
            if len(buys) != 1:
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=BUY_ORDER_AMBIGUOUS count=%s",
                            code, len(buys))
                continue
            buy = buys[0]
            bought_at = buy.get("created_at")
            if bought_at is None:
                continue
            # Validate the date as Korea-local even when SQL stores UTC.
            if getattr(bought_at, "tzinfo", None) is None:
                from datetime import timezone
                bought_at = bought_at.replace(tzinfo=timezone.utc)
            bought_on = bought_at.astimezone(_KST).date()
            if not earliest_day <= bought_on <= trade_date:
                log.info("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=OUTSIDE_BOUNDED_LOOKBACK", code)
                continue
            if any(o["side"] == "SELL" for o in orders):
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=SELL_ORDER_PRESENT", code)
                continue

            # Imported lot is valid only when original BUY started from zero
            # holdings. Subsequent additions/transfers cannot be inferred.
            request = _as_json(buy.get("request_json"))
            if str(request.get("pre_order_holding_qty")) != "0":
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=NONZERO_PREBUY_BALANCE", code)
                continue

            # Price/quantity-adjusting broker fills of *other* orders cannot
            # be interpreted as belonging to this original frozen contract.
            fills = conn.execute(sa.select(
                schema.fills.c.side, schema.fills.c.order_id,
                schema.fills.c.qty, schema.fills.c.filled_at,
            ).where(sa.and_(
                schema.fills.c.env == env,
                schema.fills.c.trading_epoch_id == pos["trading_epoch_id"],
                schema.fills.c.code == code,
            ))).mappings().all()
            unrelated = [f for f in fills if str(f.get("order_id")) != str(buy["order_id"])]
            relevant = [f for f in fills if str(f.get("order_id")) == str(buy["order_id"])]
            if (unrelated or any(f.get("side") != "BUY" for f in relevant)
                    or (relevant and sum(int(f.get("qty") or 0) for f in relevant) != int(buy["qty"]))):
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=FILL_ACTIVITY_AMBIGUOUS", code)
                continue

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
                    schema.execution_claims.c.account_id == account_hash,
                    schema.execution_claims.c.strategy_owner == "PB1",
                    schema.execution_claims.c.trade_date == bought_on,
                    schema.execution_claims.c.trading_epoch_id == pos["trading_epoch_id"],
                    schema.execution_claims.c.action == "BUY_ENTRY:PB1-AM",
                ))
            ).mappings().all() if str(c.get("lifecycle_id") or "").split(":")[-1] == code]
            if len(claims) != 1:
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=CLAIM_AMBIGUOUS count=%s",
                            code, len(claims))
                continue

            verified_request = _verified_buy(buy, pos, claims[0], bought_on, account_hash)
            if verified_request is None:
                log.warning("[KR_CONTRACT_RECOVERY][SKIP] code=%s reason=CONTRACT_OR_KIS_FILL_INVALID", code)
                continue

            plan = verified_request["entry_exit_plan"]
            previous_meta = _as_json(pos.get("position_meta"))
            new_meta = dict(previous_meta)
            new_meta["same_day_entry_proven"] = True
            new_meta["same_day_entry_provenance"] = "ORDER_FILL_ORIGINAL_TRADE_DATE"
            new_meta["holding_age_unknown"] = False
            new_meta["frozen_contract_recovery"] = {
                "source_buy_order_id": str(buy["order_id"]),
                "old_imported_cycle": str(pos["position_cycle_id"]),
                "entry_contract_sha256": verified_request["entry_contract_sha256"],
                "entry_exit_plan_sha256": _kr_entry_exit_plan_sha256(plan),
                "source_trade_date": bought_on.isoformat(),
                "reconciled_trade_date": trade_date.isoformat(),
                "provenance": "KIS_ORIGINAL_CUMULATIVE_PLUS_FRESH_HOLDINGS_PLUS_CLAIM",
            }
            entry_meta = dict(_as_json(verified_request.get("entry_meta")))
            entry_meta.update({
                "entry_contract_sha256": verified_request["entry_contract_sha256"],
                "source_buy_order_id": str(buy["order_id"]),
                "source_buy_client_order_key": verified_request.get("client_order_key"),
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
                    schema.positions.c.qty == pos["qty"],
                )).values(**updates)
            )
            if int(result.rowcount or 0) != 1:
                raise RuntimeError("KR_CONTRACT_RECOVERY_CONCURRENT_MUTATION")
            post = conn.execute(
                sa.select(schema.positions).where(
                    schema.positions.c.position_id == pos["position_id"]
                )
            ).mappings().one()
            if (post["exit_policy_family"] != plan["exit_policy_family"]
                    or post["position_origin"] != "SYSTEM"
                    or str(post["position_cycle_id"]) != str(buy["position_cycle_id"])
                    or _as_json(post["entry_meta_json"]).get("entry_contract_sha256")
                       != verified_request["entry_contract_sha256"]
                    or int(post["qty"]) != int(pos["qty"])):
                raise RuntimeError("KR_CONTRACT_RECOVERY_POSTWRITE_INVARIANT_BROKEN")
            restored += 1
            log.warning(
                "[KR_CONTRACT_RECOVERY][RESTORED] code=%s order=%s source_date=%s reconciled=%s",
                code, buy["order_id"], bought_on, trade_date,
            )
    return restored


def verify_recovered_contract_invariants(*, engine, env: str, strategy: str) -> int:
    """Read-only post-reconcile drift detection. Never rewrite frozen policy."""
    schema = schema_for_engine(engine)
    errors = 0
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.positions).where(sa.and_(
                schema.positions.c.env == env,
                schema.positions.c.strategy == strategy,
                schema.positions.c.status == "OPEN",
            ))
        ).mappings().all()
        for pos in rows:
            proof = _as_json(pos.get("position_meta")).get("frozen_contract_recovery")
            if not isinstance(proof, dict):
                continue
            plan = _as_json(pos.get("entry_exit_plan_json"))
            entry = _as_json(pos.get("entry_meta_json"))
            if (pos.get("position_origin") != "SYSTEM"
                    or pos.get("exit_policy_family") != plan.get("exit_policy_family")
                    or pos.get("trade_horizon") != plan.get("trade_horizon")
                    or bool(pos.get("force_eod_close")) != bool(plan.get("force_eod_close"))
                    or pos.get("eod_action") != plan.get("eod_action")
                    or str(entry.get("entry_contract_sha256") or "") != str(proof.get("entry_contract_sha256") or "")
                    or not plan.get("exit_policy_family")
                    or str(pos.get("position_cycle_id")) == str(proof.get("old_imported_cycle"))):
                errors += 1
                log.error(
                    "[KR_CONTRACT_RECOVERY][INVARIANT_BROKEN] code=%s action=QUARANTINE_REVIEW_ONLY",
                    pos["code"],
                )
    return errors
