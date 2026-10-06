"""Read-only KR/US settlement projection from existing reconciliation paths.

Feature-gated OFF by default. Never inserts fills, amends claims, alters
positions, or gives broker-submit authority. Incomplete provenance is reported,
not fabricated.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import logging
import os
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.settlement.core import (
    SettlementConflict, SettlementObservation, preview_decision,
)
from trader.settlement.schema import applications

log = logging.getLogger(__name__)


def enabled() -> bool:
    return os.getenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "0").strip() == "1"


def _digest(source: str, key: str, cumulative: int, price: Decimal | None) -> str:
    material = json.dumps({
        "source": source, "key": key, "cumulative": cumulative,
        "execution_price": str(price) if price is not None else None,
    }, sort_keys=True)
    return hashlib.sha256(material.encode()).hexdigest()


def _account_scope(env: str) -> str:
    return hashlib.sha256(get_account_key(env=env).encode("utf-8")).hexdigest()


def _review(market: str, reason: str, symbol: str) -> dict:
    result = {"status": "REVIEW_REQUIRED", "market": market, "reason": reason, "symbol": symbol}
    log.warning("[SETTLEMENT_SHADOW][REVIEW_REQUIRED] %s", result)
    return result


def _report(obs: SettlementObservation, *, engine=None, symbol: str = "") -> dict:
    """Read existing ledger watermark when present, without changing DB."""
    applied_qty = 0
    applied_notional = Decimal(0)
    price_status = "PRICE_PENDING"
    baseline = "NOT_MIGRATED"
    if engine is not None:
        try:
            with engine.connect() as conn:
                row = conn.execute(sa.select(
                    applications.c.applied_qty,
                    applications.c.applied_notional,
                    applications.c.price_status,
                ).where(applications.c.settlement_key == (
                    __import__("trader.settlement.core", fromlist=["canonical_settlement_key"])
                    .canonical_settlement_key(obs)
                ))).mappings().one_or_none()
            if row is not None:
                applied_qty = int(row["applied_qty"])
                applied_notional = Decimal(str(row["applied_notional"]))
                price_status = str(row["price_status"])
                baseline = "EXISTING_APPLICATION"
        except Exception as exc:
            baseline = "LEDGER_UNAVAILABLE:" + type(exc).__name__
    try:
        decision = preview_decision(
            obs, applied_qty=applied_qty,
            applied_notional=applied_notional, price_status=price_status,
        )
        result = {
            "status": "SHADOW_ONLY", "market": obs.market, "symbol": symbol,
            "owner": obs.strategy_owner, "baseline": baseline,
            "cumulative_qty": obs.cumulative_qty, "proposed_qty_delta": decision.qty_delta,
            "proposed_notional_delta": str(decision.notional_delta),
            "price_status": decision.price_status,
        }
        log.info("[SETTLEMENT_SHADOW][COMPARE] %s", result)
        return result
    except SettlementConflict as exc:
        return _review(obs.market, str(exc), symbol)


def shadow_kr_promotion(*, engine, order: dict, request_json: dict,
                        cumulative_qty: int, broker_fill_price,
                        pre_holding_qty: int | None, post_holding_qty: int,
                        exclusive_order_proof: bool) -> dict | None:
    if not enabled():
        return None
    symbol = str(order.get("code") or "")
    try:
        raw_owner = str(request_json.get("strategy_owner") or "")
        owner = {"KR_STANDARD": "PB1", "PB1": "PB1",
                 "KR_INFINITE": "KR_INFINITE"}.get(raw_owner, "")
        if not owner:
            return _review("KR", "STRATEGY_OWNER_MISSING", symbol)
        submitted = order.get("submitted_at") or order.get("created_at")
        if not isinstance(submitted, datetime) or not submitted.tzinfo:
            return _review("KR", "ORIGINAL_ORDER_TIME_UNKNOWN", symbol)
        original_day = submitted.astimezone(ZoneInfo("Asia/Seoul")).date()
        side = str(order.get("side") or "").upper()
        source = "KIS_ORDER_CUMULATIVE_ACTUAL" if broker_fill_price else "HOLDINGS_DELTA_EXCLUSIVE"
        price = Decimal(str(broker_fill_price)) if broker_fill_price else None
        client_key = str(order.get("client_order_key") or "")
        obs = SettlementObservation(
            env=str(order.get("env") or "practice"), market="KR",
            account_scope=_account_scope(str(order.get("env") or "practice")),
            trading_epoch_id=str(order.get("trading_epoch_id") or ""),
            strategy_owner=owner, position_cycle_id=str(order.get("position_cycle_id") or ""),
            client_order_key=client_key, broker_trade_date=original_day,
            exchange=str(order.get("market") or "KRX"),
            broker_order_no=str(order.get("kis_odno") or order.get("broker_order_id") or ""),
            side=side, requested_qty=int(order.get("qty") or 0),
            cumulative_qty=int(cumulative_qty),
            evidence_type=source, currency="KRW",
            execution_price=price, pre_holding_qty=pre_holding_qty,
            post_holding_qty=post_holding_qty,
            exclusive_order_proof=exclusive_order_proof,
            evidence_digest=_digest(source, client_key, cumulative_qty, price),
        )
        return _report(obs, engine=engine, symbol=symbol)
    except (SettlementConflict, ValueError, TypeError) as exc:
        return _review("KR", str(exc), symbol)


def shadow_us_reconcile(*, order: dict, trade_date: str,
                        cumulative_qty: int, broker_fill_price,
                        evidence_type: str, pre_holding_qty=None,
                        post_holding_qty=None, exclusive_order_proof: bool = False,
                        engine=None) -> dict | None:
    if not enabled():
        return None
    symbol = str(order.get("symbol") or "")
    meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
    try:
        owner = str(meta.get("strategy_owner") or order.get("strategy_owner") or "")
        if owner not in {"US_STANDARD", "PB1", "TQQQ_INFINITE"}:
            return _review("US", "STRATEGY_OWNER_MISSING", symbol)
        env = str(order.get("env") or os.getenv("KIS_ENV") or "practice")
        key = str(order.get("client_order_key") or "")
        price = Decimal(str(broker_fill_price)) if broker_fill_price else None
        source = (
            "KIS_ORDER_CUMULATIVE_ACTUAL"
            if evidence_type == "KIS_ORDER_CUMULATIVE_ACTUAL"
            else "HOLDINGS_DELTA_EXCLUSIVE"
        )
        if source == "HOLDINGS_DELTA_EXCLUSIVE":
            price = None  # balance / limit price is never execution-price proof
        obs = SettlementObservation(
            env=env, market="US", account_scope=_account_scope(env),
            trading_epoch_id=str(order.get("trading_epoch_id") or meta.get("trading_epoch_id") or ""),
            strategy_owner=owner,
            position_cycle_id=str(meta.get("position_lifecycle_id") or order.get("position_lifecycle_id") or ""),
            client_order_key=key, broker_trade_date=date.fromisoformat(str(trade_date)),
            exchange=str(order.get("exchange") or meta.get("exchange") or ""),
            broker_order_no=str(order.get("order_no") or ""),
            side=str(order.get("side") or "").upper(),
            requested_qty=int(order.get("qty_requested") or order.get("qty") or 0),
            cumulative_qty=int(cumulative_qty),
            evidence_type=source, evidence_digest=_digest(source, key, cumulative_qty, price),
            currency="USD", execution_price=price,
            pre_holding_qty=pre_holding_qty, post_holding_qty=post_holding_qty,
            exclusive_order_proof=exclusive_order_proof,
        )
        return _report(obs, engine=engine, symbol=symbol)
    except (SettlementConflict, ValueError, TypeError) as exc:
        return _review("US", str(exc), symbol)
