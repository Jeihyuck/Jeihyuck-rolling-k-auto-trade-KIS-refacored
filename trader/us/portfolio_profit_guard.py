"""Signed US_STANDARD portfolio giveback policy and audit-only valuation.

NO thresholds are set by the application. Values must be approved in a
separately HMAC-signed, scoped policy. This module does NOT submit orders,
rewrite immutable BUY contracts or use Infinite equity as trigger capital.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

POLICY_PURPOSE = "US_STANDARD_PORTFOLIO_GIVEBACK_SELL_V1"


def canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("BOOLEAN_FINANCIAL_VALUE")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("INVALID_FINANCIAL_VALUE") from exc
    if not number.is_finite():
        raise ValueError("NONFINITE_FINANCIAL_VALUE")
    return number


def _utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("MISSING_UTC_TIMESTAMP")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError("INVALID_UTC_TIMESTAMP") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("NAIVE_TIMESTAMP")
    return timestamp.astimezone(timezone.utc)


def policy_digest(authorization: dict) -> str:
    body = {k: v for k, v in authorization.items() if k != "hmac_sha256"}
    return hashlib.sha256(canonical(body)).hexdigest()


def verify_portfolio_policy(
    authorization: dict, *,
    signing_key: str,
    account_hash: str,
    trading_epoch_id: str,
    portfolio_epoch_id: str,
    now_utc: datetime,
) -> tuple[dict, str]:
    """Strict, no-fallback authorization for active portfolio SELL.

    This is distinct from original BUY contract signatures and V2→V3 handoffs.
    Never accept a digest, unsigned manifest, altered policy, or a signing key
    shorter than 32 bytes as native order permission.
    """
    if not isinstance(authorization, dict):
        raise ValueError("PORT_GUARD_POLICY_MISSING")
    if len(signing_key.encode("utf-8")) < 32:
        raise ValueError("PORT_GUARD_SIGNING_KEY_UNVERIFIED")
    if not isinstance(now_utc, datetime) or now_utc.tzinfo is None:
        raise ValueError("PORT_GUARD_NOW_UNVERIFIED")
    body = {k: v for k, v in authorization.items() if k != "hmac_sha256"}
    actual_signature = str(authorization.get("hmac_sha256") or "")
    expected_signature = hmac.new(signing_key.encode("utf-8"), canonical(body), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(actual_signature, expected_signature):
        raise ValueError("PORT_GUARD_POLICY_SIGNATURE_INVALID")
    required = {
        "purpose": POLICY_PURPOSE,
        "mode": "ACTIVE",
        "env": "practice",
        "sleeve": "US_STANDARD",
        "account_hash": account_hash,
        "trading_epoch_id": trading_epoch_id,
        "portfolio_epoch_id": portfolio_epoch_id,
    }
    if any(body.get(field) != value for field, value in required.items()):
        raise ValueError("PORT_GUARD_POLICY_SCOPE_MISMATCH")
    if not body.get("policy_id") or not body.get("policy_version"):
        raise ValueError("PORT_GUARD_POLICY_VERSION_MISSING")
    if not (_utc(body.get("effective_at")) <= now_utc.astimezone(timezone.utc) < _utc(body.get("expires_at"))):
        raise ValueError("PORT_GUARD_POLICY_NOT_IN_FORCE")
    if body.get("accounting_method") != "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1":
        raise ValueError("PORT_GUARD_ACCOUNTING_METHOD_UNVERIFIED")
    if body.get("mark_source") not in ("KIS_BID", "KIS_EXECUTABLE_QUOTE"):
        raise ValueError("PORT_GUARD_MARK_SOURCE_UNVERIFIED")
    ttl = _decimal(body.get("quote_ttl_sec"))
    skew = _decimal(body.get("max_snapshot_skew_sec"))
    activation = _decimal(body.get("activation_profit_usd"))
    if not (Decimal("0") < ttl <= Decimal("5") and
            Decimal("0") < skew <= Decimal("5") and activation > 0):
        raise ValueError("PORT_GUARD_ACTIVATION_OR_FRESHNESS_INVALID")
    stages = body.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= 5:
        raise ValueError("PORT_GUARD_STAGES_MISSING")
    last_giveback = Decimal("0")
    last_target = Decimal("0")
    for stage in stages:
        if not isinstance(stage, dict):
            raise ValueError("PORT_GUARD_STAGE_INVALID")
        giveback = _decimal(stage.get("giveback_fraction"))
        reduction = _decimal(stage.get("cumulative_reduction_fraction"))
        if not (last_giveback < giveback < 1 and last_target < reduction <= 1):
            raise ValueError("PORT_GUARD_STAGE_MONOTONICITY_INVALID")
        last_giveback, last_target = giveback, reduction
    if body.get("rearm_mode") != "OPERATOR_SIGNED_NEW_EPISODE":
        raise ValueError("PORT_GUARD_REARM_POLICY_UNVERIFIED")
    return body, policy_digest(authorization)


def verified_sleeve_pnl(
    fills: list[dict], positions: list[dict], *,
    snapshot_at: datetime, account_hash: str, trading_epoch_id: str,
    quote_ttl_sec: Decimal, max_skew_sec: Decimal,
) -> tuple[Decimal, dict]:
    """Strict KIS-native fill economics, no arbitrary cash allocation.

    Every original BUY/SELL fill is cumulative-broker-confirmed, unique, and
    scope-bound. Position quantities must equal native BUY-SELL net for each
    US_STANDARD symbol. Missing/stale/duplicated marks fail closed; never
    convert an unresolved order into a settled fill.
    """
    if snapshot_at.tzinfo is None:
        raise ValueError("PORT_GUARD_SNAPSHOT_NAIVE")
    if not fills:
        raise ValueError("PORT_GUARD_NATIVE_LEDGER_EMPTY")
    book: dict[str, dict] = {}
    unique: set[tuple] = set()
    points = []
    buy_total = sell_total = Decimal("0")
    for f in fills:
        if (str(f.get("strategy_owner") or "") != "US_STANDARD"
                or str(f.get("symbol") or "").upper() == "TQQQ"
                or f.get("account_hash") != account_hash
                or f.get("trading_epoch_id") != trading_epoch_id
                or f.get("fill_evidence_type") != "KIS_ORDER_CUMULATIVE_ACTUAL"
                or f.get("is_synthetic") is True):
            raise ValueError("PORT_GUARD_FILL_SCOPE_OR_SOURCE_INVALID")
        side = str(f.get("side") or "").upper()
        sym = str(f.get("symbol") or "").upper()
        order = str(f.get("order_no") or "")
        client = str(f.get("client_order_key") or "")
        day = str(f.get("trade_date") or "")
        if side not in {"BUY", "SELL"} or not (sym and order and client and day):
            raise ValueError("PORT_GUARD_FILL_IDENTITY_INCOMPLETE")
        qty = _decimal(f.get("qty"))
        price = _decimal(f.get("price_usd"))
        if qty <= 0 or qty != qty.to_integral_value() or price <= 0:
            raise ValueError("PORT_GUARD_FILL_ECONOMICS_INVALID")
        identity = (sym, side, day, order, client, qty, price)
        if identity in unique:
            raise ValueError("PORT_GUARD_DUPLICATE_NATIVE_FILL")
        unique.add(identity)
        slot = book.setdefault(sym, {"net_qty": Decimal("0"), "buys": Decimal("0"), "sells": Decimal("0")})
        amount = qty * price
        if side == "BUY":
            slot["net_qty"] += qty
            slot["buys"] += amount
            buy_total += amount
        else:
            slot["net_qty"] -= qty
            slot["sells"] += amount
            sell_total += amount
        if slot["net_qty"] < 0:
            raise ValueError("PORT_GUARD_NEGATIVE_NATIVE_POSITION")
    holdings = {}
    for p in positions:
        if p.get("strategy_owner") != "US_STANDARD" or str(p.get("symbol") or "").upper() == "TQQQ":
            raise ValueError("PORT_GUARD_POSITION_OWNER_INVALID")
        if p.get("account_hash") != account_hash or p.get("trading_epoch_id") != trading_epoch_id:
            raise ValueError("PORT_GUARD_POSITION_SCOPE_INVALID")
        sym = str(p.get("symbol") or "").upper()
        if not sym or sym in holdings:
            raise ValueError("PORT_GUARD_AMBIGUOUS_POSITION")
        qty = _decimal(p.get("holding_qty"))
        orderable = _decimal(p.get("orderable_qty"))
        if (qty < 0 or qty != qty.to_integral_value()
                or orderable < 0 or orderable > qty or orderable != orderable.to_integral_value()):
            raise ValueError("PORT_GUARD_KIS_QUANTITY_INVALID")
        mark = _decimal(p.get("mark_bid_usd"))
        if mark <= 0 or p.get("mark_source") not in ("KIS_BID", "KIS_EXECUTABLE_QUOTE"):
            raise ValueError("PORT_GUARD_NATIVE_MARK_INVALID")
        asof = _utc(p.get("quote_asof_utc"))
        age = Decimal(str((snapshot_at.astimezone(timezone.utc) - asof).total_seconds()))
        if age < 0 or age > quote_ttl_sec:
            raise ValueError("PORT_GUARD_MARK_STALE")
        if p.get("snapshot_native_kis") is not True:
            raise ValueError("PORT_GUARD_BALANCE_NOT_NATIVE")
        points.append(asof)
        holdings[sym] = {"qty": qty, "orderable": orderable, "mark": mark}
    if not points or (max(points) - min(points)).total_seconds() > max_skew_sec:
        raise ValueError("PORT_GUARD_SNAPSHOT_SKEW")
    if set(book) != set(holdings):
        # Closed positions must be represented as qty=0 native rows so we can
        # prove full-history net and no hidden owner/IPO/transfer mismatch.
        raise ValueError("PORT_GUARD_UNACCOUNTED_POSITION")
    marked = Decimal("0")
    for sym, amounts in book.items():
        if amounts["net_qty"] != holdings[sym]["qty"]:
            raise ValueError("PORT_GUARD_KIS_LEDGER_NET_MISMATCH")
        marked += amounts["net_qty"] * holdings[sym]["mark"]
    pnl = sell_total + marked - buy_total
    return pnl, {"buy_total": buy_total, "sell_total": sell_total,
                 "marked_open_equity": marked,
                 "method": "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1"}


def triggered_stage(*, pnl: Decimal, hwm: Decimal, policy: dict) -> int:
    """Return highest threshold stage, 0=WATCH, without mutating HWM."""
    if hwm <= 0 or hwm < _decimal(policy.get("activation_profit_usd")):
        return 0
    giveback = (hwm - pnl) / hwm
    stage = 0
    for idx, setting in enumerate(policy["stages"], start=1):
        if giveback >= _decimal(setting["giveback_fraction"]):
            stage = idx
    return stage


def plan_verified_guard_sell(
    positions: list[dict], *, policy: dict, stage_no: int,
    reference_exposure_usd: Decimal, already_reduced_usd: Decimal,
    excluded_symbols: set[str],
) -> list[dict]:
    """Deterministic integer-share plan from FIXED episode exposure.

    No order authority is produced. A durable reservation and canonical
    broker-submit gate are required before routing this output.
    """
    if stage_no < 1 or stage_no > len(policy["stages"]):
        return []
    stage = policy["stages"][stage_no - 1]
    remaining_target = max(
        Decimal("0"),
        reference_exposure_usd * _decimal(stage["cumulative_reduction_fraction"])
        - already_reduced_usd,
    )
    if remaining_target <= 0:
        return []
    candidates = []
    for p in positions:
        sym = str(p.get("symbol") or "").upper()
        if sym in excluded_symbols or sym == "TQQQ" or p.get("strategy_owner") != "US_STANDARD":
            continue
        if not p.get("position_cycle_id") or p.get("unresolved_sell") is True:
            continue
        qty = int(_decimal(p.get("orderable_qty")))
        price = _decimal(p.get("mark_bid_usd"))
        entry = _decimal(p.get("verified_entry_usd"))
        if qty > 0 and price > 0 and entry > 0:
            candidates.append(((price / entry - 1), sym, p, qty, price))
    candidates.sort(key=lambda row: (row[0], row[1]))
    plans = []
    for _, sym, p, available, price in candidates:
        if remaining_target <= 0:
            break
        # Never exceed policy cumulative target or KIS orderable quantity.
        count = min(available, int(remaining_target / price))
        if count <= 0:
            continue
        plans.append({
            "symbol": sym, "qty": count, "position_cycle_id": p["position_cycle_id"],
            "reference_price_usd": str(price),
            "planned_reduced_usd": str(count * price),
        })
        remaining_target -= count * price
    return plans
