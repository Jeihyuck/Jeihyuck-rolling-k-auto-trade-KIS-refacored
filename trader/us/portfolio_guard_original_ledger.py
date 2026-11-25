"""Verify historical account-epoch economics without rewriting original BUY scope.

Only a separately signed trading-PnL policy may opt in to pre-portfolio
history. Every archived fill must match one actual practice order and the
original V2 BUY plus its account-bound historical runtime lifecycle.
The prospective management cycle is an in-memory economic grouping, never
a retroactive portfolio/cycle assignment to a historical fill. This module
does not authorize, reserve, submit or mutate any order or original row.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import date
from typing import Any

from sqlalchemy import text

from trader.us.entry_exit_contract import (
    US_ENTRY_EXIT_CONTRACT_VERSION, verify_us_entry_exit_contract,
)
from trader.us.portfolio_profit_guard import _decimal
from trader.us.utils.order_no import normalize_us_order_no

LEGACY_ACCOUNTING_SCOPE = "VERIFIED_ORIGINAL_ACCOUNT_EPOCH_V1"
_FORWARD_VERSION = "US_LEGACY_NINE_V2_SELL_V1"
_SCOPE_FIELDS = ("account_id", "portfolio_epoch_id", "position_cycle_id")


def _meta(row: dict) -> dict:
    return row["meta"] if isinstance(row.get("meta"), dict) else {}


def _scope_count(row: dict) -> int:
    return sum(bool(str(row.get(field) or "").strip()) for field in _SCOPE_FIELDS)


def legacy_economic_row(row: dict) -> bool:
    return _scope_count(row) != len(_SCOPE_FIELDS) or (
        _meta(row).get("forward_management_not_original_buy") is True
    )


def fill_key(row: dict) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("symbol") or "").upper(), str(row.get("side") or "").upper(),
        str(row.get("trade_date") or ""), normalize_us_order_no(row.get("order_no")),
        str(row.get("client_order_key") or ""),
    )


def _require(ok: Any, reason: str) -> None:
    if not ok:
        raise ValueError("PORT_GUARD_LEGACY_" + reason)


def _native_scope(row: dict, *, raw_account: str, epoch: str) -> bool:
    account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
    if str(row.get("trading_epoch_id") or "") != epoch:
        return False
    for source in (row, _meta(row)):
        if any(str(source.get(field) or "") not in ("", raw_account, account_hash)
               for field in ("account_id", "account_key", "broker_account_hash")):
            return False
        if str(source.get("trading_epoch_id") or "") not in ("", epoch):
            return False
        if str(source.get("env") or "") not in ("", "practice"):
            return False
        if str(source.get("strategy_owner") or "") not in ("", "US_STANDARD"):
            return False
        if str(source.get("sleeve_id") or "") not in ("", "US_STANDARD"):
            return False
    m = _meta(row)
    return (
        m.get("strategy_owner") == "US_STANDARD"
        and m.get("fill_evidence_type") == "KIS_ORDER_CUMULATIVE_ACTUAL"
        and m.get("is_synthetic") is False
        and m.get("synthetic") is not True
        and m.get("accounting_active") is not False
    )


def _matching_native_order(fill: dict, candidates: list[dict], *, raw_account: str, epoch: str) -> dict:
    key = fill_key(fill)
    _require(all(key) and key[1] in ("BUY", "SELL"), "FILL_KEY_INVALID")
    _require(len(candidates) == 1, "ORIGINAL_ORDER_NOT_UNIQUE")
    order = candidates[0]
    _require(fill_key(order) == key and order.get("env") == "practice"
             and order.get("dry_run") is False, "ORDER_SCOPE_INVALID")
    _require(_native_scope(fill, raw_account=raw_account, epoch=epoch)
             and _native_scope(order, raw_account=raw_account, epoch=epoch),
             "NATIVE_EVIDENCE_INVALID")
    qty = _decimal(fill.get("qty"))
    price = _decimal(fill.get("price_usd"))
    requested = _decimal(order.get("qty_requested"))
    _require(qty > 0 and qty == qty.to_integral_value() and price > 0
             and requested >= qty and requested == requested.to_integral_value()
             and qty == _decimal(order.get("qty_filled"))
             and price == _decimal(order.get("avg_price_usd")),
             "ORIGINAL_ORDER_ECONOMICS_MISMATCH")
    _require(str(order.get("status") or "").upper() in
             ("FILLED", "RECONCILED", "CANCELLED", "CANCELED"),
             "ORIGINAL_ORDER_NOT_TERMINAL")
    _require(_meta(fill).get("position_lifecycle_id")
             and _meta(fill).get("position_lifecycle_id") == _meta(order).get("position_lifecycle_id"),
             "ORDER_FILL_LIFECYCLE_MISMATCH")
    for field in _SCOPE_FIELDS:
        _require(str(fill.get(field) or "") == str(order.get(field) or ""),
                 "ORDER_FILL_SCOPE_MISMATCH")
    return order


def legacy_management_cycle(*, raw_account: str, epoch: str, portfolio: str,
                            original_cycle: str, symbol: str, contract_sha: str) -> str:
    """Same forward-only identity used by the existing legacy SELL router."""
    _require(all((raw_account, epoch, portfolio, original_cycle, symbol, contract_sha)),
             "FORWARD_SCOPE_UNVERIFIED")
    material = "|".join((
        hashlib.sha256(raw_account.encode()).hexdigest(), epoch, portfolio,
        original_cycle, symbol, contract_sha, "FORWARD_ONLY_V2_LEGACY_SELL_V1",
    ))
    return hashlib.sha256(material.encode()).hexdigest()[:32]


def prove_legacy_epoch_fills(
    fills: list[dict], orders: list[dict], histories: list[dict], *,
    raw_account: str, epoch: str, portfolio: str, portfolio_start_day: date,
) -> tuple[dict, dict]:
    """Return exact in-memory fill-key -> proven group and symbol -> group.

    A closed original group and a later canonical BUY of the same ticker are
    separate economic cycles. Current canonical positions are never inferred
    to belong to the archived group by ticker. Partial historical scope,
    ambiguous original contracts, or negative quantity fail closed.
    """
    from trader.us.strategy_ownership import is_standard_symbol
    _require(all((raw_account, epoch, portfolio)), "ACCOUNT_EPOCH_REQUIRED")
    account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
    groups = defaultdict(list)
    for row in fills:
        if not legacy_economic_row(row):
            continue
        symbol = str(row.get("symbol") or "").upper()
        _require(is_standard_symbol(symbol) and _meta(row).get("strategy_owner") == "US_STANDARD",
                 "OWNER_INVALID")
        _require(_scope_count(row) in (0, 3), "PARTIAL_SCOPE_UNVERIFIED")
        groups[symbol].append(row)
    order_map = defaultdict(list)
    for order in orders:
        order_map[fill_key(order)].append(order)
    keyed, proven = {}, {}
    for symbol, rows in sorted(groups.items()):
        original_buys = [f for f in rows if f.get("side") == "BUY" and _scope_count(f) == 0]
        _require(len(original_buys) == 1, "ORIGINAL_BUY_NOT_UNIQUE")
        buy = original_buys[0]
        buy_key = fill_key(buy)
        buy_order = _matching_native_order(buy, order_map[buy_key], raw_account=raw_account, epoch=epoch)
        buy_day = buy_key[2]
        _require(date.fromisoformat(buy_day) < portfolio_start_day, "BUY_NOT_PREPORTFOLIO")
        bm, om = _meta(buy), _meta(buy_order)
        contract = bm.get("entry_exit_contract")
        digest = str(bm.get("entry_exit_contract_sha256") or "")
        _require(isinstance(contract, dict) and contract == om.get("entry_exit_contract")
                 and contract.get("version") == US_ENTRY_EXIT_CONTRACT_VERSION
                 and contract.get("strategy_owner") == "US_STANDARD"
                 and contract.get("sha256") == digest == om.get("entry_exit_contract_sha256")
                 and verify_us_entry_exit_contract(contract)
                 and bm.get("position_lifecycle_id") == "ENTRY:US_STANDARD:" + symbol
                 and _decimal(buy.get("qty")) == _decimal(buy_order.get("qty_requested")),
                 "ORIGINAL_BUY_CONTRACT_INVALID")
        source_histories = [
            row["lifecycle"] for row in histories
            if str(row.get("symbol") or "").upper() == symbol
            and str(row.get("trading_epoch_id") or "") == epoch
            and isinstance(row.get("lifecycle"), dict)
            and str(row["lifecycle"].get("opened_trade_date") or "") == buy_day
        ]
        _require(source_histories, "ORIGINAL_RUNTIME_HISTORY_MISSING")
        cycles = set()
        for h in source_histories:
            life = str(h.get("lifecycle_id") or "")
            hsha = (h.get("entry_policy") or {}).get("entry_exit_contract_sha256")
            _require(len(life) >= 20 and h.get("account_id") == raw_account
                     and h.get("env") == "practice" and h.get("strategy_owner") == "US_STANDARD"
                     and str(h.get("trading_epoch_id") or "") == epoch
                     and not h.get("portfolio_epoch_id") and not h.get("position_cycle_id")
                     and hsha in (None, "", digest), "ORIGINAL_RUNTIME_ACCOUNT_OR_CYCLE_CONFLICT")
            cycles.add(life)
        _require(len(cycles) == 1 and any(h.get("is_open") is True for h in source_histories),
                 "ORIGINAL_RUNTIME_CYCLE_NOT_UNIQUE")
        original = next(iter(cycles))
        management = legacy_management_cycle(
            raw_account=raw_account, epoch=epoch, portfolio=portfolio,
            original_cycle=original, symbol=symbol, contract_sha=digest,
        )
        net = 0
        group = dict(
            symbol=symbol, original_cycle=original, management_cycle=management,
            buy_day=buy_day, buy_order_no=str(buy["order_no"]),
            contract_sha=digest, contract=contract,
            buy_qty=int(_decimal(buy["qty"])),
        )
        for fill in rows:
            key = fill_key(fill)
            _require(key not in keyed, "DUPLICATE_NATIVE_FILL")
            order = _matching_native_order(fill, order_map[key], raw_account=raw_account, epoch=epoch)
            m = _meta(fill)
            _require(key[2] >= buy_day, "FILL_BEFORE_ORIGINAL_BUY")
            if fill is buy:
                _require(_scope_count(fill) == 0 and not m.get("forward_management_not_original_buy"),
                         "ORIGINAL_BUY_SCOPE_CONFLICT")
                net += int(_decimal(fill["qty"]))
            else:
                _require(key[1] == "SELL", "NON_ORIGINAL_LEGACY_BUY")
                if _scope_count(fill) == 0:
                    for row in (fill, order):
                        rm = _meta(row)
                        _require(rm.get("position_lifecycle_id") == original
                                 and rm.get("source_entry_contract_sha256") in (None, "", digest)
                                 and not rm.get("forward_management_not_original_buy")
                                 and not rm.get("portfolio_epoch_id") and not rm.get("position_cycle_id"),
                                 "ARCHIVED_SELL_SOURCE_CONFLICT")
                else:
                    required = {
                        "source_original_runtime_lifecycle_id": original,
                        "source_original_buy_date": buy_day,
                        "source_original_entry_exit_contract_sha256": digest,
                        "us_legacy_forward_management_version": _FORWARD_VERSION,
                        "position_lifecycle_id": management,
                        "position_cycle_id": management,
                        "portfolio_epoch_id": portfolio,
                        "broker_account_hash": account_hash,
                        "trading_epoch_id": epoch,
                    }
                    for row in (fill, order):
                        rm = _meta(row)
                        _require(str(row.get("account_id") or "") == account_hash
                                 and str(row.get("portfolio_epoch_id") or "") == portfolio
                                 and str(row.get("position_cycle_id") or "") == management
                                 and rm.get("forward_management_not_original_buy") is True
                                 and rm.get("automated_sell_authorized") is True
                                 and normalize_us_order_no(rm.get("source_original_buy_order_no")) == buy_key[3]
                                 and all(str(rm.get(k) or "") == str(v) for k, v in required.items()),
                                 "FORWARD_SELL_SOURCE_CONFLICT")
                net -= int(_decimal(fill["qty"]))
            keyed[key] = group
        _require(net >= 0, "ORIGINAL_GROUP_NEGATIVE_NET")
        group["net_qty"] = net
        proven[symbol] = group
    return keyed, proven


def load_legacy_epoch_proof(conn, fills: list[dict], *, raw_account: str,
                            epoch: str, portfolio: str, trade_date: str) -> tuple[dict, dict]:
    symbols = sorted({str(row["symbol"]).upper() for row in fills if legacy_economic_row(row)})
    if not symbols:
        return {}, {}
    starts = conn.execute(text("""
        SELECT started_at FROM public.portfolio_epochs
        WHERE portfolio_epoch_id=:portfolio AND trading_epoch_id=:epoch
          AND account_id=:account AND env='practice' AND strategy='US_STANDARD'
          AND status='ACTIVE' AND sid=0 AND mode=0
    """), {"portfolio": portfolio, "epoch": epoch, "account": raw_account}).mappings().all()
    _require(len(starts) == 1 and starts[0]["started_at"] is not None, "PORTFOLIO_START_UNVERIFIED")
    started = starts[0]["started_at"]
    _require(getattr(started, "tzinfo", None) is not None, "PORTFOLIO_START_UNVERIFIED")
    params = {"symbols": symbols, "epoch": epoch, "day": trade_date}
    orders = conn.execute(text("""
        SELECT trade_date,symbol,side,qty_requested,qty_filled,avg_price_usd,
               order_no,client_order_key,env,dry_run,status,account_id,
               trading_epoch_id,portfolio_epoch_id,position_cycle_id,meta
        FROM public.us_orders
        WHERE trading_epoch_id=:epoch AND symbol=ANY(:symbols)
          AND env='practice' AND dry_run IS FALSE AND trade_date<=:day
    """), params).mappings().all()
    histories = conn.execute(text("""
        SELECT symbol,trading_epoch_id,state->'lifecycle' AS lifecycle
        FROM public.us_position_risk_state
        WHERE trading_epoch_id=:epoch AND symbol=ANY(:symbols) AND trade_date<=:day
          AND NULLIF(state->'lifecycle'->>'lifecycle_id','') IS NOT NULL
    """), params).mappings().all()
    return prove_legacy_epoch_fills(
        fills, [dict(r) for r in orders], [dict(r) for r in histories],
        raw_account=raw_account, epoch=epoch, portfolio=portfolio,
        portfolio_start_day=started.date(),
    )
