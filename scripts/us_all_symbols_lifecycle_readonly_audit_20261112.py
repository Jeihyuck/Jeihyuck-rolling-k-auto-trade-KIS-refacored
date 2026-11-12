#!/usr/bin/env python3
"""All US holdings: AM/Close, practice-only, read-only lifecycle evidence.

This is *not* a KIS-origin certification, operational SELL authorization,
historical rebinder, or repair tool. It never places an order, applies a
migration, synthesizes a fill or alters a position, claim, TP or epoch.

Usage (on authorized US practice WSL at AM start or Close):
  KIS_ENV=practice WSL_RUN_MARKET=US NO_TRADE=1 DRY_RUN=1 \\
    US_ORDER_ARMED=0 DISABLE_REAL_TRADING=1 \\
    .venv/bin/python -m scripts.us_all_symbols_lifecycle_readonly_audit_20261112 \\
      --as-of 2026-11-11 --confirm-practice-readonly
"""
from __future__ import annotations

import argparse
from datetime import date
from collections.abc import Mapping
import hashlib
import json
import os
from typing import Any

from sqlalchemy import text

from trader.us.entry_exit_contract import verify_us_entry_exit_contract


CURRENT_HOLDINGS_SQL = text("""
WITH latest AS (
    SELECT DISTINCT ON (p.symbol) p.symbol,p.as_of,p.qty,p.trading_epoch_id,
           p.account_id,p.portfolio_epoch_id,p.position_cycle_id,p.meta
      FROM us_positions p
     WHERE p.trading_epoch_id=:epoch AND p.as_of<=:as_of
     ORDER BY p.symbol,p.as_of DESC,p.created_at DESC
)
SELECT symbol,as_of,qty,account_id,portfolio_epoch_id,position_cycle_id,meta
  FROM latest WHERE qty>0 ORDER BY symbol
""")

# No account/order number, token, BUY contract body or raw KIS response
# appears in the returned user-facing report. The database is opened READ ONLY.
RISK_SQL = text("""
SELECT trade_date, state->'lifecycle' AS lifecycle
FROM us_position_risk_state
WHERE trading_epoch_id=:epoch AND symbol=:symbol AND trade_date<=:as_of
ORDER BY trade_date DESC,updated_at DESC LIMIT 1
""")

BUY_SOURCE_SQL = text("""
SELECT o.trade_date, o.account_id, o.portfolio_epoch_id, o.position_cycle_id,
       o.meta AS order_meta, i.meta AS intent_meta, f.meta AS fill_meta,
       f.qty AS filled_shares, f.order_no AS fill_order,
       o.order_no AS broker_order, o.status, o.qty_filled, o.qty_requested,
       i.qty AS intended_shares
FROM us_orders o
JOIN us_order_intents i ON i.client_order_key=o.client_order_key
    AND i.trading_epoch_id=o.trading_epoch_id AND i.trade_date=o.trade_date
JOIN us_fills f ON f.client_order_key=o.client_order_key
    AND f.trading_epoch_id=o.trading_epoch_id AND f.trade_date=o.trade_date
    AND f.side='BUY'
WHERE o.symbol=:symbol AND o.trading_epoch_id=:epoch AND o.side='BUY'
  AND o.trade_date<=:as_of AND o.dry_run IS FALSE
ORDER BY o.trade_date,o.client_order_key LIMIT 51
""")

PENDING_SELL_SQL = text("""
SELECT trading_epoch_id, status, side
FROM us_orders
WHERE symbol=:symbol AND trade_date<=:as_of AND side='SELL'
  AND status NOT IN ('FILLED','CANCELLED','REJECTED','BLOCKED','EXPIRED')
ORDER BY trade_date DESC LIMIT 101
""")


def _dict(value: Any) -> dict:
    return dict(value) if isinstance(value, Mapping) else {}


def _safe_original_buy_count(rows: list[dict], *, account_hash: str, contract_sha: str,
                             cycle: str, portfolio: str, epoch: str) -> int:
    """Count only exact, non-synthetic, canonical source rows.

    A KIS_ORDER_CUMULATIVE_ACTUAL DB record is not a fresh KIS read; this
    proof supports diagnosis only. Later partial fills and different lots
    require a separate unique original KIS certification before repair.
    """
    valid_keys: set[tuple[str, str]] = set()
    for r in rows:
        om = _dict(r.get("order_meta"))
        im = _dict(r.get("intent_meta"))
        fm = _dict(r.get("fill_meta"))
        contract = _dict(im.get("entry_exit_contract"))
        if not (verify_us_entry_exit_contract(contract)
                and contract.get("sha256") == contract_sha
                and str(r.get("account_id") or "") == account_hash
                and str(r.get("position_cycle_id") or "") == cycle
                and str(r.get("portfolio_epoch_id") or "") == portfolio
                and str(om.get("strategy_owner") or "") == "US_STANDARD"
                and str(fm.get("fill_evidence_type") or "") == "KIS_ORDER_CUMULATIVE_ACTUAL"
                and fm.get("is_synthetic") is not True
                and all(str(m.get("entry_exit_contract_sha256") or "") == contract_sha
                        for m in (om, im, fm))
                and str(r.get("fill_order") or "")
                and str(r.get("fill_order")) == str(r.get("broker_order"))
                and str(r.get("status") or "") in {"FILLED", "PARTIALLY_FILLED"}
                and r.get("filled_shares") is not None):
            continue
        valid_keys.add((str(r["trade_date"]), str(r["broker_order"])))
    return len(valid_keys)


def audit_all_positions(conn, *, as_of: str) -> dict:
    """Only call inside a PostgreSQL READ ONLY transaction."""
    epoch_rows = conn.execute(text("""
        SELECT trading_epoch_id,account_id FROM trading_epochs
        WHERE env='practice' AND status='ACTIVE'
    """)).mappings().all()
    if len(epoch_rows) != 1 or not epoch_rows[0].get("account_id"):
        raise RuntimeError("US_ACTIVE_PRACTICE_EPOCH_NOT_UNIQUE")
    epoch = str(epoch_rows[0]["trading_epoch_id"])
    account_hash = hashlib.sha256(str(epoch_rows[0]["account_id"]).encode()).hexdigest()
    positions = conn.execute(CURRENT_HOLDINGS_SQL, {
        "epoch": epoch, "as_of": as_of,
    }).mappings().all()

    findings = []
    for p in positions:
        symbol = str(p["symbol"] or "").upper()
        meta = _dict(p.get("meta"))
        owner = str(meta.get("strategy_owner") or "").upper()
        contract = _dict(meta.get("entry_exit_contract"))
        contract_valid = bool(contract and verify_us_entry_exit_contract(contract))
        digest = str(meta.get("entry_exit_contract_sha256") or "")
        signed_match = contract_valid and digest == contract["sha256"]
        cycle = str(p.get("position_cycle_id") or meta.get("position_cycle_id") or "")
        portfolio = str(p.get("portfolio_epoch_id") or meta.get("portfolio_epoch_id") or "")
        account_column = str(p.get("account_id") or "")
        account_meta = str(meta.get("broker_account_hash") or "")
        account_conflict = any(x and x != account_hash for x in (account_column, account_meta))
        column_conflict = (
            (bool(p.get("position_cycle_id")) and str(p["position_cycle_id"]) != cycle)
            or (bool(p.get("portfolio_epoch_id")) and str(p["portfolio_epoch_id"]) != portfolio)
        )
        risks = conn.execute(RISK_SQL, {
            "epoch": epoch, "symbol": symbol, "as_of": as_of,
        }).mappings().all()
        lifecycle = _dict(_dict(risks[0]).get("lifecycle")) if risks else {}
        risk_matches = bool(
            owner == "US_STANDARD" and cycle and portfolio
            and lifecycle.get("is_open") is True
            and str(lifecycle.get("lifecycle_id") or "") == cycle
            and str(lifecycle.get("position_cycle_id") or "") == cycle
            and str(lifecycle.get("portfolio_epoch_id") or "") == portfolio
            and str(lifecycle.get("trading_epoch_id") or "") == epoch
            and str(lifecycle.get("account_id") or "") == str(epoch_rows[0]["account_id"])
            and str(_dict(lifecycle.get("entry_policy")).get("entry_exit_contract_sha256") or "")
                == digest
        )
        buy_rows = conn.execute(BUY_SOURCE_SQL, {
            "epoch": epoch, "symbol": symbol, "as_of": as_of,
        }).mappings().all() if owner == "US_STANDARD" else []
        source_count = (_safe_original_buy_count(
            [dict(r) for r in buy_rows], account_hash=account_hash,
            contract_sha=digest, cycle=cycle, portfolio=portfolio, epoch=epoch,
        ) if len(buy_rows) < 51 else 0)
        opens = conn.execute(PENDING_SELL_SQL, {
            "symbol": symbol, "as_of": as_of,
        }).mappings().all()
        same_epoch_open = sum(str(r.get("trading_epoch_id") or "") == epoch for r in opens)
        other_epoch_open = len(opens) - same_epoch_open

        if symbol == "TQQQ":
            status = "MANUAL_ACTION"
            reason = "DEDICATED_INFINITE_STATE_AND_NATIVE_KIS_PROOF_REQUIRED"
        elif account_conflict or column_conflict:
            status, reason = "CONFLICT", "ORIGINAL_ACCOUNT_OR_CYCLE_CONFLICT"
        elif not (owner == "US_STANDARD" and signed_match and cycle and portfolio):
            status, reason = "UNVERIFIED", "MISSING_OR_INVALID_ORIGINAL_CONTRACT_IDENTITY"
        elif not risk_matches or source_count != 1:
            status, reason = "UNVERIFIED", "RISK_LIFECYCLE_OR_EXACT_ORIGINAL_BUY_NOT_PROVEN"
        elif same_epoch_open or other_epoch_open:
            status, reason = "MANUAL_ACTION", "OPEN_SELL_NEEDS_ORIGINAL_BROKER_STATUS"
        elif p.get("account_id") is None or p.get("position_cycle_id") is None:
            status, reason = "UNVERIFIED", "ORIGINAL_DB_SOURCE_CANDIDATE_NEEDS_FRESH_KIS_PROOF"
        else:
            status, reason = "UNVERIFIED", "DB_CANDIDATE_NEEDS_FRESH_KIS_BALANCE_PROOF"
        findings.append({
            "symbol": symbol, "qty_db_snapshot": int(p["qty"]),
            "snapshot_date": str(p["as_of"]),
            "owner_db": owner or "UNRESOLVED",
            "strategy_db": str(meta.get("entry_style_selected") or "UNRESOLVED"),
            "contract_version": str(contract.get("version") or "UNRESOLVED"),
            "identity_columns_complete": all(p.get(x) is not None for x in
                                              ("account_id", "portfolio_epoch_id", "position_cycle_id")),
            "frozen_contract_valid": signed_match,
            "risk_matches_source": risk_matches,
            "original_db_buy_candidates": source_count,
            "open_sell_current_epoch": same_epoch_open,
            "open_sell_other_or_unknown_epoch": other_epoch_open,
            "classification": status, "reason": reason,
            "current_broker_balance_verification": "NOT_VERIFIED",
            "broker_order_original_day_verification": "NOT_VERIFIED",
            "automated_selling_authorized_by_this_audit": False,
        })

    return {
        "scope": "ACTIVE_US_PRACTICE_ONLY",
        "as_of": as_of, "positions_audited": len(findings),
        "status": "DB_ONLY_KIS_PROOF_REQUIRED",
        "positions": findings, "db_rows_modified": 0,
        "orders_submitted": 0, "claims_released": 0,
        "fresh_kis_read": False, "read_only": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--confirm-practice-readonly", action="store_true")
    args = parser.parse_args()
    try:
        date.fromisoformat(args.as_of)
    except ValueError:
        parser.error("as-of must be YYYY-MM-DD")
    if (not args.confirm_practice_readonly or
            os.getenv("KIS_ENV") != "practice" or
            os.getenv("WSL_RUN_MARKET", "").upper() != "US" or
            os.getenv("US_ORDER_ARMED", "1") != "0" or
            os.getenv("NO_TRADE") != "1" or
            os.getenv("DRY_RUN") != "1"):
        print(json.dumps({"status": "PRACTICE_US_READONLY_REQUIRED",
                          "db_rows_modified": 0, "orders_submitted": 0}))
        return 42
    try:
        from trader.us.db.repos import _get_engine_or_none
        engine = _get_engine_or_none()
        if engine is None:
            raise RuntimeError("DB_ENGINE_UNAVAILABLE")
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            report = audit_all_positions(conn, as_of=args.as_of)
    except Exception as exc:
        print(json.dumps({"status": "DB_READONLY_AUDIT_FAILED",
                          "error_type": type(exc).__name__,
                          "db_rows_modified": 0, "orders_submitted": 0}))
        return 43
    print(json.dumps(report, ensure_ascii=False, default=str, sort_keys=True))
    return 0 if report["positions_audited"] >= 0 else 44


if __name__ == "__main__":
    raise SystemExit(main())
