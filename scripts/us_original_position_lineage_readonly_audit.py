#!/usr/bin/env python3
"""Read-only US original portfolio/cycle provenance triage.

Reports the *missing pieces* for each current holding. This is NOT a lineage
repair and NEVER infers original portfolio identity from today's symbol,
balance, current epoch, or any newly created portfolio record.

Run only on the authorized practice WSL host. No KIS order/cancel endpoint,
UPDATE, migration, broker data mutation, or claim release is called.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from typing import Any

from sqlalchemy import text

SYMBOLS = ("ABBV", "DHR", "HCA", "KO", "KR", "MCK", "PFE", "PG", "TMO")

# Postgres-specific SELECT; values passed as bind parameters. The row exposes
# only scoped counts and presence flags, not account IDs, client keys, order
# numbers, raw payloads, or potentially identifying immutable contract hashes.
READONLY_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (p.symbol)
        p.symbol, p.qty, p.as_of, p.account_id, p.trading_epoch_id,
        p.portfolio_epoch_id, p.position_cycle_id, p.meta
    FROM public.us_positions p
    WHERE p.symbol = ANY(:symbols) AND p.as_of <= :trade_date
    ORDER BY p.symbol, p.as_of DESC, p.created_at DESC
)
SELECT p.symbol, p.qty, p.as_of,
    (p.account_id = :account) AS account_matches,
    (p.account_id IS NULL) AS account_absent,
    (p.trading_epoch_id = :epoch) AS epoch_matches,
    (p.portfolio_epoch_id IS NOT NULL) AS portfolio_present,
    (p.position_cycle_id IS NOT NULL) AS cycle_present,
    (COALESCE(p.meta->>'strategy_owner', '') <> '') AS owner_present,
    (SELECT COUNT(*) FROM public.us_orders o
        WHERE o.symbol=p.symbol AND o.side='BUY' AND o.dry_run IS FALSE
          AND o.env='practice' AND o.account_id=:account
          AND o.trading_epoch_id=:epoch) AS scoped_buy_orders,
    (SELECT COUNT(*) FROM public.us_fills f
        WHERE f.symbol=p.symbol AND f.side='BUY'
          AND f.account_id=:account AND f.trading_epoch_id=:epoch
    ) AS scoped_buy_fills,
    (SELECT COUNT(*) FROM public.us_orders o
        WHERE o.symbol=p.symbol AND o.side='BUY' AND o.dry_run IS FALSE
          AND o.env='practice' AND o.trading_epoch_id=:epoch
          AND o.account_id IS NULL
    ) AS legacy_unscoped_buy_orders,
    (SELECT COUNT(*) FROM public.us_fills f
        WHERE f.symbol=p.symbol AND f.side='BUY'
          AND f.trading_epoch_id=:epoch AND f.account_id IS NULL
    ) AS legacy_unscoped_buy_fills,
    (SELECT COUNT(*) FROM public.us_position_risk_state r
        WHERE r.symbol=p.symbol AND r.account_id=:account
          AND r.trading_epoch_id=:epoch AND r.trade_date <= :trade_date
          AND r.state ? 'lifecycle'
    ) AS scoped_legacy_lifecycle_rows
FROM latest p
ORDER BY p.symbol
"""


def classify_readonly_candidate(row: dict[str, Any]) -> dict[str, Any]:
    """Never use a DB-only candidate to authorize a SELL or amend a claim."""
    symbol = str(row.get("symbol") or "").upper()
    if symbol not in SYMBOLS:
        raise ValueError("US_LINEAGE_SYMBOL_NOT_IN_ALLOWLIST")
    # A NULL legacy account column is *absence of provenance*, not proof
    # that the original order belonged to a different broker account.
    if row.get("account_absent") is True:
        reason = "LEGACY_ORIGINAL_ACCOUNT_SCOPE_ABSENT"
    elif row.get("account_matches") is not True:
        reason = "ORIGINAL_ACCOUNT_SCOPE_MISMATCH_OR_UNKNOWN"
    elif row.get("epoch_matches") is not True:
        reason = "TRADING_EPOCH_MISMATCH"
    elif (row.get("portfolio_present") is not True
          or row.get("cycle_present") is not True):
        reason = "ORIGINAL_PORTFOLIO_CYCLE_UNVERIFIED"
    elif row.get("owner_present") is not True:
        reason = "STRATEGY_OWNER_UNVERIFIED"
    elif (int(row.get("scoped_buy_orders") or 0) == 0
          or int(row.get("scoped_buy_fills") or 0) == 0):
        reason = "ORIGINAL_BUY_ATTEMPT_OR_FILL_UNVERIFIED"
    else:
        reason = "CANDIDATE_NEEDS_KIS_AND_FROZEN_CONTRACT_PROOF"
    return {
        "symbol": symbol,
        "qty_db_only": row.get("qty"),
        "trade_date": str(row.get("as_of") or ""),
        "classification": reason,
        "evidence_status": "READONLY_DB_CANDIDATE_NOT_BROKER_PROOF",
        "scoped_buy_orders": int(row.get("scoped_buy_orders") or 0),
        "scoped_buy_fills": int(row.get("scoped_buy_fills") or 0),
        # Historical legacy BUY rows are only leads for a KIS original-date
        # probe; they can NEVER substitute for account-scoped broker proof.
        "legacy_unscoped_buy_orders": int(row.get("legacy_unscoped_buy_orders") or 0),
        "legacy_unscoped_buy_fills": int(row.get("legacy_unscoped_buy_fills") or 0),
        "scoped_legacy_lifecycle_rows": int(
            row.get("scoped_legacy_lifecycle_rows") or 0),
        "auto_identity_rebind_allowed": False,
        "claims_released": 0,
        "orders_submitted": 0,
        "db_rows_modified": 0,
    }


def audit_readonly(engine: Any, *, account_fingerprint: str,
                   trading_epoch_id: str, trade_date: str) -> dict:
    if (not account_fingerprint or not trading_epoch_id
            or len(account_fingerprint) != 64):
        raise RuntimeError("US_LINEAGE_ACCOUNT_EPOCH_UNVERIFIED")
    date.fromisoformat(trade_date)
    with engine.connect() as conn:
        # First SQL statement on this connection: enforce PostgreSQL
        # READ ONLY for the remainder of the transaction, even if refactored.
        conn.execute(text("SET TRANSACTION READ ONLY"))
        rows = conn.execute(text(READONLY_SQL), {
            "symbols": list(SYMBOLS),
            "trade_date": trade_date,
            "account": account_fingerprint,
            "epoch": trading_epoch_id,
        }).mappings().all()
    candidates = [classify_readonly_candidate(dict(row)) for row in rows]
    known = {row["symbol"] for row in candidates}
    for symbol in SYMBOLS:
        if symbol not in known:
            candidates.append({
                "symbol": symbol, "classification": "NO_CURRENT_POSITION_ROW",
                "evidence_status": "READONLY_DB_CANDIDATE_NOT_BROKER_PROOF",
                "auto_identity_rebind_allowed": False,
                "claims_released": 0, "orders_submitted": 0,
                "db_rows_modified": 0,
            })
    candidates.sort(key=lambda row: row["symbol"])
    return {
        "status": "ORIGINAL_LINEAGE_UNVERIFIED",
        "trade_date": trade_date,
        "positions_audited": len(candidates),
        "candidates": candidates,
        "kis_original_order_verified": False,
        "frozen_buy_contract_verified": False,
        "scope_rebindings": 0,
        "claims_released": 0,
        "orders_submitted": 0,
        "db_rows_modified": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--confirm-practice-readonly", action="store_true")
    args = parser.parse_args()
    try:
        date.fromisoformat(args.trade_date)
    except ValueError:
        parser.error("trade-date must be YYYY-MM-DD")
    if (not args.confirm_practice_readonly
            or os.getenv("KIS_ENV", "").lower() != "practice"
            or os.getenv("WSL_RUN_MARKET", "").upper() != "US"):
        print(json.dumps({"status": "PRACTICE_US_READONLY_REQUIRED",
                          "orders_submitted": 0, "db_rows_modified": 0}))
        return 42
    try:
        from trader.us.db.repos import (
            _get_engine_or_none, _us_execution_claim_scope,
        )
        scope = _us_execution_claim_scope()
        if scope.get("env") != "practice" or scope.get("market") != "US":
            raise RuntimeError("US_LINEAGE_SCOPE_NOT_PRACTICE")
        engine = _get_engine_or_none()
        if engine is None:
            raise RuntimeError("US_LINEAGE_DB_UNAVAILABLE")
        result = audit_readonly(
            engine, account_fingerprint=scope["account_id"],
            trading_epoch_id=scope["trading_epoch_id"],
            trade_date=args.trade_date,
        )
    except Exception as exc:
        result = {
            "status": "LINEAGE_READONLY_FAILED",
            "error_type": type(exc).__name__,
            "orders_submitted": 0, "db_rows_modified": 0,
        }
        print(json.dumps(result, sort_keys=True))
        return 43
    print(json.dumps(result, sort_keys=True, default=str))
    # A read-only DB-only candidate is NEVER sufficient for order authority.
    return 44


if __name__ == "__main__":
    raise SystemExit(main())
