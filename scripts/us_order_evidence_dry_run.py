#!/usr/bin/env python3
"""Read-only, dry-run US order evidence/claim audit. Never mutates database.

Usage: python scripts/us_order_evidence_dry_run.py --from-date 2026-10-12 --to-date 2026-10-16
Prints symbols and statuses only; NEVER raw account numbers, credentials or order IDs.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from trader.us.db.repos import _get_engine_or_none
from sqlalchemy import text

READ_ONLY_SQL = """
SELECT o.trade_date, o.symbol, o.side, o.qty_requested, o.qty_filled,
       o.status AS order_status, a.attempt_state, a.authoritative,
       c.action_state, fproof.synthetic_count, fproof.broker_count
FROM us_orders o
LEFT JOIN us_execution_attempts a
  ON a.client_order_key = o.client_order_key
LEFT JOIN us_execution_claims c
  ON c.action_key = a.action_key
 AND c.trade_date = o.trade_date
LEFT JOIN LATERAL (
    SELECT count(*) FILTER (
      WHERE coalesce(f.meta->>'is_synthetic','false') = 'true'
         OR f.meta->>'fill_evidence_type' IN ('BALANCE_DELTA_SYNTHETIC','LEGACY_SYNTHETIC')
    ) AS synthetic_count,
    count(*) FILTER (
      WHERE f.meta->>'fill_evidence_type' IN (
       'KIS_ORDER_CUMULATIVE_ACTUAL', 'KIS_EXECUTION_ACTUAL')
       AND coalesce(f.meta->>'is_synthetic','false') <> 'true'
    ) AS broker_count
    FROM us_fills f
    WHERE f.trade_date = o.trade_date
      AND f.client_order_key = o.client_order_key
      AND f.symbol = o.symbol AND f.side = o.side
      AND coalesce(f.meta->>'accounting_active','true') <> 'false'
) fproof ON TRUE
WHERE o.trade_date BETWEEN :start_date AND :end_date
  AND o.dry_run IS DISTINCT FROM TRUE
ORDER BY o.trade_date,o.symbol,o.side,o.id
LIMIT 2000
"""

# Recovery inventory starts at the durable active attempt, not the orders
# table: missing orders must remain visible, and old/superseded attempts must
# not multiply the current claim. All identity filters are bound parameters.
ACTIVE_CLAIM_SQL = """
SELECT c.trade_date, o.symbol, o.side, a.requested_qty AS qty_requested,
       o.qty_filled, o.status AS order_status, a.attempt_state, a.authoritative,
       c.action_state, c.remaining_target_qty,
       count(f.id) FILTER (WHERE f.meta->>'fill_evidence_type' IN
         ('BALANCE_DELTA_SYNTHETIC','LEGACY_SYNTHETIC')
         OR coalesce(f.meta->>'is_synthetic','false')='true') AS synthetic_count,
       count(f.id) FILTER (WHERE f.meta->>'fill_evidence_type' IN
         ('KIS_ORDER_CUMULATIVE_ACTUAL','KIS_EXECUTION_ACTUAL')
         AND coalesce(f.meta->>'is_synthetic','false')<>'true') AS broker_count
FROM us_execution_claims c
JOIN us_execution_attempts a ON a.action_key=c.action_key
 AND a.attempt_id=c.active_attempt_id
LEFT JOIN us_orders o ON o.client_order_key=a.client_order_key
 AND o.trade_date=c.trade_date AND o.env=c.env
 AND o.trading_epoch_id=c.trading_epoch_id
LEFT JOIN us_fills f ON f.client_order_key=a.client_order_key
 AND f.trade_date=c.trade_date AND f.trading_epoch_id=c.trading_epoch_id
 AND f.symbol=o.symbol AND f.side=o.side
 AND coalesce(f.meta->>'accounting_active','true')<>'false'
WHERE c.env=:env AND c.account_id=:account_id AND c.market=:market
 AND c.trading_epoch_id=:trading_epoch_id AND c.active_attempt_id IS NOT NULL
 AND c.trade_date BETWEEN :start_date AND :end_date
GROUP BY c.action_key,c.trade_date,o.id,o.symbol,o.side,a.requested_qty,
         o.qty_filled,o.status,a.attempt_state,a.authoritative,
         c.action_state,c.remaining_target_qty
ORDER BY c.trade_date,o.symbol,c.action_key
LIMIT 2000
"""


def classify(row: dict) -> dict:
    synth = int(row.get("synthetic_count") or 0)
    broker = int(row.get("broker_count") or 0)
    order_filled = str(row.get("order_status") or "").upper() == "FILLED"
    claim = str(row.get("action_state") or "").upper()
    requires_review = bool(synth and not broker and order_filled)
    attempt_state = str(row.get("attempt_state") or "").upper()
    if requires_review:
        plan = "VERIFY_ORIGINAL_ORDER_WITH_ACCOUNT_SCOPED_KIS_EVIDENCE"
        defer_reason = "BALANCE_DELTA_IS_NOT_BROKER_EXECUTION_PROOF"
    elif broker and (claim in {"IN_FLIGHT", "UNCERTAIN"}
                     or attempt_state not in {"FILLED"}
                     or row.get("authoritative") is not True):
        plan = "VALIDATE_ORDER_ATTEMPT_IDENTITY_BEFORE_IDEMPOTENT_RECONCILIATION"
        defer_reason = "FILL_ROW_COUNT_ALONE_DOES_NOT_VERIFY_ATTEMPT_IDENTITY"
    elif broker:
        plan, defer_reason = "NO_STATE_MUTATION_NEEDED", "NONE"
    elif claim in {"IN_FLIGHT", "UNCERTAIN"} or str(row.get("order_status") or "").upper() in {"OPEN", "ACK", "PARTIALLY_FILLED"}:
        plan = "REQUERY_BROKER_ORIGINAL_TRADE_DATE_KEEP_CLAIM_FENCED"
        defer_reason = "BROKER_QUERY_ABSENCE_IS_NOT_TERMINAL_PROOF"
    else:
        plan, defer_reason = "MANUAL_REVIEW_NO_AUTOMATIC_REPAIR", "EVIDENCE_INCOMPLETE"
    return {
        "trade_date": str(row.get("trade_date") or ""),
        "symbol": str(row.get("symbol") or ""),
        "side": str(row.get("side") or ""),
        "requested_qty": row.get("qty_requested"),
        "order_filled_qty": row.get("qty_filled"),
        "order_status": str(row.get("order_status") or ""),
        "attempt_state": str(row.get("attempt_state") or ""),
        "claim_state": claim,
        "attempt_authoritative": row.get("authoritative"),
        "remaining_target_qty": row.get("remaining_target_qty"),
        "synthetic_evidence_count": synth,
        "broker_evidence_count": broker,
        "finding": "SYNTHETIC_ONLY_FILLED_ORDER" if requires_review else
                   "BROKER_EVIDENCE_PRESENT" if broker else
                   "AWAIT_BROKER_ORDER_PROOF" if claim in {"IN_FLIGHT","UNCERTAIN"} else "NO_ACTION",
        # Describe a potential future repair only; do not change DB state,
        # claims, PnL or the order's execution provenance in a dry-run.
        "proposed_change": "NONE_DRY_RUN",
        "proposed_change_plan": plan,
        "defer_reason": defer_reason,
        "release_fence": False,
        "verification_required": bool(requires_review or plan != "NO_STATE_MUTATION_NEEDED"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    ap.add_argument("--active-claims-only", action="store_true")
    args = ap.parse_args()
    start_date = date.fromisoformat(args.from_date)
    end_date = date.fromisoformat(args.to_date)
    if start_date > end_date:
        raise SystemExit("from-date must be <= to-date")
    engine = _get_engine_or_none()
    if engine is None:
        raise SystemExit("DATABASE_UNAVAILABLE: dry-run cannot establish evidence")
    params = {"start_date": start_date, "end_date": end_date}
    query = READ_ONLY_SQL
    if args.active_claims_only:
        from trader.us.db.repos import _us_execution_claim_scope
        scope = _us_execution_claim_scope()
        if any(not scope.get(key) for key in ('env','account_id','market','trading_epoch_id')):
            raise SystemExit("CLAIM_SCOPE_UNAVAILABLE: refusing unscoped active-claim audit")
        params.update(scope)
        query = ACTIVE_CLAIM_SQL
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            rows = conn.execute(text(query), params).mappings().all()
    print(json.dumps({"dry_run": True, "no_mutations": True,
                      "active_claims_only": args.active_claims_only,
                      "date_from": str(start_date), "date_to": str(end_date),
                      "rows": [classify(dict(row)) for row in rows]},
                     ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
