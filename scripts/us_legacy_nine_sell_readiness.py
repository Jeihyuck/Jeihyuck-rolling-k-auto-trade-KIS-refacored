#!/usr/bin/env python3
"""US practice legacy-nine SELL readiness: actionable native KIS GET only.

NEVER places, cancels, retries, approves or attributes an order. This is an
operator preflight for direct broker/manual SELL while old portfolio-cycle IDs
remain missing. It does not invent historical IDs or declare a synthetic TP.

Usage on authorized practice WSL, with other trading sessions stopped:
    KIS_ENV=practice WSL_RUN_MARKET=US NO_TRADE=1 DRY_RUN=1 \
      DISABLE_REAL_TRADING=1 US_ORDER_ARMED=0 \
      .venv/bin/python -m scripts.us_legacy_nine_sell_readiness \
      --trade-date YYYY-MM-DD --confirm-practice-readonly
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from typing import Any

from sqlalchemy import text

SYMBOLS = ("ABBV", "DHR", "HCA", "KO", "KR", "MCK", "PFE", "PG", "TMO")
TERMINAL_DB_STATUSES = frozenset({
    "FILLED", "RECONCILED", "CANCELLED", "REJECTED", "BLOCKED", "EXPIRED",
})

CURRENT_SQL = """
SELECT DISTINCT ON (symbol) symbol,qty,as_of,trading_epoch_id,
       account_id,portfolio_epoch_id,position_cycle_id
FROM public.us_positions
WHERE trading_epoch_id=:epoch AND symbol=ANY(:symbols) AND as_of<=:as_of
ORDER BY symbol,as_of DESC,created_at DESC
"""
PENDING_SQL = """
SELECT symbol,trade_date,order_no,status,qty_requested,qty_filled
FROM public.us_orders
WHERE symbol=ANY(:symbols) AND side='SELL'
  AND env='practice' AND status NOT IN
  ('FILLED','RECONCILED','CANCELLED','REJECTED','BLOCKED','EXPIRED')
ORDER BY trade_date,symbol,order_no
"""


def _qty(value: Any) -> int | None:
    if type(value) is int:
        return value if value >= 0 else None
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    return None


def _balance_symbols(balance: Any) -> tuple[dict[str, dict], str | None]:
    """Require complete non-conflicting KIS cross-exchange balance pages."""
    if (not isinstance(balance, dict)
            or balance.get("rt_cd") != "0"
            or balance.get("balance_complete") is not True
            or balance.get("balance_authoritative") is not True
            or balance.get("failed_exchanges")
            or balance.get("balance_conflicts")
            or not isinstance(balance.get("output1"), list)):
        return {}, "FRESH_BROKER_BALANCE_INCOMPLETE"
    result = {}
    for row in balance["output1"]:
        if not isinstance(row, dict):
            return {}, "BROKER_BALANCE_ROW_INVALID"
        symbol = str(row.get("ovrs_pdno") or row.get("pdno") or "").upper()
        if symbol not in SYMBOLS:
            continue
        if symbol in result:
            return {}, "BROKER_DUPLICATE_SYMBOL"
        holding = next((row.get(k) for k in (
            "ovrs_cblc_qty", "cblc_qty", "hldg_qty",
        ) if row.get(k) not in (None, "")), None)
        orderable = next((row.get(k) for k in (
            "ord_psbl_qty", "sll_psbl_qty", "sellable_qty", "orderable_qty",
        ) if row.get(k) not in (None, "")), None)
        h, o = _qty(holding), _qty(orderable)
        if h is None or o is None or o > h:
            return {}, "BROKER_HOLDING_OR_ORDERABLE_UNVERIFIED"
        result[symbol] = {"holding": h, "orderable": o}
    return result, None


def evaluate_readiness(
    *, latest_positions: list[dict], unresolved_orders: list[dict],
    balance: dict, original_day_orders: dict[str, list[dict]],
    current_trade_date: str,
) -> dict:
    """Pure safety decision. None of these flags grant KIS POST authority.

    KIS balance/orderable quantity proves what broker currently holds, but
    an unresolved DB order may still have a late fill or stale ACK. Never
    subtract a guessed open quantity, assume a stale order expired, or
    allow an automated sell via this diagnostic.
    """
    date.fromisoformat(current_trade_date)
    from scripts.us_original_order_readonly_probe import classify_original_order
    from trader.us.data_provider import normalize_us_order_status_row

    broker, error = _balance_symbols(balance)
    prior = {str(row.get("symbol") or "").upper(): row for row in latest_positions}
    if len(prior) != len(latest_positions):
        error = error or "DUPLICATE_POSITION_SYMBOL"

    problems = {sym: [] for sym in SYMBOLS}
    statuses = {sym: [] for sym in SYMBOLS}
    if error:
        for sym in SYMBOLS:
            problems[sym].append(error)

    for row in unresolved_orders:
        sym = str(row.get("symbol") or "").upper()
        if sym not in problems:
            continue
        day = str(row.get("trade_date") or "")
        num = str(row.get("order_no") or "")
        requested = _qty(row.get("qty_requested"))
        filled = _qty(row.get("qty_filled"))
        if not day or not num or requested is None or requested <= 0 or filled is None:
            problems[sym].append("DB_UNRESOLVED_SELL_IDENTITY_INCOMPLETE")
            continue
        rows = original_day_orders.get(day)
        if not isinstance(rows, list):
            problems[sym].append("KIS_ORIGINAL_DAY_ORDERS_NOT_QUERIED")
            continue
        try:
            native = classify_original_order(
                rows, order_no=num, symbol=sym,
                expected_qty=requested, trade_date=day, side="SELL",
            )
            classification = native.get("classification")
        except Exception:
            classification = "KIS_ORDER_PROOF_INVALID"
        statuses[sym].append({
            "trade_date": day,
            "broker_classification": classification,
            # This is a status *observation*, not a write or adjudication.
            "db_status": str(row.get("status") or ""),
        })
        # Even a broker-confirmed cancellation requires reconciliation of
        # the historical outstanding DB claim before automated SELL resumes.
        problems[sym].append("HISTORICAL_SELL_RECONCILIATION_REQUIRED")
        if classification not in {"PROVEN_FILLED", "PROVEN_CANCELLED"}:
            problems[sym].append("HISTORICAL_KIS_SELL_TERMINAL_UNVERIFIED")
        elif classification == "PROVEN_FILLED" and (
            _qty(native.get("cumulative_filled_qty")) != filled
        ):
            problems[sym].append("DB_BROKER_FILLED_QTY_CONFLICT")

    current_rows = original_day_orders.get(current_trade_date)
    if not isinstance(current_rows, list):
        for sym in SYMBOLS:
            problems[sym].append("CURRENT_KIS_ORDER_BOOK_NOT_QUERIED")
    else:
        for row in current_rows:
            if not isinstance(row, dict):
                for sym in SYMBOLS:
                    problems[sym].append("CURRENT_KIS_ORDER_ROW_INVALID")
                continue
            normalized = normalize_us_order_status_row(row)
            sym = normalized.get("symbol")
            if sym not in problems:
                continue
            if normalized.get("normalization_result") != "normalized":
                problems[sym].append("CURRENT_ORDER_SCHEMA_UNVERIFIED")
            elif normalized.get("broker_operation") == "CANCEL":
                # A cancellation request is not native proof that the
                # original SELL disappeared. The operator must inspect its
                # original KIS order and remaining quantity separately.
                problems[sym].append("CURRENT_KIS_CANCEL_TERMINAL_UNVERIFIED")
            elif normalized.get("status") not in {
                "FILLED", "CANCELLED", "REJECTED",
            }:
                # An in-flight BUY can also change the holdings and cycle;
                # never infer the old orderable shares are a stable exit.
                problems[sym].append(
                    "CURRENT_KIS_SELL_UNRESOLVED"
                    if normalized.get("side") == "SELL"
                    else "CURRENT_KIS_BUY_UNRESOLVED"
                )

    output = []
    for sym in SYMBOLS:
        pos = prior.get(sym)
        bpos = broker.get(sym)
        if not pos:
            problems[sym].append("NO_CURRENT_DB_POSITION")
        else:
            db_qty = _qty(pos.get("qty"))
            if db_qty is None or db_qty == 0:
                problems[sym].append("DB_HOLDING_NONPOSITIVE_OR_INVALID")
            elif not bpos or bpos["holding"] != db_qty:
                problems[sym].append("KIS_DB_HOLDING_MISMATCH")
            if not pos.get("trading_epoch_id"):
                problems[sym].append("DB_TRADING_EPOCH_MISSING")
        if not bpos:
            problems[sym].append("KIS_HOLDING_MISSING")
        elif bpos["orderable"] == 0:
            problems[sym].append("KIS_ORDERABLE_ZERO")
        findings = sorted(set(problems[sym]))
        output.append({
            "symbol": sym,
            "db_snapshot_qty": _qty(pos.get("qty")) if pos else None,
            "kis_holding_qty": bpos["holding"] if bpos else None,
            "kis_orderable_qty": bpos["orderable"] if bpos else None,
            "broker_manual_screen_review": (
                "CHECK_PRICE_AND_SUBMIT_IN_BROKER_UI" if not findings
                else "STOP_AND_RECONCILE_FIRST"
            ),
            "automated_sell_authorized": False,
            "unresolved_original_day_evidence": statuses[sym],
            "blockers": findings,
        })
    return {
        "status": "READONLY_MANUAL_SELL_PREFLIGHT",
        "date": current_trade_date,
        "positions": output,
        "review_candidates": sum(not row["blockers"] for row in output),
        "automated_sell_authorized": False,
        "broker_orders_submitted": 0,
        "broker_orders_cancelled": 0,
        "db_rows_modified": 0,
    }


def load_db_readonly(engine: Any, *, epoch: str, trade_date: str):
    if not epoch:
        raise RuntimeError("ACTIVE_EPOCH_MISSING")
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        params = {"epoch": epoch, "symbols": list(SYMBOLS), "as_of": trade_date}
        positions = [dict(r) for r in conn.execute(
            text(CURRENT_SQL), params,
        ).mappings().all()]
        unresolved = [dict(r) for r in conn.execute(
            text(PENDING_SQL), params,
        ).mappings().all()]
    return positions, unresolved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--confirm-practice-readonly", action="store_true")
    args = parser.parse_args()
    date.fromisoformat(args.trade_date)
    if (not args.confirm_practice_readonly
            or os.getenv("KIS_ENV") != "practice"
            or os.getenv("WSL_RUN_MARKET", "").upper() != "US"
            or os.getenv("NO_TRADE") != "1"
            or os.getenv("DRY_RUN") != "1"
            or os.getenv("US_ORDER_ARMED", "0") != "0"
            or os.getenv("DISABLE_REAL_TRADING") != "1"):
        print(json.dumps({"status": "PRACTICE_READONLY_REQUIRED",
                          "broker_orders_submitted": 0}))
        return 42
    try:
        from trader.us.execution.kis_us_client import KisUSClient
        from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
        from scripts.us_legacy_nine_originals_readonly_certify import (
            assert_practice_client_matches_current_scope,
        )
        scope = _us_execution_claim_scope()
        if scope.get("env") != "practice" or scope.get("market") != "US":
            raise RuntimeError("SCOPE_INVALID")
        engine = _get_engine_or_none()
        if engine is None:
            raise RuntimeError("DATABASE_UNAVAILABLE")
        client = KisUSClient(env="practice")
        assert_practice_client_matches_current_scope(client, scope)
        positions, unresolved = load_db_readonly(
            engine, epoch=scope["trading_epoch_id"], trade_date=args.trade_date,
        )
        # Broker source day is mandatory for every unresolved historical SELL.
        days = {args.trade_date}
        days.update(str(r.get("trade_date")) for r in unresolved)
        source = {}
        for day in sorted(days):
            if not day:
                raise RuntimeError("UNRESOLVED_ORDER_DATE_MISSING")
            source[day] = client.get_us_today_orders(trade_date=day)
        balance = client.get_us_balance(force_refresh=True)
        report = evaluate_readiness(
            latest_positions=positions, unresolved_orders=unresolved,
            balance=balance, original_day_orders=source,
            current_trade_date=args.trade_date,
        )
    except Exception as exc:
        report = {"status": "READONLY_PREFLIGHT_ERROR",
                  "error_type": type(exc).__name__,
                  "broker_orders_submitted": 0, "db_rows_modified": 0}
        print(json.dumps(report, sort_keys=True))
        return 43
    print(json.dumps(report, sort_keys=True, default=str))
    # These flags are never broker order authorization; operator must use
    # authenticated KIS broker UI and fresh order screen independently.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
