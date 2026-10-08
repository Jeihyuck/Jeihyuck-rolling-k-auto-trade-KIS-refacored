"""Read-only KIS practice order evidence probe; never submit or cancel orders.

Usage:
 python -m scripts.us_kis_order_inspect --trade-date 2026-10-06 \
   --targets MRVL:34237,AMD:35281 --output artifacts/us-kis-order-inspection.json

BROKER_NOT_FOUND is never proof of cancellation. No database writes are possible.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

from trader.us.data_provider import normalize_us_order_status_row
from trader.us.db.repos import normalize_us_order_no
from trader.us.protective_tp_recovery import (
    broker_proves_open_tp_order,
    broker_proves_terminal_after_cancel,
)


_TARGET_RE = re.compile(r"^([A-Z][A-Z0-9.]{0,9}):([0-9]{1,12})$")
_ORDER_SQL = text("""
    SELECT o.symbol, o.trade_date, o.order_no, o.side, o.status,
           o.qty_requested, o.qty_filled, o.meta, o.trading_epoch_id,
           c.action_state AS claim_state
      FROM us_orders o
      LEFT JOIN us_execution_claims c
        ON c.trading_epoch_id = o.trading_epoch_id
       AND c.env = o.env AND c.market = 'US'
       AND c.trade_date = o.trade_date
       AND c.strategy_owner = 'US_STANDARD'
       AND c.lifecycle_id = o.meta->>'position_lifecycle_id'
       AND c.action = UPPER(o.meta->>'profit_capture_stage')
     WHERE o.env = 'practice' AND o.trading_epoch_id = :epoch
       AND o.trade_date = :trade_date AND o.symbol = :symbol
       AND LTRIM(o.order_no, '0') = :order_no
""")


def parse_targets(value: str) -> list[tuple[str, str]]:
    raw = value.split(",")
    if not 1 <= len(raw) <= 10:
        raise ValueError("Specify between 1 and 10 exact symbol:order_no pairs")
    result = []
    for item in raw:
        match = _TARGET_RE.fullmatch(item.strip())
        if not match:
            raise ValueError("Targets must be uppercase SYMBOL:DIGITS, comma separated")
        pair = (match.group(1), normalize_us_order_no(match.group(2)))
        if pair in result:
            raise ValueError("Duplicate target")
        result.append(pair)
    return result


def validate_trade_date(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Use YYYY-MM-DD")
    parsed = date.fromisoformat(value)
    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    if parsed > today_et:
        raise ValueError("Future US trade date")
    return value


def classify_target(
    symbol: str, order_no: str, db_rows: list[dict], broker_rows: list[dict],
) -> dict:
    """Conservative decision; this function never authorizes a write or cancel."""
    result = {"symbol": symbol, "order_no": order_no,
              "decision": "FENCED_UNVERIFIED", "db_status": None,
              "claim_state": None, "broker_status": None,
              "broker_filled": None, "broker_remaining": None}
    if len(db_rows) != 1:
        result["decision"] = "DB_ORDER_NOT_UNIQUE_OR_MISSING"
        return result
    db = db_rows[0]
    meta = db.get("meta") if isinstance(db.get("meta"), dict) else {}
    result["db_status"] = db.get("status")
    result["claim_state"] = db.get("claim_state")
    if (str(db.get("side") or "").upper() != "SELL"
            or str(meta.get("strategy_owner") or "") != "US_STANDARD"
            or not str(meta.get("position_lifecycle_id") or "")
            or str(meta.get("profit_capture_stage") or "") not in ("tp1", "tp2", "tp3")):
        result["decision"] = "OWNER_OR_LIFECYCLE_MISMATCH"
        return result
    if len(broker_rows) != 1:
        result["decision"] = ("BROKER_NOT_FOUND_NO_PROOF" if not broker_rows
                              else "BROKER_DUPLICATE_AMBIGUOUS")
        return result
    broker = broker_rows[0]
    result["broker_status"] = broker.get("status")
    result["broker_filled"] = broker.get("filled_qty")
    result["broker_remaining"] = broker.get("remaining_qty")
    if (str(broker.get("symbol") or "") != symbol
            or str(broker.get("side") or "") != "SELL"
            or normalize_us_order_no(broker.get("order_no")) != order_no):
        result["decision"] = "BROKER_IDENTITY_MISMATCH"
        return result
    if broker.get("requires_reconcile") or broker.get("remaining_qty_present") is not True:
        result["decision"] = "BROKER_EVIDENCE_INCOMPLETE"
        return result
    if broker_proves_terminal_after_cancel(db, broker):
        result["decision"] = "BROKER_TERMINAL_RECONCILIATION_REVIEW"
    elif (db.get("claim_state") in ("IN_FLIGHT", "UNCERTAIN", "PARTIALLY_SATISFIED")
          and broker_proves_open_tp_order(db, broker)):
        result["decision"] = "BROKER_OPEN_CANCELLATION_REVIEW"
    else:
        result["decision"] = "FENCED_UNVERIFIED"
    return result


def inspect(trade_date: str, targets: list[tuple[str, str]]) -> dict:
    # Import credentials/config only after input validation and hard read-only gates.
    os.environ["KIS_ENV"] = "practice"
    os.environ["DRY_RUN"] = "1"
    os.environ["ALLOW_REAL_ORDER"] = "0"
    os.environ["US_LIVE_TRADING_ENABLED"] = "0"
    os.environ["DISABLE_REAL_TRADING"] = "1"
    os.environ["US_KIS_ORDER_ALLOWED"] = "0"
    os.environ["US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED"] = "0"
    url = os.environ.get("PBCORE_DB_URL") or os.environ.get("US_DB_URL") or os.environ.get("DATABASE_URL")
    cano = os.environ.get("CANO_US") or os.environ.get("CANO")
    product = os.environ.get("ACNT_PRDT_CD_US") or os.environ.get("ACNT_PRDT_CD")
    if not url or not cano or not product:
        raise RuntimeError("DB URL and KIS paper account configuration are required")
    account_id = f"practice:{cano}:{product}"
    engine = create_engine(url)
    from trader.us.execution.kis_us_client import KisUSClient
    try:
        with engine.connect() as conn:
            epochs = conn.execute(text("""
                SELECT trading_epoch_id FROM trading_epochs
                 WHERE env = 'practice' AND account_id = :account_id AND status = 'ACTIVE'
            """), {"account_id": account_id}).scalars().all()
            if len(epochs) != 1:
                raise RuntimeError("Exactly one matching ACTIVE practice epoch is required")
            epoch = epochs[0]
            db = {}
            for symbol, order_no in targets:
                db[(symbol, order_no)] = [
                    dict(r) for r in conn.execute(_ORDER_SQL, {
                        "epoch": epoch, "trade_date": trade_date,
                        "symbol": symbol, "order_no": order_no,
                    }).mappings()
                ]
        # One KIS inquiry for the exact historical date; no order endpoints.
        client = KisUSClient(env="practice", offline=False)
        raw = client.get_us_today_orders(trade_date=trade_date)
        normalized = [normalize_us_order_status_row(row) for row in raw]
        result_rows = []
        for symbol, order_no in targets:
            matching = [r for r in normalized
                        if str(r.get("symbol") or "") == symbol
                        and normalize_us_order_no(r.get("order_no")) == order_no]
            result_rows.append(classify_target(
                symbol, order_no, db[(symbol, order_no)], matching,
            ))
        return {"mode": "READ_ONLY", "trade_date": trade_date,
                "orders": result_rows,
                "note": "A missing order/ACK is not terminal proof; no writes or cancels executed."}
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--targets", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    trade_date = validate_trade_date(args.trade_date)
    targets = parse_targets(args.targets)
    report = inspect(trade_date, targets)
    target_path = Path(args.output)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
