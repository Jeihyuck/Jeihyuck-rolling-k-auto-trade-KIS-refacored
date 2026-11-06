"""Read-only 2026-10-27 PB1 293490 BUY: exact active-account KIS evidence.

Requires explicitly configured KIS practice credentials and an ACTIVE epoch.
Reads DB and KIS daily-ccld; never submits/cancels orders, changes claims,
writes reconciliation rows or infers fills from ACK.
Run locally on the credentialed WSL workstation:
  KIS_ENV=practice STRATEGY_ENV=practice \
  python -m scripts.kr_20261027_293490_readonly_proof
"""
from __future__ import annotations

import json
import math
import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

import sqlalchemy as sa

DAY = date(2026, 10, 27)
CODE = "293490"
EXPECTED_QTY = 174
KST = ZoneInfo("Asia/Seoul")


def _clean_order_no(value: Any) -> str:
    return str(value or "").strip().lstrip("0") or "0"


def classify_kis_ccld(rows: list[dict[str, Any]], *, odno: str) -> dict[str, Any]:
    """Demand exact date, code, side, broker ODNO and order quantity."""
    matches = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("pdno") or row.get("PDNO") or "").strip().lstrip("A").zfill(6)
        day = str(row.get("ord_dt") or row.get("ord_date") or "").replace("-", "").strip()
        side = str(row.get("sll_buy_dvsn_cd") or row.get("side") or "").strip().upper()
        number = _clean_order_no(row.get("odno") or row.get("ODNO"))
        if (code == CODE and day == DAY.strftime("%Y%m%d")
                and side in {"02", "2", "BUY", "B"}
                and number == _clean_order_no(odno)):
            matches.append(row)
    if len(matches) != 1:
        return {
            "status": "KIS_EXACT_ORDER_MISSING_OR_AMBIGUOUS",
            "matches": len(matches), "broker_cumulative_proven": False,
        }
    row = matches[0]
    try:
        order_qty = int(str(row.get("ord_qty") or "0"))
        filled = int(str(row.get("tot_ccld_qty") or "0"))
        amount = float(str(row.get("tot_ccld_amt") or "0"))
        avg = float(str(row.get("avg_prvs") or "0"))
    except (ValueError, TypeError):
        return {"status": "KIS_QUANTITY_OR_PRICE_INVALID", "broker_cumulative_proven": False}
    if order_qty != EXPECTED_QTY or filled < 0 or filled > order_qty:
        return {"status": "KIS_ORDER_QUANTITY_CONFLICT", "broker_cumulative_proven": False}
    if filled == 0:
        return {"status": "KIS_NO_FILL_OBSERVED", "broker_cumulative_proven": True,
                "confirmed_filled_qty": 0, "requested_qty": order_qty}
    if (not math.isfinite(amount) or not math.isfinite(avg)
            or amount <= 0 or avg <= 0
            or abs(amount / filled - avg) > max(1.0, avg * 0.001)):
        return {"status": "KIS_ECONOMIC_FILL_UNVERIFIED", "broker_cumulative_proven": False}
    return {
        "status": "KIS_FULL_FILL_PROVEN" if filled == order_qty else "KIS_PARTIAL_FILL_PROVEN",
        "broker_cumulative_proven": True,
        "confirmed_filled_qty": filled, "requested_qty": order_qty,
        "confirmed_filled_notional": amount,
        "confirmed_avg_price": round(amount / filled, 4),
    }


def load_db_scope(*, engine: Any, account_key: str) -> dict[str, Any]:
    """Read-only immutable order, claim, and fill proof for the ACTIVE epoch."""
    from trader.db.schema import schema_for_engine
    s = schema_for_engine(engine)
    with engine.connect() as conn:
        epochs = conn.execute(
            sa.select(s.trading_epochs.c.trading_epoch_id,
                      s.trading_epochs.c.started_at)
            .where(s.trading_epochs.c.env == "practice",
                   s.trading_epochs.c.account_id == account_key,
                   s.trading_epochs.c.status == "ACTIVE")
        ).all()
        if len(epochs) != 1:
            raise RuntimeError("ACTIVE_ACCOUNT_EPOCH_UNVERIFIED")
        epoch, started = epochs[0]
        if started is None or started.astimezone(KST).date() < date(2026, 10, 25):
            raise RuntimeError("CURRENT_OCTOBER_EPOCH_REQUIRED")
        begin = datetime(2026, 10, 27, tzinfo=KST).astimezone(timezone.utc)
        end = begin + timedelta(days=1)
        orders = conn.execute(
            sa.select(s.orders.c.order_id, s.orders.c.kis_odno,
                      s.orders.c.status, s.orders.c.qty,
                      s.orders.c.position_cycle_id)
            .where(s.orders.c.env == "practice",
                   s.orders.c.trading_epoch_id == str(epoch),
                   s.orders.c.code == CODE, s.orders.c.side == "BUY",
                   s.orders.c.created_at >= begin,
                   s.orders.c.created_at < end)
        ).all()
        if len(orders) != 1 or orders[0].qty != EXPECTED_QTY or not orders[0].kis_odno:
            raise RuntimeError("EXACT_OCT27_BUY_ORDER_UNVERIFIED")
        order = orders[0]
        claims = conn.execute(
            sa.select(s.execution_claims.c.action_state,
                      s.execution_claims.c.cumulative_filled_qty)
            .where(s.execution_claims.c.env == "practice",
                   s.execution_claims.c.trading_epoch_id == str(epoch),
                   s.execution_claims.c.trade_date == DAY,
                   s.execution_claims.c.strategy_owner == "PB1",
                   s.execution_claims.c.lifecycle_id.like("%:" + CODE),
                   s.execution_claims.c.action.like("BUY_ENTRY:%"))
        ).all()
        if len(claims) != 1:
            raise RuntimeError("CURRENT_PB1_CLAIM_IDENTITY_UNVERIFIED")
        fill_qty = conn.execute(
            sa.select(sa.func.coalesce(sa.func.sum(s.fills.c.qty), 0))
            .where(s.fills.c.env == "practice",
                   s.fills.c.trading_epoch_id == str(epoch),
                   s.fills.c.order_id == order.order_id,
                   s.fills.c.side == "BUY")
        ).scalar_one()
        return {
            "epoch_prefix": str(epoch)[:8],
            "order_status": str(order.status),
            "order_no_suffix": str(order.kis_odno)[-4:],
            "broker_odno": str(order.kis_odno),
            "db_claim_status": str(claims[0].action_state),
            "db_claim_cumulative_qty": int(claims[0].cumulative_filled_qty or 0),
            "db_fill_qty": int(fill_qty or 0),
        }


def main() -> int:
    if (os.getenv("KIS_ENV", "").lower() != "practice"
            or os.getenv("STRATEGY_ENV", "").lower() != "practice"
            or "openapivts.koreainvestment.com" not in os.getenv("KIS_REST_URL", "")):
        raise SystemExit("PRACTICE_KIS_ENDPOINT_REQUIRED")
    if not all(os.getenv(x) for x in ("KIS_APP_KEY", "KIS_APP_SECRET", "CANO", "ACNT_PRDT_CD")):
        raise SystemExit("PRACTICE_KIS_CREDENTIALS_UNAVAILABLE")
    from trader.account_state import get_account_key
    from trader.db.engine import get_engine
    from trader.kis_wrapper import KisAPI
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    engine = get_engine()
    try:
        proof = load_db_scope(engine=engine, account_key=get_account_key(env="practice"))
    finally:
        engine.dispose()
    client = KisAPI(kis_env="practice")
    if str(getattr(client, "env", "")).lower() != "practice":
        raise SystemExit("KIS_CLIENT_PRACTICE_NOT_PROVEN")
    result = _daily_ccld_all_pages_for_date(client, DAY)
    if result.get("_sep29_complete") is not True or str(result.get("rt_cd")) != "0":
        raise SystemExit("KIS_DAILY_CCLD_NOT_COMPLETE")
    broker = classify_kis_ccld(list(result.get("output1") or []), odno=proof.pop("broker_odno"))
    print(json.dumps({
        "mode": "READ_ONLY_NO_DB_WRITE_NO_BROKER_SUBMIT",
        "date": DAY.isoformat(), "code": CODE,
        "account_epoch": proof, "kis": broker,
        "reconciliation_performed": False,
    }, ensure_ascii=False, indent=2))
    # Complete KIS pagination with a zero or partial fill is useful evidence,\n    # but it is NOT a resolved 174-share economic execution. Signal nonzero.\n    return 0 if broker.get("status") == "KIS_FULL_FILL_PROVEN" else 2


if __name__ == "__main__":
    raise SystemExit(main())
