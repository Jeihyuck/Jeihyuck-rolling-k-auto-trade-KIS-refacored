"""KR PR257 read-only old account KIS price/quantity evidence audit.

This tool cannot place/cancel orders or update the database. Old-epoch BUY
and price-unresolved SELL are audited against exactly matching account-bound
KIS records for the ORIGINAL trade date. If the current practice credentials
are not the original account, rows are kept unresolved (never rebased onto a
new practice epoch). CI only tests pure evaluators with synthetic evidence.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import json
import os
from typing import Any
from zoneinfo import ZoneInfo

import sqlalchemy as sa

KST = ZoneInfo("Asia/Seoul")
PRICE_UNRESOLVED = "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"


def _positive_decimal(value: Any) -> Decimal | None:
    try:
        d = Decimal(str(value))
        return d if d.is_finite() and d > 0 else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _whole_quantity(value: Any) -> int | None:
    try:
        d = Decimal(str(value))
        return int(d) if d.is_finite() and d >= 0 and d == d.to_integral_value() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def prove_exact_cumulative_economics(
    broker_rows: list[dict[str, Any]], *,
    trade_date: date, code: str, side: str, order_no: str,
    requested_qty: int,
) -> dict[str, Any]:
    """An ORDER summary is not an individual fill; use exact cumulative truth."""
    code = str(code).zfill(6)
    order_no = str(order_no).zfill(10)
    found: list[dict[str, Any]] = []
    for row in broker_rows:
        if not isinstance(row, dict):
            continue
        source_code = str(row.get("pdno") or row.get("code") or "").removeprefix("A").zfill(6)
        source_no = str(row.get("odno") or row.get("ODNO") or "").zfill(10)
        source_day = str(row.get("ord_dt") or row.get("trd_dt") or row.get("ccld_dt") or "").replace("-", "")
        source_side = str(row.get("sll_buy_dvsn_cd") or row.get("side") or "").upper()
        observed_side = "BUY" if source_side in ("02", "BUY") else "SELL" if source_side in ("01", "SELL") else ""
        if (source_code == code and source_no == order_no
                and source_day == trade_date.strftime("%Y%m%d")
                and observed_side == side):
            found.append(row)
    if len(found) != 1:
        return {"verified": False, "reason": "MISSING_OR_AMBIGUOUS_ORIGINAL_KIS_ORDER"}
    row = found[0]
    requested = _whole_quantity(row.get("ord_qty"))
    cumulative = _whole_quantity(row.get("tot_ccld_qty"))
    amount = _positive_decimal(row.get("tot_ccld_amt"))
    avg = _positive_decimal(row.get("avg_prvs"))
    if requested is None or requested != requested_qty:
        return {"verified": False, "reason": "BROKER_ORDER_REQUESTED_QTY_MISMATCH"}
    if cumulative is None or cumulative > requested:
        return {"verified": False, "reason": "BROKER_CUMULATIVE_QTY_INVALID"}
    if cumulative == 0:
        return {"verified": False, "reason": "BROKER_CONFIRMED_ZERO_FILL",
                "broker_cumulative_qty": 0}
    if amount is None or avg is None:
        return {"verified": False, "reason": "BROKER_FILL_PRICE_UNPROVEN",
                "broker_cumulative_qty": cumulative}
    computed = amount / Decimal(cumulative)
    if abs(computed - avg) > max(Decimal("1"), avg * Decimal("0.001")):
        return {"verified": False, "reason": "BROKER_AVG_VS_AMOUNT_CONFLICT",
                "broker_cumulative_qty": cumulative}
    return {"verified": True, "reason": "KIS_CUMULATIVE_QTY_AMOUNT_AVG_PROVEN",
            "broker_cumulative_qty": cumulative,
            "broker_cumulative_notional_krw": str(amount),
            "broker_average_krw": str(computed)}


def select_unresolved_original_orders(engine: Any, *, start: date, end: date) -> list[dict]:
    from trader.db.schema import schema_for_engine
    s = schema_for_engine(engine)
    start_at = datetime.combine(start, datetime.min.time(), tzinfo=KST)
    end_at = datetime.combine(end, datetime.min.time(), tzinfo=KST)
    with engine.connect() as conn:
        rows = conn.execute(sa.select(
            s.orders.c.order_id, s.orders.c.env, s.orders.c.strategy,
            s.orders.c.code, s.orders.c.side, s.orders.c.qty,
            s.orders.c.created_at, s.orders.c.kis_odno,
            s.orders.c.status, s.orders.c.portfolio_epoch_id,
            s.orders.c.trading_epoch_id, s.portfolio_epochs.c.account_id,
            s.portfolio_epochs.c.trading_epoch_id.label("epoch_trading_id"),
            s.portfolio_epochs.c.env.label("epoch_env"),
            s.portfolio_epochs.c.strategy.label("epoch_strategy"),
        ).join(
            s.portfolio_epochs,
            s.orders.c.portfolio_epoch_id == s.portfolio_epochs.c.portfolio_epoch_id,
        ).where(sa.and_(
            s.orders.c.env == "practice",
            s.orders.c.strategy == "pb1_pullback_close",
            s.orders.c.created_at >= start_at,
            s.orders.c.created_at < end_at,
            sa.or_(
                sa.and_(s.orders.c.side == "SELL", s.orders.c.status == PRICE_UNRESOLVED),
                sa.and_(s.orders.c.side == "BUY", s.orders.c.code == "012750",
                        s.orders.c.created_at >= datetime(2026, 10, 23, tzinfo=KST),
                        s.orders.c.created_at < datetime(2026, 10, 24, tzinfo=KST)),
            ),
        )).order_by(s.orders.c.created_at.asc())).mappings().all()
    return [dict(row) for row in rows]


def analyze_rows_for_account(
    targets: list[dict], *,
    current_account_key: str, broker_by_date: dict[date, list[dict]],
) -> dict[str, Any]:
    results = []
    for row in targets:
        order_time = row["created_at"].astimezone(KST)
        day = order_time.date()
        account_ok = (
            row.get("account_id") == current_account_key
            and row.get("env") == "practice"
            and row.get("epoch_env") == "practice"
            and row.get("strategy") == "pb1_pullback_close"
            and row.get("epoch_strategy") == "pb1_pullback_close"
            and str(row.get("trading_epoch_id") or "") == str(row.get("epoch_trading_id") or "")
            and bool(row.get("trading_epoch_id"))
        )
        output = {
            "trade_date": day.isoformat(),
            "code": str(row.get("code") or "").zfill(6),
            "side": str(row.get("side") or ""),
            "order_no_suffix": str(row.get("kis_odno") or "")[-4:],
            "requested_qty": int(row.get("qty") or 0),
            "current_account_matches_original": account_ok,
            "database_status": str(row.get("status") or ""),
        }
        if not account_ok:
            proof = {"verified": False, "reason": "ORIGINAL_ACCOUNT_OR_EPOCH_NOT_AUTHORIZED"}
        elif day not in broker_by_date:
            proof = {"verified": False, "reason": "ORIGINAL_KIS_DATE_NOT_QUERIED"}
        else:
            proof = prove_exact_cumulative_economics(
                broker_by_date[day], trade_date=day,
                code=output["code"], side=output["side"],
                order_no=str(row.get("kis_odno") or ""),
                requested_qty=output["requested_qty"],
            )
        output.update(proof)
        results.append(output)
    return {
        "orders_checked": len(results),
        "broker_economics_verified": sum(bool(row["verified"]) for row in results),
        "unresolved": sum(not bool(row["verified"]) for row in results),
        "database_modified": False,
        "kis_order_submissions": 0,
        "results": results,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Strict read-only KR historic fill-price evidence")
    parser.add_argument("--start", default="2026-10-13")
    parser.add_argument("--end-exclusive", default="2026-10-27")
    args = parser.parse_args(argv)
    if (
        os.getenv("KIS_ENV", "").strip().lower() != "practice"
        or os.getenv("STRATEGY_ENV", "").strip().lower() != "practice"
        or "openapivts.koreainvestment.com" not in os.getenv("KIS_REST_URL", "")
    ):
        raise SystemExit("PRACTICE_KIS_READ_ONLY_CREDENTIALS_REQUIRED")
    if not all(os.getenv(k) for k in ("KIS_APP_KEY", "KIS_APP_SECRET", "CANO", "ACNT_PRDT_CD")):
        raise SystemExit("PRACTICE_KIS_CREDENTIALS_MISSING")
    from trader.db.engine import get_engine
    from trader.account_state import get_account_key
    from trader.kis_wrapper import KisAPI
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date

    engine = get_engine()
    targets = select_unresolved_original_orders(
        engine, start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end_exclusive),
    )
    kis = KisAPI(kis_env="practice")
    current_account = get_account_key(env="practice", kis=kis)
    # Only query original-account trading days. A new account can never
    # provide an old account's order truth.
    days = sorted({row["created_at"].astimezone(KST).date() for row in targets
                   if row.get("account_id") == current_account
                   and row.get("trading_epoch_id") == row.get("epoch_trading_id")})
    by_day: dict[date, list[dict]] = {}
    for day in days:
        response = _daily_ccld_all_pages_for_date(kis, day)
        if response.get("_sep29_complete") is True and str(response.get("rt_cd")) == "0":
            by_day[day] = response.get("output1") or []
    audit = analyze_rows_for_account(targets, current_account_key=current_account,
                                     broker_by_date=by_day)
    # Never expose raw broker rows, tokens, or full account/epoch IDs.
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0 if audit["unresolved"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
