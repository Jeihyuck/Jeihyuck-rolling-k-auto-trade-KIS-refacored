"""KR244 historical KIS↔DB SELL reconciliation, explicitly invoked by an operator.

Default mode is DB read-only. --apply-verified is practice-only and may write
ONLY through the existing broker-evidence reconcile_today pipeline. It cannot
create, resubmit, cancel or modify broker orders. No fabricated fill prices.

Example (with the actual practice credentials and DB configured):
  python -m scripts.kr244_reconcile_day --env practice --trade-date 2026-10-20
  python -m scripts.kr244_reconcile_day --env practice --trade-date 2026-10-20 --apply-verified
"""
from __future__ import annotations

import argparse
from datetime import date
import json
import os
from uuid import uuid4

from trader.db.engine import get_engine
from trader.kr.unpriced_sell_audit import audit_kr_pending_sell_orders
from trader.time_utils import now_kst



def verify_exact_oct20_sell_restoration(engine, targets: dict[str, tuple[str, int]]) -> dict:
    """Fail closed until each proven KIS cumulative SELL has a priced DB fill.

    This validates durable source order, exact lifecycle and broker-fill ID.
    It never writes or attempts a broker order.
    """
    import sqlalchemy as sa
    from trader.db.repos import OrdersRepo
    from trader.db.schema import schema_for_engine, uuid_value_for_url

    schema = schema_for_engine(engine)
    repo = OrdersRepo(engine)
    incomplete = []
    verified = 0
    for code, (odno, requested_qty) in targets.items():
        order = repo.get_order_by_kis_odno_scoped(
            "practice", odno, trade_date=date(2026, 10, 20),
            code=code, side="SELL",
        )
        if not order:
            incomplete.append((code, "ORIGINAL_ORDER_MISSING"))
            continue
        if str(order.get("status") or "") != "FILLED":
            incomplete.append((code, "ORDER_NOT_FILLED"))
            continue
        resp = order.get("response_json") or {}
        if isinstance(resp, str):
            resp = json.loads(resp)
        if (
            int(resp.get("confirmed_fill_qty") or 0) != requested_qty
            or float(resp.get("confirmed_fill_notional") or 0) <= 0
            or float(resp.get("confirmed_fill_price") or 0) <= 0
        ):
            incomplete.append((code, "ORDER_PRICING_NOT_CONFIRMED"))
            continue
        with engine.connect() as conn:
            fill_rows = conn.execute(sa.select(
                schema.fills.c.qty, schema.fills.c.price, schema.fills.c.fill_meta_json,
            ).where(sa.and_(
                schema.fills.c.env == "practice",
                schema.fills.c.order_id ==
                uuid_value_for_url(str(engine.url), str(order["order_id"])),
                schema.fills.c.side == "SELL",
            ))).mappings().all()
            pos = conn.execute(sa.select(
                schema.positions.c.qty, schema.positions.c.status,
                schema.positions.c.position_cycle_id,
            ).where(sa.and_(
                schema.positions.c.env == "practice",
                schema.positions.c.code == code,
                schema.positions.c.strategy == order["strategy"],
                schema.positions.c.position_cycle_id == order["position_cycle_id"],
                schema.positions.c.portfolio_epoch_id == order["portfolio_epoch_id"],
            ))).mappings().first()
        relevant = []
        for fill in fill_rows:
            meta = fill["fill_meta_json"] or {}
            if isinstance(meta, str):
                meta = json.loads(meta)
            if meta.get("fill_source") == "daily_ccld_order_cumulative":
                relevant.append(fill)
        if sum(int(x["qty"]) for x in relevant) != requested_qty or any(
            float(x["price"] or 0) <= 0 for x in relevant
        ):
            incomplete.append((code, "EXACT_CUMULATIVE_FILL_MISSING"))
        elif not pos or int(pos.get("qty") or 0) != 0 or pos.get("status") != "CLOSED":
            incomplete.append((code, "POSITION_QTY_NOT_RECONCILED"))
        else:
            verified += 1
    return {
        "expected": len(targets),
        "fully_restored": verified,
        "unresolved": incomplete,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KR practice exact-order SELL reconciliation")
    parser.add_argument("--env", choices=("practice", "real"), required=True)
    parser.add_argument("--trade-date", required=True, help="KST YYYY-MM-DD")
    parser.add_argument("--apply-verified", action="store_true", help="Write only actual KIS execution evidence (practice only)")
    args = parser.parse_args(argv)
    trade_date = date.fromisoformat(args.trade_date)
    if trade_date > now_kst().date():
        parser.error("Cannot reconcile a future trading day")
    if args.apply_verified:
        if args.env != "practice":
            parser.error("--apply-verified is restricted to the practice account")
        if trade_date != date(2026, 10, 20):
            parser.error("--apply-verified is restricted to the proven Oct-20 SELL recovery")
        if os.getenv("DISABLE_REAL_TRADING", "") != "1":
            parser.error("DISABLE_REAL_TRADING=1 must explicitly prevent broker orders")
        if "openapivts.koreainvestment.com" not in str(os.getenv("KIS_REST_URL") or ""):
            parser.error("KIS endpoint must be the KIS practice endpoint")
        kis_env = str(os.getenv("KIS_ENV") or "").strip().lower()
        strategy_env = str(os.getenv("STRATEGY_ENV") or "").strip().lower()
        if kis_env != "practice" or strategy_env != "practice":
            parser.error("KIS_ENV and STRATEGY_ENV must both be explicitly practice")

    engine = get_engine()
    try:
        before = audit_kr_pending_sell_orders(engine, env=args.env, trade_date=trade_date)
        result = {"before": before, "mode": "READ_ONLY"}
        if args.apply_verified:
            from trader.kis_wrapper import KisAPI
            from trader.reconcile_kis import reconcile_today
            from trader.run_context import RunContext
            from trader.account_state import get_account_key
            from scripts.kr244_kis_readonly_probe import EXPECTED_OCT20_SELLS
            import hashlib
            expected_scope_prefix = "fef5d08b17"
            actual_scope_prefix = hashlib.sha256(
                get_account_key(env="practice").encode("utf-8")
            ).hexdigest()[:len(expected_scope_prefix)]
            if actual_scope_prefix != expected_scope_prefix:
                raise RuntimeError("KR244_DB_ACCOUNT_SCOPE_MISMATCH")
            kis = KisAPI(kis_env="practice")
            if str(getattr(kis, "KIS_ENV", getattr(kis, "env", "")) or "practice").lower() not in ("practice", ""):
                raise RuntimeError("KIS environment mismatch; refusing DB mutation")
            ctx = RunContext(
                run_id=str(uuid4()), env="practice", strategy="pb1_pullback_close",
                started_at=now_kst(), dry_run=False,
            )
            evidence = reconcile_today(
                engine=engine, kis=kis, ctx=ctx,
                trade_date=trade_date, require_authoritative=True,
                strict_sell_targets=EXPECTED_OCT20_SELLS,
            )
            after = audit_kr_pending_sell_orders(engine, env=args.env, trade_date=trade_date)
            verification = verify_exact_oct20_sell_restoration(
                engine, EXPECTED_OCT20_SELLS,
            )
            result = {
                "before": before, "broker_reconciliation": evidence,
                "after": after, "verification": verification,
                "mode": "APPLY_KIS_EXECUTION_EVIDENCE_ONLY",
            }
            if verification["fully_restored"] != verification["expected"]:
                print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
                raise RuntimeError("KR244_11_SELLS_NOT_FULLY_RESTORED")
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
