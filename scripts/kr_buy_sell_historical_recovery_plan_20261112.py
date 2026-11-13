"""Read-only historical KR lineage recovery *plan*, NEVER a data repair.

Six exact original-source orders. The operator must independently obtain all
pages of original KIS daily_ccld, exact account, order, side, date, qty,
notional, and current broker balance before authorizing any eventual mapping.
An ACK or imported position's qty is insufficient. This executable dry-run
cannot fabricate a fill, change a cycle, or turn a pending claim satisfied.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.engine import get_engine
from trader.db.trading_epoch import active_trading_epoch_id

EXACT_CASES = (
    ("2026-10-27", "028050", "0000011241", 33, "삼성E&A"),
    ("2026-10-27", "012750", "0000011653", 21, "에스원"),
    ("2026-10-27", "455890", "0000011685", 35, "RISE 머니마켓액티브"),
    ("2026-10-28", "253590", "0000029434", 110, "네오셈"),
    ("2026-10-29", "059090", "0000011865", 79, "미코"),
    ("2026-10-27", "293490", "0000011280", 174, "카카오게임즈"),
)


def collect_plan(engine: sa.Engine, *, env: str, epoch: str) -> dict[str, Any]:
    if env != "practice":
        raise RuntimeError("KR_HISTORICAL_PLAN_PRACTICE_ONLY")
    account = get_account_key(env=env)
    with engine.connect() as conn:
        active = active_trading_epoch_id(
            conn, env=env, account_id=account, required=True,
        )
        if str(active or "") != epoch:
            raise RuntimeError("KR_HISTORICAL_PLAN_ACTIVE_ACCOUNT_EPOCH_MISMATCH")
        if conn.execute(sa.text("""
            SELECT count(*) FROM public.trading_epochs
            WHERE env=:env AND account_id=:account AND status='ACTIVE'
                AND trading_epoch_id=:epoch
        """), {"env": env, "account": account, "epoch": epoch}).scalar_one() != 1:
            raise RuntimeError("KR_HISTORICAL_PLAN_ACCOUNT_BINDING_UNPROVEN")
        all_orders = [dict(row) for row in conn.execute(sa.text("""
            SELECT order_id, code, side, qty, status, kis_odno, market, strategy,
                   position_cycle_id, portfolio_epoch_id, trading_epoch_id,
                   created_at, request_json, response_json
            FROM public.orders
            WHERE env=:env AND trading_epoch_id=:epoch
              AND code IN ('028050','012750','455890','253590','059090','293490')
        """), {"env": env, "epoch": epoch}).mappings()]
        all_fills = [dict(row) for row in conn.execute(sa.text("""
            SELECT order_id, code, side, qty, price, kis_odno, trade_id,
                   broker_fill_id, position_cycle_id, portfolio_epoch_id,
                   trading_epoch_id, fill_meta_json, filled_at
            FROM public.fills
            WHERE env=:env AND trading_epoch_id=:epoch
              AND code IN ('028050','012750','455890','253590','059090','293490')
        """), {"env": env, "epoch": epoch}).mappings()]
        all_positions = [dict(row) for row in conn.execute(sa.text("""
            SELECT code, strategy, qty, status, position_origin,
                   position_cycle_id, portfolio_epoch_id, trading_epoch_id
            FROM public.positions
            WHERE env=:env AND trading_epoch_id=:epoch
              AND code IN ('028050','012750','455890','253590','059090','293490')
        """), {"env": env, "epoch": epoch}).mappings()]
    plans = []
    for date, code, number, expected_qty, name in EXACT_CASES:
        original = [
            o for o in all_orders if
            str(o["code"]) == code and str(o["side"]) == "BUY"
            and str(o.get("kis_odno") or "").lstrip("0") == number.lstrip("0")
            and o["created_at"].astimezone(
                __import__("zoneinfo").ZoneInfo("Asia/Seoul")
            ).date().isoformat() == date
        ]
        source = original[0] if len(original) == 1 else {}
        fills = [f for f in all_fills
                 if source and str(f.get("order_id")) == str(source.get("order_id"))]
        same_symbol_positions = [p for p in all_positions if p["code"] == code]
        same_cycle = [p for p in same_symbol_positions if source
                      and p["position_cycle_id"] == source.get("position_cycle_id")
                      and p["portfolio_epoch_id"] == source.get("portfolio_epoch_id")]
        imported = [p for p in same_symbol_positions if p["position_origin"] == "IMPORTED"]
        foreign = [o for o in all_orders if o["code"] == code and o["side"] == "SELL"
                   and source and o["position_cycle_id"] != source.get("position_cycle_id")]
        issues = []
        if len(original) != 1:
            issues.append("ORIGINAL_ORDER_NOT_UNIQUE_IN_DB")
        if source and source["qty"] != expected_qty:
            issues.append("ORIGINAL_SUBMITTED_QTY_UNEXPECTED")
        if not fills:
            issues.append("ORIGINAL_FILL_ROW_MISSING")
        if not same_cycle:
            issues.append("ORIGINAL_CYCLE_POSITION_NOT_FOUND")
        if imported:
            issues.append("IMPORTED_CYCLES_MUST_NOT_AUTOMERGE")
        if foreign:
            issues.append("SELL_OTHER_CYCLE_REQUIRES_INDEPENDENT_KIS_PROOF")
        issues.append("ORIGINAL_KIS_ALL_PAGES_AND_ACCOUNT_PROOF_NOT_COLLECTED")
        issues.append("CURRENT_BROKER_HOLDINGS_AND_UNFILLED_NOT_COLLECTED")
        plans.append({
            "date_kst": date, "code": code, "name": name,
            "original_broker_order_no": number, "expected_buy_qty": expected_qty,
            "db_original_orders": len(original),
            "db_original_order_id": source.get("order_id"),
            "original_cycle_id": source.get("position_cycle_id"),
            "original_fills_in_db": len(fills),
            "exact_cycle_positions": [
                {"cycle": p["position_cycle_id"], "qty": p["qty"],
                 "status": p["status"]} for p in same_cycle
            ],
            "other_imported_cycles": [
                {"cycle": p["position_cycle_id"], "qty": p["qty"],
                 "status": p["status"]} for p in imported
            ],
            "other_cycle_sell_records": len(foreign),
            "issues": sorted(set(issues)),
            "recovery_action": "NONE_PROOF_REQUIRED",
            "approved_for_operational_mutation": False,
        })
    return {
        "version": "KR_HISTORICAL_EVIDENCE_DRY_RUN_20261112_V1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "env": env, "account": "REDACTED", "trading_epoch_id": epoch,
        "all_cases_unresolved_before_independent_KIS_proof": len(plans),
        "cases": plans, "operations_performed": "READ_ONLY",
        "KIS_queries_made": 0, "DB_rows_modified": 0,
        "required_next_step": (
            "Obtain all original-day KIS execution pages and prove exact account/"
            "ODNO/date/code/side/qty/notional plus current holdings and unfilled; "
            "only then separately review a transactionally reversible repair."
        ),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--env", default="practice", choices=["practice"])
    args = parser.parse_args(argv)
    report = collect_plan(get_engine(), env=args.env, epoch=args.epoch)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
