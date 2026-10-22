"""Read-only KIS original GET vs durable DB evidence, never a data repair command.

Requires complete KIS report and ten-row same-epoch DB snapshot.
Authenticated provenance must also be independently verified by an operator:
JSON labels are not cryptographic proof. Never authorize a write from this output.
"""
from __future__ import annotations

import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from scripts.kr.pr249_original_kis_readonly import TARGETS


def _num(value, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError(field)
        return number
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("PR249_INVALID_" + field.upper()) from exc


def build_repair_plan(kis_report: dict, db_snapshot: list[dict]) -> dict:
    if not isinstance(kis_report, dict) or kis_report.get("source") != "KIS_PRACTICE_READONLY_NETWORK":
        raise ValueError("PR249_AUTHENTICATED_KIS_GET_REQUIRED")
    if kis_report.get("status") != "READONLY_EVIDENCE_ONLY_NO_DB_MUTATIONS":
        raise ValueError("PR249_INVALID_ORIGINAL_PROVENANCE")
    if not isinstance(kis_report.get("reports"), list) or not isinstance(db_snapshot, list):
        raise ValueError("PR249_INVALID_REPORT_FORMAT")
    expected = {(day.isoformat(), code, side, odno): requested
                for day, targets in TARGETS.items()
                for (code, side, odno), requested in targets.items()}
    original = {}
    for day_report in kis_report["reports"]:
        if not isinstance(day_report, dict) or day_report.get("complete_kis_pages") is not True:
            raise ValueError("PR249_ORIGINAL_PAGINATION_UNPROVEN")
        for row in day_report.get("results", []):
            if not isinstance(row, dict):
                raise ValueError("PR249_INVALID_ORIGINAL_ROW")
            day = row.get("trade_date")
            code = row.get("code")
            side = row.get("side")
            suffix = str(row.get("kis_odno_suffix") or "")
            keys = [key for key in expected if key[:3] == (day, code, side)
                    and suffix and key[3].endswith(suffix)]
            if len(keys) != 1:
                raise ValueError("PR249_ORIGINAL_ODNO_NOT_EXACTLY_SCOPED")
            key = keys[0]
            if key in original or row.get("status") not in {
                "FULL_VERIFIED", "PARTIAL_VERIFIED", "ZERO_CONFIRMED"
            }:
                raise ValueError("PR249_ORIGINAL_NOT_UNIQUE_OR_UNVERIFIED")
            try:
                requested = int(row["requested_qty"])
                qty = int(row["broker_verified_qty"])
                amount = _num(row["broker_verified_krw"], "broker_amount")
            except (ValueError, TypeError, KeyError) as exc:
                raise ValueError("PR249_ORIGINAL_ECONOMICS_INVALID") from exc
            if requested != expected[key] or not 0 <= qty <= requested or (
                qty > 0 and amount <= 0
            ) or (qty == 0 and amount != 0):
                raise ValueError("PR249_ORIGINAL_ECONOMICS_INCONSISTENT")
            status = "FULL_VERIFIED" if qty == requested else (
                "PARTIAL_VERIFIED" if qty else "ZERO_CONFIRMED"
            )
            if row["status"] != status:
                raise ValueError("PR249_ORIGINAL_STATUS_INCONSISTENT")
            original[key] = {"qty": qty, "amount": amount}
    if set(original) != set(expected):
        raise ValueError("PR249_MISSING_ORIGINAL_KIS_TARGETS")

    durable = {}
    epoch_ids = set()
    for row in db_snapshot:
        if not isinstance(row, dict):
            raise ValueError("PR249_DB_ROW_INVALID")
        key = (row.get("trade_date"), row.get("code"), row.get("side"), row.get("kis_odno"))
        if key not in expected or key in durable or row.get("env") != "practice" or row.get("strategy") != "pb1_pullback_close":
            raise ValueError("PR249_DB_TARGET_DUPLICATE_OR_SCOPE_MISMATCH")
        if int(row.get("ordered_qty") or 0) != expected[key]:
            raise ValueError("PR249_ORDER_QTY_MISMATCH")
        epoch = row.get("portfolio_epoch_id")
        if not epoch or not row.get("position_cycle_id") or not row.get("order_id"):
            raise ValueError("PR249_DB_EPOCH_CYCLE_OR_ORDER_MISSING")
        epoch_ids.add(str(epoch))
        qty = int(row.get("stored_fill_qty") or 0)
        amount = _num(row.get("stored_fill_notional", 0), "stored_notional")
        if qty < 0 or amount < 0:
            raise ValueError("PR249_DB_NEGATIVE_ECONOMICS")
        durable[key] = {
            "qty": qty, "amount": amount, "cycle": str(row["position_cycle_id"])
        }
    if set(durable) != set(expected) or len(epoch_ids) != 1:
        raise ValueError("PR249_DB_TARGETS_OR_PORTFOLIO_EPOCH_INCOMPLETE")

    rows = []
    for day, code, side, odno in sorted(expected):
        key = day, code, side, odno
        broker, stored = original[key], durable[key]
        delta = broker["qty"] - stored["qty"]
        notional_delta = broker["amount"] - stored["amount"]
        issues = []
        if delta:
            issues.append("DB_OVERSHOOT" if delta < 0 else "DB_UNAPPLIED_EXECUTION")
        if abs(notional_delta) > Decimal("1"):
            issues.append("NOTIONAL_MISMATCH")
        if side == "SELL":
            buys = [value for k, value in durable.items()
                    if k[0] == day and k[1] == code and k[2] == "BUY"]
            if len(buys) == 1 and buys[0]["cycle"] != stored["cycle"]:
                issues.append("SELL_CYCLE_MISMATCH_MANUAL_ATTRIBUTION_REQUIRED")
        if broker["qty"] < expected[key]:
            issues.append("BROKER_PARTIAL_KEEP_REMAINDER_UNRESOLVED")
        rows.append({
            "trade_date": day, "code": code, "side": side,
            "kis_odno_suffix": odno[-4:],
            "requested_qty": expected[key],
            "kis_verified_cumulative_qty": broker["qty"],
            "db_stored_fill_qty": stored["qty"],
            "dry_run_qty_delta": delta,
            "kis_verified_krw": str(broker["amount"]),
            "db_stored_notional_krw": str(stored["amount"]),
            "dry_run_notional_delta_krw": str(notional_delta),
            "issues": issues,
        })
    return {
        "status": "REVIEW_REQUIRED_NO_WRITES",
        "targets": len(rows), "discrepancies": sum(bool(row["issues"]) for row in rows),
        "operations_executed": 0,
        "note": "Do not repair any account from this plan. First independently verify broker source and all account/cycle/claim identities.",
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kis", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    try:
        plan = build_repair_plan(json.loads(args.kis.read_text(encoding="utf-8")),
                                 json.loads(args.db.read_text(encoding="utf-8")))
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "BLOCK", "reason": str(exc)}))
        return 2
    rendered = json.dumps(plan, ensure_ascii=False, indent=2)
    if args.out:
        args.out.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
