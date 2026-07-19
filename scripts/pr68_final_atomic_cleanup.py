#!/usr/bin/env python3
from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise RuntimeError(f"pattern not found in {path}: {old[:120]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def append_once(path: Path, marker: str, block: str) -> None:
    text = path.read_text(encoding="utf-8")
    if marker in text:
        return
    path.write_text(text.rstrip() + "\n\n" + block.strip() + "\n", encoding="utf-8")


def restore_reports_from_base() -> None:
    subprocess.run(["git", "fetch", "origin", "dual-agent", "--depth=1"], cwd=ROOT, check=True)
    proc = subprocess.run(
        ["git", "diff", "--name-only", "origin/dual-agent", "--", "reports/us_daily", "reports/us_prep"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    for rel in [line.strip() for line in proc.stdout.splitlines() if line.strip()]:
        exists = subprocess.run(["git", "cat-file", "-e", f"origin/dual-agent:{rel}"], cwd=ROOT).returncode == 0
        target = ROOT / rel
        if exists:
            subprocess.run(["git", "checkout", "origin/dual-agent", "--", rel], cwd=ROOT, check=True)
        else:
            target.unlink(missing_ok=True)
    subprocess.run(["git", "clean", "-fd", "reports/us_daily", "reports/us_prep"], cwd=ROOT, check=True)


def patch_repos() -> None:
    path = ROOT / "trader/us/db/repos.py"
    old = '''    engine = _get_engine_or_none()
    if engine is None:
'''
    new = '''    engine = _get_engine_or_none()
    atomic_count = 0
    if engine is not None:
        # KIS order-level cumulative snapshots must be validated and persisted
        # atomically with the matching us_orders row.  Do not first upsert an
        # active actual fill and only later discover overflow/conflict in ACK
        # reconcile; conflict/overflow/regression must leave both us_orders and
        # us_fills unchanged.
        remaining_fills: list[dict] = []
        for f in fills:
            fill_meta = f.get("meta") if isinstance(f.get("meta"), dict) else {}
            evidence = str(fill_meta.get("fill_evidence_type") or f.get("fill_evidence_type") or "")
            if not _is_kis_order_cumulative_evidence(evidence):
                remaining_fills.append(f)
                continue
            try:
                order = load_us_order_for_fill(
                    order_no=f.get("order_no"),
                    client_order_key=f.get("client_order_key"),
                    symbol=str(f.get("symbol") or ""),
                    trade_date=td,
                )
                if not order:
                    f["save_status"] = "FILL_ORDER_NOT_FOUND"
                    logger.error(
                        "[US_FILLS][ATOMIC_ACTUAL][ORDER_NOT_FOUND] trade_date=%s order_no=%s symbol=%s side=%s",
                        td, f.get("order_no"), f.get("symbol"), f.get("side"),
                    )
                    continue
                resolved_symbol = str(f.get("symbol") or order.get("symbol") or "").strip().upper()
                resolved_side = str(f.get("side") or order.get("side") or "").strip().upper()
                requested_qty = int(fill_meta.get("requested_qty") or f.get("requested_qty") or order.get("qty_requested") or f.get("qty") or 0)
                cumulative_qty = int(fill_meta.get("cumulative_filled_qty") or f.get("cumulative_filled_qty") or f.get("qty") or 0)
                avg_price = float(f.get("price_usd") or f.get("price") or fill_meta.get("avg_price_usd") or 0.0)
                merged_meta = {**_parse_json_meta(order.get("meta")), **fill_meta}
                merged_meta.setdefault("source", fill_meta.get("source") or "save_fills_kis_actual")
                merged_meta.setdefault("fill_evidence_type", evidence)
                merged_meta.setdefault("is_synthetic", False)
                mark_result = mark_order_filled_by_reconcile(
                    order_no=str(f.get("order_no") or order.get("order_no") or ""),
                    client_order_key=str(f.get("client_order_key") or order.get("client_order_key") or ""),
                    symbol=resolved_symbol,
                    side=resolved_side,
                    filled_qty=cumulative_qty,
                    requested_qty=requested_qty,
                    cumulative_filled_qty=cumulative_qty,
                    evidence_type=evidence,
                    avg_price_usd=avg_price,
                    source="save_fills_kis_actual",
                    trade_date=td,
                    meta=merged_meta,
                )
                status = str((mark_result or {}).get("status") or "RECONCILE_UPDATE_FAILED")
                f["save_status"] = status
                f["atomic_reconcile_result"] = mark_result
                if status == "OK":
                    atomic_count += 1
                else:
                    logger.error(
                        "[US_FILLS][ATOMIC_ACTUAL][FAILED] status=%s trade_date=%s order_no=%s symbol=%s result=%s",
                        status, td, f.get("order_no"), resolved_symbol, mark_result,
                    )
            except Exception as exc:
                f["save_status"] = "RECONCILE_UPDATE_FAILED"
                f["atomic_reconcile_error"] = str(exc)
                logger.error("[US_FILLS][ATOMIC_ACTUAL][ERROR] %s", exc)
        if len(remaining_fills) != len(fills):
            fills = remaining_fills
            if not fills:
                logger.info("[US_FILLS][SAVE][ATOMIC_ACTUAL_DONE] confirmed=%d input=%d", atomic_count, len(fills))
                return atomic_count
    if engine is None:
'''
    replace_once(path, old, new)
    replace_once(path, "    count = 0\n    skipped_duplicates = 0\n", "    count = atomic_count\n    skipped_duplicates = 0\n")
    old_result = '''def save_fills_with_result(fills: list[dict], trade_date: str | None = None) -> dict:
    """Structured fill-save result for close/reconcile callers."""
    inserted = save_fills(fills, trade_date=trade_date)
    regression_count = sum(1 for f in (fills or []) if f.get("save_status") == "EVIDENCE_QUANTITY_REGRESSION")
    if _LAST_SAVE_FILLS_ERROR:
        return {"status": "DB_ERROR", "inserted_count": int(inserted or 0), "updated_count": 0,
                "unchanged_count": 0, "regression_count": regression_count, "error": _LAST_SAVE_FILLS_ERROR}
    if regression_count:
        return {"status": "EVIDENCE_QUANTITY_REGRESSION", "inserted_count": int(inserted or 0),
                "updated_count": 0, "unchanged_count": max(0, len(fills or []) - int(inserted or 0) - regression_count),
                "regression_count": regression_count}
    return {"status": "OK", "inserted_count": int(inserted or 0), "updated_count": 0,
            "unchanged_count": max(0, len(fills or []) - int(inserted or 0)), "regression_count": 0}
'''
    new_result = '''def save_fills_with_result(fills: list[dict], trade_date: str | None = None) -> dict:
    """Structured fill-save result for close/reconcile callers."""
    inserted = save_fills(fills, trade_date=trade_date)
    terminal_statuses = {
        "EVIDENCE_QUANTITY_REGRESSION",
        "EVIDENCE_QUANTITY_CONFLICT",
        "EVIDENCE_QUANTITY_OVERFLOW",
        "FILL_ACCOUNTING_INVARIANT_FAILED",
        "RECONCILE_UPDATE_FAILED",
        "FILL_ORDER_NOT_FOUND",
        "RECONCILE_ORDER_IDENTITY_REQUIRED",
        "RECONCILE_TRADE_DATE_REQUIRED",
        "RECONCILE_QTY_EVIDENCE_MISSING",
    }
    status_counts: dict[str, int] = {}
    for f in fills or []:
        status = str(f.get("save_status") or "")
        if status in terminal_statuses:
            status_counts[status] = status_counts.get(status, 0) + 1
    regression_count = status_counts.get("EVIDENCE_QUANTITY_REGRESSION", 0)
    if _LAST_SAVE_FILLS_ERROR:
        return {"status": "DB_ERROR", "inserted_count": int(inserted or 0), "updated_count": 0,
                "unchanged_count": 0, "regression_count": regression_count,
                "error_status_counts": status_counts, "error": _LAST_SAVE_FILLS_ERROR}
    if status_counts:
        priority = [
            "EVIDENCE_QUANTITY_OVERFLOW",
            "EVIDENCE_QUANTITY_CONFLICT",
            "EVIDENCE_QUANTITY_REGRESSION",
            "FILL_ACCOUNTING_INVARIANT_FAILED",
            "RECONCILE_UPDATE_FAILED",
            "FILL_ORDER_NOT_FOUND",
            "RECONCILE_ORDER_IDENTITY_REQUIRED",
            "RECONCILE_TRADE_DATE_REQUIRED",
            "RECONCILE_QTY_EVIDENCE_MISSING",
        ]
        status = next((s for s in priority if status_counts.get(s)), next(iter(status_counts)))
        return {"status": status, "inserted_count": int(inserted or 0),
                "updated_count": 0,
                "unchanged_count": max(0, len(fills or []) - int(inserted or 0) - sum(status_counts.values())),
                "regression_count": regression_count, "error_status_counts": status_counts}
    return {"status": "OK", "inserted_count": int(inserted or 0), "updated_count": 0,
            "unchanged_count": max(0, len(fills or []) - int(inserted or 0)), "regression_count": 0,
            "error_status_counts": {}}
'''
    replace_once(path, old_result, new_result)


def patch_tests() -> None:
    path = ROOT / "tests/us/test_us_postgres_integration_real.py"
    block = r'''

def test_real_postgres_save_fills_actual_conflict_and_overflow_are_atomic(pg_engine):
    from sqlalchemy import text
    import trader.us.db.repos as repos

    _seed_order(pg_engine, key="K3", order_no="C1", qty_requested=10, qty_filled=7)
    conflict = repos.save_fills_with_result([{
        "symbol": "AMD", "exchange": "NASDAQ", "side": "SELL", "qty": 3,
        "price_usd": 99, "order_no": "C1", "client_order_key": "K3",
        "cumulative_filled_qty": 3, "requested_qty": 10,
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "cumulative_filled_qty": 3, "requested_qty": 10, "is_synthetic": False},
    }], trade_date="2026-07-16")
    assert conflict["status"] == "EVIDENCE_QUANTITY_CONFLICT"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT qty_filled FROM us_orders WHERE order_no='C1'")).scalar_one() == 7
        assert conn.execute(text("SELECT count(*) FROM us_fills WHERE order_no='C1' AND NOT COALESCE((meta->>'is_synthetic')::boolean,false)")).scalar_one() == 0
        assert conn.execute(text("SELECT COALESCE((meta->>'accounting_active')::boolean,true) FROM us_fills WHERE order_no='C1'")).scalar_one() is True

    _seed_order(pg_engine, key="K4", order_no="OFL", qty_requested=10, qty_filled=0)
    overflow = repos.save_fills_with_result([{
        "symbol": "AMD", "exchange": "NASDAQ", "side": "SELL", "qty": 11,
        "price_usd": 101, "order_no": "OFL", "client_order_key": "K4",
        "cumulative_filled_qty": 11, "requested_qty": 10,
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "cumulative_filled_qty": 11, "requested_qty": 10, "is_synthetic": False},
    }], trade_date="2026-07-16")
    assert overflow["status"] == "EVIDENCE_QUANTITY_OVERFLOW"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT qty_filled FROM us_orders WHERE order_no='OFL'")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) FROM us_fills WHERE order_no='OFL' AND NOT COALESCE((meta->>'is_synthetic')::boolean,false)")).scalar_one() == 0
'''
    append_once(path, "test_real_postgres_save_fills_actual_conflict_and_overflow_are_atomic", block)


def main() -> None:
    patch_repos()
    patch_tests()
    restore_reports_from_base()
    # Remove this temporary staging script and workflow before the verified commit.
    (ROOT / "scripts/pr68_final_atomic_cleanup.py").unlink(missing_ok=True)
    (ROOT / ".github/workflows/pr68-final-atomic-cleanup.yml").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
