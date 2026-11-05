"""Read-only 0062 broker recovery migration provenance audit.

Schema objects are NOT evidence that schema_migrations recorded 0062.
This script is operator-only: it never applies migrations, changes a claim,
edits order/fill history, starts Stage4, or calls KIS.
"""
from __future__ import annotations

import json
import os
from typing import Any

from sqlalchemy import text


VERSION = "0062_us_portfolio_position_cycle_recovery_scope.sql"
REQUIRED_INDEXES = frozenset({
    "uq_us_orders_broker_account_epoch_day_0062",
    "uq_us_fills_broker_account_epoch_0062",
    "ix_us_orders_recovery_scope_0062",
    "ix_us_claims_recovery_scope_0062",
    "ix_us_stop_debt_recovery_scope_0062",
    "uq_us_portfolio_epoch_owner_0062",
})
SCOPE_FIELDS = {
    "us_orders": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
    "us_order_intents": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
    "us_order_events": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
    "us_fills": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
    "us_positions": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
    "us_position_risk_state": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
    "us_execution_claims": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
    "us_protective_stop_obligations": {"account_id", "portfolio_epoch_id", "position_cycle_id"},
}


def assess_us0062_readiness(
    *, applied_versions: set[str], indexes: set[str],
    columns: dict[str, set[str]], constraint_rows: list[dict],
) -> dict[str, Any]:
    """Pure conservative report; no automatic inference or stamp repair."""
    missing_indices = sorted(REQUIRED_INDEXES - set(indexes))
    missing_columns = sorted(
        f"{table}.{field}"
        for table, fields in SCOPE_FIELDS.items()
        for field in fields - set(columns.get(table, set()))
    )
    present = [
        item for item in constraint_rows
        if item.get("conname") == "ck_us_order_scope_complete_0062"
        and "portfolio_epoch_id IS NULL" in str(item.get("definition") or "")
    ]
    stamped = VERSION in applied_versions
    has_constraint = len(present) == 1
    if not stamped:
        status = "MIGRATION_STAMP_MISSING"
    elif missing_indices or missing_columns or not has_constraint:
        status = "SCHEMA_INCOMPLETE_OR_DRIFTED"
    else:
        status = "PROVENANCE_AND_CATALOG_PRESENT"
    return {
        "status": status,
        "version": VERSION,
        "migration_stamped": stamped,
        "missing_indexes": missing_indices,
        "missing_columns": missing_columns,
        "scope_constraint_present": has_constraint,
        "scope_constraint_validated": (
            bool(present[0].get("convalidated")) if has_constraint else None
        ),
        "auto_repair_performed": False,
        "orders_submitted": 0,
        "claims_released": 0,
    }


def read_us0062_readiness(engine) -> dict[str, Any]:
    """SELECT-only catalog inquiry using authorized, preexisting DB engine."""
    with engine.connect() as conn:
        versions = {
            str(row[0]) for row in conn.execute(text(
                "SELECT version FROM public.schema_migrations"
            )).all()
        }
        indices = {
            str(row[0]) for row in conn.execute(text(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
            )).all()
        }
        cols: dict[str, set[str]] = {}
        for table, field in conn.execute(text(
            "SELECT table_name,column_name FROM information_schema.columns "
            "WHERE table_schema='public'"
        )).all():
            cols.setdefault(str(table), set()).add(str(field))
        constraint = [dict(row) for row in conn.execute(text(
            "SELECT conname,convalidated,pg_get_constraintdef(oid) AS definition "
            "FROM pg_constraint "
            "WHERE conname='ck_us_order_scope_complete_0062'"
        )).mappings().all()]
    return assess_us0062_readiness(
        applied_versions=versions,
        indexes=indices,
        columns=cols,
        constraint_rows=constraint,
    )


def main() -> int:
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--confirm-practice-readonly", action="store_true")
    args = p.parse_args()
    if not args.confirm_practice_readonly or os.getenv("KIS_ENV", "").lower() != "practice":
        print(json.dumps({"status": "BLOCKED_PRACTICE_READONLY_APPROVAL_REQUIRED"}))
        return 42
    try:
        from trader.us.db.repos import _get_engine_or_none
        engine = _get_engine_or_none()
        if engine is None:
            raise RuntimeError("US0062_DB_UNAVAILABLE")
        result = read_us0062_readiness(engine)
    except Exception as exc:
        print(json.dumps({"status": "DB_QUERY_UNVERIFIED", "error_type": type(exc).__name__}))
        return 43
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "PROVENANCE_AND_CATALOG_PRESENT" else 44


if __name__ == "__main__":
    raise SystemExit(main())
