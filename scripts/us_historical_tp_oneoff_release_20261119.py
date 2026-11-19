#!/usr/bin/env python3
"""One-off, original-day KIS-verified release of four old US TP reservations.

This is an operator maintenance task, NOT a trading session or broker order path.
It invokes PR #371's ACTIVE release engine without bypassing any evidence guard.
Not intended for scheduled deployment or inclusion in dual-agent.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

EXPECTED = {
    ("2026-11-04", "HCA", "tp3"): 1,
    ("2026-11-04", "KR", "tp2"): 9,
    ("2026-11-11", "KO", "tp2"): 6,
    ("2026-11-16", "PG", "tp3"): 2,
}
REPO = "Jeihyuck/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
BRANCH = "ops/us-original-day-tp-db-release-20261119"
PRACTICE_REST = "https://openapivts.koreainvestment.com:29443"
EVIDENCES = {
    "KIS_ORIGINAL_DAY_NO_ACCEPTED_SELL",
    "KIS_ORIGINAL_DAY_ONLY_SETTLED_PRIOR_TP_SELLS",
}


def validate_event() -> None:
    """Only the named, owner-created, ready-for-review PR can do a DB write."""
    if (
        os.environ.get("GITHUB_EVENT_NAME") != "pull_request"
        or os.environ.get("GITHUB_REPOSITORY") != REPO
        or os.environ.get("US_TP_ONEOFF_RELEASE") != "AUTHORIZED_PR371_PRACTICE_REPAIR"
    ):
        raise RuntimeError("ONEOFF_OPERATOR_EVENT_NOT_AUTHORIZED")
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as fh:
        event = json.load(fh)
    pr = event.get("pull_request") or {}
    if not (
        event.get("action") == "ready_for_review"
        and pr.get("draft") is False
        and (pr.get("head") or {}).get("ref") == BRANCH
        and ((pr.get("head") or {}).get("repo") or {}).get("full_name") == REPO
        and (pr.get("base") or {}).get("ref") == "dual-agent"
        and (pr.get("user") or {}).get("login") == "Jeihyuck"
    ):
        raise RuntimeError("ONEOFF_OPERATOR_PR_SCOPE_NOT_AUTHORIZED")


def seal_broker_order_gates() -> None:
    """Leave authentication/GET enabled, prevent ALL orders/cancels."""
    if os.environ.get("KIS_REST_URL", "").rstrip("/") != PRACTICE_REST:
        raise RuntimeError("ONEOFF_REQUIRES_EXACT_PRACTICE_KIS_ENDPOINT")
    os.environ.update({
        "KIS_ENV": "practice", "WSL_RUN_MARKET": "US", "TRADING_REGION": "US",
        "OFFLINE_MODE": "0", "US_OFFLINE_MODE": "0", "KIS_HTTP_BLOCK": "0",
        "US_ORDER_ARMED": "0", "US_LIVE_TRADING_ENABLED": "0",
        "LIVE_TRADING_ENABLED": "0", "US_PAPER_TRADING_ENABLED": "0",
        "US_KIS_ORDER_ALLOWED": "0", "DISABLE_REAL_TRADING": "1",
        "DISABLE_LIVE_TRADING": "1", "ALLOW_REAL_ORDER": "0", "DRY_RUN": "1",
    })
    if os.environ.get("PGOPTIONS", "").find("default_transaction_read_only=on") != -1:
        raise RuntimeError("ONEOFF_CANNOT_APPLY_WITH_READ_ONLY_DATABASE")


def validate_candidates(rows: list[dict]) -> None:
    """A changed/missing/extra target aborts BEFORE any KIS lookup or DB write."""
    from trader.us.execution.historical_tp_resolution import eligible, number
    found = {}
    for row in rows:
        key = (str(row["trade_date"]), row["symbol"], row["stage"])
        if key in found:
            raise RuntimeError("ONEOFF_DUPLICATE_STAGE_IN_ACTIVE_EPOCH")
        if (
            key not in EXPECTED
            or not eligible(row)
            or number(row.get("requested_qty")) != EXPECTED[key]
        ):
            raise RuntimeError("ONEOFF_UNEXPECTED_ACTIVE_TP_CANDIDATE")
        found[key] = row
    if set(found) != set(EXPECTED):
        raise RuntimeError("ONEOFF_TARGET_SET_CHANGED_NO_WRITE")


class OriginalDayGetOnly:
    """Broker facade cannot expose order/cancel methods."""
    def __init__(self, provider):
        self._provider = provider

    def fork_for_stage(self, **kwargs):
        return OriginalDayGetOnly(self._provider.fork_for_stage(**kwargs))

    def get_original_day_order_snapshot(self, source_date):
        return self._provider.get_original_day_order_snapshot(source_date)


def run() -> int:
    validate_event()
    seal_broker_order_gates()
    logging.basicConfig(level=logging.WARNING)
    from sqlalchemy import text
    from trader.us.data_provider import USDataProvider
    from trader.us.db.repos import _get_engine_or_none
    from trader.us.execution.historical_tp_resolution import (
        CANDIDATES_SQL, _scope, metadata, resolve_historical_tp_at_boundary
    )

    engine = _get_engine_or_none()
    if engine is None or not str(engine.dialect.name).startswith("postgres"):
        raise RuntimeError("ONEOFF_DURABLE_POSTGRES_MISSING")
    trade_date = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    with engine.connect() as conn:
        scope = _scope(conn)
        rows = [dict(r) for r in conn.execute(
            text(CANDIDATES_SQL),
            {"epoch": scope["trading_epoch_id"], "today": trade_date},
        ).mappings().all()]
        validate_candidates(rows)
    original = {
        (str(r["trade_date"]), r["symbol"], r["stage"]): r
        for r in rows
    }

    provider = OriginalDayGetOnly(USDataProvider(offline=False, env="practice"))
    results = []
    for source_day in sorted({k[0] for k in EXPECTED}):
        # Operator-invoked CLOSE recovery path: only the existing PR371
        # evidence-driven reservation release; no broker trading authority.
        result = resolve_historical_tp_at_boundary(
            provider=provider, trade_date=trade_date, phase="CLOSE",
            apply=True, budget_sec=30, source_trade_date=source_day,
        )
        results.append({
            "source_day": source_day,
            "status": result["status"], "checked": result["checked"],
            "released": result["released"],
            "unresolved": result["unresolved"],
            "source_days_queried": result["source_days_queried"],
        })
        if result["status"] != "OK":
            # Do not continue applying to other source-days after a failure.
            break

    verified = []
    with engine.connect() as conn:
        active = _scope(conn)
        if active != scope:
            raise RuntimeError("ONEOFF_ACTIVE_ACCOUNT_EPOCH_CHANGED")
        for key in sorted(EXPECTED):
            row = original[key]
            current = conn.execute(text("""
                SELECT * FROM us_profit_capture_lifecycle
                WHERE trade_date=:day AND symbol=:symbol AND stage=:stage
                AND position_lifecycle_id=:cycle AND trading_epoch_id=:epoch
            """), {"day": key[0], "symbol": key[1], "stage": key[2],
                    "cycle": row["position_lifecycle_id"],
                    "epoch": scope["trading_epoch_id"]}).mappings().one_or_none()
            m = metadata(dict(current)) if current else {}
            audit_count = conn.execute(text("""
                SELECT count(*) FROM us_order_events
                WHERE client_order_key=:key AND trading_epoch_id=:epoch
                  AND event_type='TP_HISTORICAL_BROKER_ABSENCE_VERIFIED'
            """), {"key": row["client_order_key"],
                    "epoch": scope["trading_epoch_id"]}).scalar_one()
            orders = conn.execute(text("""
                SELECT count(*) FROM us_orders WHERE client_order_key=:key
            """), {"key": row["client_order_key"]}).scalar_one()
            fills = conn.execute(text("""
                SELECT count(*) FROM us_fills WHERE client_order_key=:key
            """), {"key": row["client_order_key"]}).scalar_one()
            verified.append({
                "symbol": key[1], "source_trade_date": key[0], "stage": key[2],
                "status": current["stage_status"] if current else "MISSING",
                "evidence": m.get("last_evidence_type"),
                "audit_observation_count": int(audit_count),
                "original_key_order_count": int(orders),
                "original_key_fill_count": int(fills),
            })

    success = bool(
        len(results) == 3
        and sum(x["released"] for x in results) == 4
        and all(x["status"] == "OK" for x in results)
        and all(v["status"] == "FAILED"
                and v["evidence"] in EVIDENCES
                and v["audit_observation_count"] >= 1
                and v["original_key_order_count"] == 0
                and v["original_key_fill_count"] == 0
                for v in verified)
    )
    output = {
        "oneoff_db_release": "SUCCESS" if success else "UNRESOLVED",
        "scope": "US_STANDARD_ACTIVE_PRACTICE_ONLY",
        "no_kis_orders_or_cancels": True,
        "original_stage_releases": sum(x["released"] for x in results),
        "details": verified,
        "source_day_checks": results,
        "current_tp_retries_submitted": 0,
    }
    print(json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False))
    return 0 if success else 3


if __name__ == "__main__":
    raise SystemExit(run())
