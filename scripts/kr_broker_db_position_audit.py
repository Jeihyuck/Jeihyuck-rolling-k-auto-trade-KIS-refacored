"""Manually invoked, read-only KIS-vs-DB KR PB1 position audit.

Usage (WSL, practice account only):
  FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1 KIS_ENV=practice python -m scripts.kr_broker_db_position_audit

No scheduler activation, no broker order POST, and no database writes.
"""
from __future__ import annotations

import json
import hashlib
import os
import sys


def main() -> int:
    # Do not permit the monitoring command to inherit live-order authority.
    if (os.getenv("NO_TRADE") != "1" or os.getenv("DRY_RUN") != "1"
        or os.getenv("KIS_ENV") != "practice" or os.getenv("FORCE_HTTP") != "1"):
        print(json.dumps({"status": "BLOCKED_READONLY_FLAGS_REQUIRED"}))
        return 3
    for key, value in (
        ("KR_ORDER_ARMED", "0"),
        ("LIVE_TRADING_ENABLED", "0"),
        ("KR_LIVE_TRADING_ENABLED", "0"),
        ("DISABLE_LIVE_TRADING", "1"),
    ):
        os.environ[key] = value

    import sqlalchemy as sa
    from trader.account_state import get_account_key
    from trader.db.engine import make_engine
    from trader.db.schema import schema_for_engine
    from trader.db.trading_epoch import active_trading_epoch_id
    from trader.kis_wrapper import KisAPI
    from trader.kr.broker_db_position_audit import (
        compare_pb1_db_to_kis, summarize_pb1_execution_claim_backlog,
    )

    engine = make_engine()
    try:
        schema = schema_for_engine(engine)
        with engine.connect() as conn:
            epoch = active_trading_epoch_id(
                conn, env="practice", account_id=get_account_key(env="practice"),
                required=True,
            )
            if not epoch:
                print(json.dumps({"status": "UNVERIFIED_ACTIVE_EPOCH"}))
                return 2
            db_rows = [
                dict(row) for row in conn.execute(
                    sa.select(
                        schema.positions.c.code,
                        schema.positions.c.qty,
                        schema.positions.c.total_cost,
                        schema.positions.c.status,
                    ).where(sa.and_(
                        schema.positions.c.env == "practice",
                        schema.positions.c.strategy == "pb1_pullback_close",
                        schema.positions.c.trading_epoch_id == str(epoch),
                        schema.positions.c.status == "OPEN",
                    ))
                ).mappings().all()
            ]
            claims = [
                dict(row) for row in conn.execute(
                    sa.select(
                        schema.execution_claims.c.action_key,
                        schema.execution_claims.c.action,
                        schema.execution_claims.c.action_state,
                        schema.execution_claims.c.target_qty,
                        schema.execution_claims.c.cumulative_filled_qty,
                        schema.execution_claims.c.remaining_target_qty,
                        schema.execution_claims.c.trade_date,
                    ).where(sa.and_(
                        schema.execution_claims.c.env == "practice",
                        schema.execution_claims.c.account_id == hashlib.sha256(
                            get_account_key(env="practice").encode("utf-8")
                        ).hexdigest(),
                        schema.execution_claims.c.market == "KR",
                        schema.execution_claims.c.strategy_owner == "PB1",
                        schema.execution_claims.c.trading_epoch_id == str(epoch),
                        schema.execution_claims.c.action_state.in_([
                            "UNCERTAIN", "IN_FLIGHT", "PARTIALLY_SATISFIED",
                            "RETRYABLE", "OPEN",
                        ]),
                    ))
                ).mappings().all()
            ]
            unresolved = [
                dict(row) for row in conn.execute(
                    sa.select(
                        schema.orders.c.code,
                        schema.orders.c.status,
                        schema.orders.c.order_id,
                    ).where(sa.and_(
                        schema.orders.c.env == "practice",
                        schema.orders.c.strategy == "pb1_pullback_close",
                        schema.orders.c.trading_epoch_id == str(epoch),
                        schema.orders.c.side == "SELL",
                        schema.orders.c.status.in_([
                            "ACK_DB_FAILED_RECONCILE_REQUIRED", "UNRESOLVED_ACK",
                            "RECONCILE_PENDING", "PARTIAL_FILLED", "ACKED",
                        ]),
                    ))
                ).mappings().all()
            ]
        kis = KisAPI(kis_env="practice")
        if str(kis.env).lower() != "practice":
            print(json.dumps({"status": "BLOCKED_NONPRACTICE_ACCOUNT"}))
            return 3
        snapshot, source = kis.get_balance_cached(force=True, return_source=True)
        result = compare_pb1_db_to_kis(
            positions=db_rows, broker_snapshot=snapshot, source=source,
            require_economics=True,
        )
        result["unresolved_sell_orders"] = unresolved
        claim_backlog = summarize_pb1_execution_claim_backlog(claims)
        result["execution_claim_backlog"] = claim_backlog
        # A match in holdings does not prove old SELLs' individual fills.
        if unresolved and result["verified_match"]:
            result["status"] = "UNRESOLVED_SELL_RECONCILIATION"
            result["verified_match"] = False
        if claim_backlog["unresolved_count"] and result["verified_match"]:
            result["status"] = "UNRESOLVED_PB1_EXECUTION_CLAIMS"
            result["verified_match"] = False
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0 if result["verified_match"] else 2
    except Exception as exc:
        # Never turn a broker/DB exception into an empty-balance PASS.
        print(json.dumps({"status": "READONLY_AUDIT_FAILED",
                          "exception_type": type(exc).__name__}))
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
