"""Offline KR/US settlement ledger verifier (read-only).

Usage:
  NULLIM_SETTLEMENT_AUDIT_DSN=postgresql+psycopg://... \
      python -m trader.settlement.audit --market KR --env practice \
      --epoch <active-epoch-id> --account-scope <hashed-account-scope>

Exit status 0 means *ledger only* is internally consistent, NOT live broker
parity or permission to activate a second economic writer.
"""
from __future__ import annotations

import argparse
import json
import os

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .health import check_settlement_health
from .release_gate import assert_no_unguarded_writer_env


def audit_snapshot(
    engine: Engine, *, market: str, env: str,
    trading_epoch_id: str, account_scope: str,
) -> tuple[int, dict]:
    health = check_settlement_health(
        engine, market=market, env=env, trading_epoch_id=trading_epoch_id,
        account_scope=account_scope,
    )
    result = {
        "market": health.market,
        "env": health.env,
        "trading_epoch_id": health.trading_epoch_id,
        "status": health.status,
        "application_count": health.application_count,
        "evidence_count": health.evidence_count,
        "price_pending_count": health.price_pending_count,
        "problems": list(health.problems),
        "report_scope": "LEDGER_ONLY_NOT_BROKER_PARITY",
        "writer_activation_allowed": False,
    }
    return (0 if health.ledger_consistent else 2), result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", required=True, choices=["KR", "US"])
    parser.add_argument("--env", required=True)
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--account-scope", required=True, help="hashed scope, never raw account number")
    args = parser.parse_args(argv)
    assert_no_unguarded_writer_env()
    url = os.environ.get("NULLIM_SETTLEMENT_AUDIT_DSN")
    if not url:
        print(json.dumps({
            "status": "LEDGER_UNAVAILABLE", "reason": "NULLIM_SETTLEMENT_AUDIT_DSN_MISSING",
            "writer_activation_allowed": False,
        }))
        return 2
    db = sa.create_engine(url)
    try:
        exit_code, result = audit_snapshot(
            db, market=args.market, env=args.env,
            trading_epoch_id=args.epoch, account_scope=args.account_scope,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return exit_code
    finally:
        db.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
