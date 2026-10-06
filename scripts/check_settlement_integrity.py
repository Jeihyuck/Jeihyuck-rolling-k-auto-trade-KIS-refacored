"""Read-only KR/US settlement-ledger diagnosis; no KIS call or DB mutation.

Usage:
  python -m scripts.check_settlement_integrity --market KR --env practice --epoch <id>
A zero-application ledger is INACTIVE, not proof that legacy trading is correct.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json

from trader.db.engine import get_engine
from trader.settlement.release_gate import inspect_settlement_ledger


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", choices=("KR", "US"), required=True)
    parser.add_argument("--env", choices=("practice", "real"), required=True)
    parser.add_argument("--epoch", required=True)
    args = parser.parse_args()
    health = inspect_settlement_ledger(
        get_engine(), market=args.market, env=args.env, trading_epoch_id=args.epoch,
    )
    payload = asdict(health)
    payload["status"] = (
        "INTEGRITY_ERROR" if health.findings or not health.readable
        else "NOT_MIGRATED" if health.applications == 0
        else "RECONCILE_REQUIRED" if health.quantity_pending or health.price_pending
        else "LEDGER_CONSISTENT"
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "LEDGER_CONSISTENT" else 2


if __name__ == "__main__":
    raise SystemExit(main())
