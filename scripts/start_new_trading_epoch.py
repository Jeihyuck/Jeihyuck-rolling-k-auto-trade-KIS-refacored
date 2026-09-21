#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os

from trader.account_state import get_account_key, get_masked_account_key, resolve_env_name
from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.trading_epoch import start_new_trading_epoch


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if os.getenv("TRADING_EPOCH_CONFIRM") != "YES":
        raise RuntimeError("TRADING_EPOCH_CONFIRM=YES required")
    if os.getenv("TRADING_EPOCH_BROKER_FLAT_CONFIRMED") != "YES":
        raise RuntimeError(
            "TRADING_EPOCH_BROKER_FLAT_CONFIRMED=YES required; verify KR/US holdings and pending orders are flat first"
        )
    reason = str(os.getenv("TRADING_EPOCH_REASON") or "").strip()
    if not reason:
        raise RuntimeError("TRADING_EPOCH_REASON required")

    env = resolve_env_name()
    account_id = get_account_key(env=env)
    engine = make_engine()
    run_migrations(engine)
    epoch_id = start_new_trading_epoch(
        engine, env=env, account_id=account_id, reason=reason
    )
    print(json.dumps({
        "status": "TRADING_EPOCH_STARTED",
        "trading_epoch_id": epoch_id,
        "env": env,
        "account": get_masked_account_key(env=env),
        "reason": reason,
        "history_deleted": False,
        "database_replaced": False,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
