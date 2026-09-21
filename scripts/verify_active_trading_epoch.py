#!/usr/bin/env python3
from __future__ import annotations

import json

from trader.account_state import get_account_key, get_masked_account_key, resolve_env_name
from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.trading_epoch import active_trading_epoch_id


def main() -> int:
    env = resolve_env_name()
    account_id = get_account_key(env=env)
    engine = make_engine()
    run_migrations(engine)
    epoch_id = active_trading_epoch_id(
        engine, env=env, account_id=account_id, required=True
    )
    print(json.dumps({
        "status": "ACTIVE_TRADING_EPOCH_OK",
        "trading_epoch_id": epoch_id,
        "env": env,
        "account": get_masked_account_key(env=env),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
