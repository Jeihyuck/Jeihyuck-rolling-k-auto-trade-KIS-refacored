#!/usr/bin/env python3
"""Read-only first trading day KR practice epoch gate.

Called from shared deployment preflight after .env was read and the checkout
was pinned. No token, broker HTTP, reset, epoch creation, or DB write.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from trader.account_state import get_account_key, resolve_env_name
from trader.db.engine import make_engine
from trader.kr.new_paper_epoch_preflight import (
    KrPracticeReadinessError, verify_kr_practice_first_day,
)


def main() -> int:
    env = resolve_env_name()
    trade_str = (
        os.getenv("NULLIM_TRADE_DATE") or os.getenv("KR_TRADE_DATE")
        or datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()
    )
    try:
        from datetime import date
        trading_day = date.fromisoformat(trade_str)
        expected = int(str(os.getenv("EXPECTED_PRACTICE_CAPITAL_KRW") or "0").replace(",",""))
        maximum = int(str(os.getenv("PAPER_MAX_CAPITAL_KRW") or "0").replace(",",""))
        result = verify_kr_practice_first_day(
            make_engine(),
            env=env,
            account_id=get_account_key(env=env),
            trade_date=trading_day,
            expected_capital_krw=expected,
            max_capital_krw=maximum,
        )
    except (KrPracticeReadinessError, ValueError, RuntimeError) as exc:
        # Never print raw account identifiers or credentials; the root cause
        # stays available through an unambiguous result code.
        reason = str(exc)
        if reason.startswith("KR_NEW_ACCOUNT_"):
            print(f"[KR_NEW_ACCOUNT][FAIL] reason={reason} action=BLOCK_TRADING",file=sys.stderr)
        else:
            print(f"[KR_NEW_ACCOUNT][FAIL] reason={type(exc).__name__} action=BLOCK_TRADING",file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"[KR_NEW_ACCOUNT][FAIL] reason={type(exc).__name__} action=BLOCK_TRADING",file=sys.stderr)
        return 2
    # The epoch is intentionally not printed in scheduler logs.
    print(json.dumps({
        "status":result["status"],
        "capital_krw":result.get("capital_krw"),
        "portfolio_epochs":result.get("pb1_portfolio_epoch_count"),
    },ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
