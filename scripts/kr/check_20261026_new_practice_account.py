#!/usr/bin/env python3
"""Read-only first trading day KR practice epoch gate.

Called from shared deployment preflight after .env was read and the checkout
was pinned. No token, broker HTTP, reset, epoch creation, or DB write.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from trader.account_state import get_account_key, resolve_env_name
from trader.db.engine import make_engine
from trader.kr.new_paper_epoch_preflight import (
    KrPracticeReadinessError, verify_kr_practice_first_day,
    verify_fresh_kr_practice_balance,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only new KR practice account preflight")
    parser.add_argument("--verify-kis", action="store_true",
                        help="Also fetch authentic KIS practice balance (read-only)")
    args = parser.parse_args()
    env = resolve_env_name()
    trade_str = (
        os.getenv("NULLIM_TRADE_DATE") or os.getenv("KR_TRADE_DATE")
        or datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()
    )
    try:
        from datetime import date
        trading_day = date.fromisoformat(trade_str)
        # The account's active epoch may exist while KIS_ENV accidentally
        # still points to a *real* or previous-account API configuration.
        # Never let the first-day KR practice gate validate only DB scope.
        if trading_day == date(2026, 10, 26) and (
            env != "practice"
            or str(os.getenv("KIS_ENV") or "").strip().lower() != "practice"
        ):
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_ENV_NOT_PRACTICE")
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
        if args.verify_kis:
            if result.get("status") != "KR_NEW_ACCOUNT_EPOCH_AND_100M_OK":
                raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_DATE_NOT_APPLICABLE")
            from trader.kis_wrapper import KisAPI
            # KisAPI combines page results into output1/output2 and drops
            # rt_cd on success. A fresh API read with explicit error/stub
            # detection is authoritative; cached or fabricated results are not.
            kis = KisAPI()
            if get_account_key(env=env, kis=kis) != get_account_key(env=env):
                raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_IDENTITY_MISMATCH")
            snapshot, source = kis.get_balance_cached(
                force=True, return_source=True,
            )
            result.update(verify_fresh_kr_practice_balance(
                snapshot, source=source, expected_capital_krw=expected,
            ))
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
        "kis_balance_status":result.get("kis_balance_status", "NOT_CHECKED"),
        "kis_cash_krw":result.get("kis_cash_krw"),
        "kis_holdings_count":result.get("kis_holdings_count"),
    },ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
