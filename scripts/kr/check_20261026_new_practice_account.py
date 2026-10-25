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
            from scripts.reset_practice_account_state import (
                _extract_cash_krw, _extract_holdings_count, _is_stub_balance_snapshot,
            )
            kis = KisAPI()
            if get_account_key(env=env, kis=kis) != get_account_key(env=env):
                raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_IDENTITY_MISMATCH")
            raw = kis.get_balance_cached(force=True)
            snapshot = raw[0] if isinstance(raw, tuple) else raw
            if (
                not isinstance(snapshot, dict)
                or _is_stub_balance_snapshot(snapshot)
                or str(snapshot.get("rt_cd") or "") != "0"
            ):
                raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_BALANCE_NOT_PROVEN")
            cash = _extract_cash_krw(snapshot)
            holdings = _extract_holdings_count(snapshot)
            if cash is None or abs(cash - expected) > max(1_000_000, int(expected * 0.01)):
                raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_CASH_NOT_100M")
            if holdings != 0:
                raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_HOLDINGS_NOT_EMPTY")
            result["kis_balance_status"] = "AUTHENTIC_PRACTICE_100M_CASH_NO_HOLDINGS"
            result["kis_cash_krw"] = cash
            result["kis_holdings_count"] = holdings
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
