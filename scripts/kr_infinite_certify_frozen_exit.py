"""Verify and optionally certify the 2026-10-27 KR Infinite fully sold cycle.

Default is read-only evidence inspection. This is NOT an order command.
Usage (in the deployed WSL practice repo):
  FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1 KIS_ENV=practice \
    python -m scripts.kr_infinite_certify_frozen_exit \
    --buy-day 20261026 --sell-day 20261027

Review READY_TO_CERTIFY proof, then re-run with --apply only when the
operator explicitly authorizes the metadata repair. All balances and
original KIS daily history are fetched fresh on BOTH invocations.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os


def main(argv=None):
    parser = argparse.ArgumentParser(description="Practice-only verified KR Infinite frozen TP2 repair")
    parser.add_argument("--buy-day", required=True, help="Original BUY YYYYMMDD")
    parser.add_argument("--sell-day", required=True, help="Original TP1+TP2 YYYYMMDD")
    parser.add_argument("--apply", action="store_true", help="Apply the KIS-proven atomic metadata repair")
    args = parser.parse_args(argv)

    if (
        os.getenv("FORCE_HTTP") != "1"
        or os.getenv("NO_TRADE") != "1"
        or os.getenv("DRY_RUN") != "1"
        or os.getenv("KIS_ENV") != "practice"
    ):
        print(json.dumps({"state": "BLOCKED", "reason": "PRACTICE_READ_ONLY_FLAGS_REQUIRED"}))
        return 3
    try:
        buy_day = datetime.strptime(args.buy_day, "%Y%m%d").date()
        sell_day = datetime.strptime(args.sell_day, "%Y%m%d").date()
        if not buy_day < sell_day:
            raise ValueError("invalid_date_order")
    except ValueError:
        print(json.dumps({"state": "BLOCKED", "reason": "ORIGINAL_DATES_INVALID"}))
        return 3
    for key, value in (
        ("LIVE_TRADING_ENABLED", "0"),
        ("KR_LIVE_TRADING_ENABLED", "0"),
        ("KR_ORDER_ARMED", "0"),
        ("DISABLE_LIVE_TRADING", "1"),
        ("DISABLE_REAL_TRADING", "1"),
    ):
        os.environ[key] = value

    from trader.kis_wrapper import KisAPI
    from trader.db.engine import make_engine
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    from trader.kr.infinite.frozen_exit_certification import certify_frozen_infinite_cycle

    engine = make_engine()
    try:
        kis = KisAPI(kis_env="practice")
        if str(kis.env).lower() != "practice":
            raise RuntimeError("NOT_PRACTICE_KIS")
        balance, source = kis.get_balance_cached(force=True, return_source=True)
        pages = {
            day.strftime("%Y%m%d"): _daily_ccld_all_pages_for_date(kis, day)
            for day in (buy_day, sell_day)
        }
        proof = certify_frozen_infinite_cycle(
            engine=engine, broker_balance=balance,
            balance_source=source, original_pages=pages, apply=args.apply,
        )
        # Never print account identifiers, passwords, or bearer tokens.
        report = {
            key: str(val) if key == "last_kis_order_time_utc" else val
            for key, val in proof.items()
            if key not in {"mirror_position_id"}
        }
        print(json.dumps(report, ensure_ascii=False, default=str))
        return 0 if proof["state"] in {"READY_TO_CERTIFY", "CERTIFIED"} else 2
    except Exception as exc:
        print(json.dumps({"state": "BLOCKED", "reason": type(exc).__name__}))
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
