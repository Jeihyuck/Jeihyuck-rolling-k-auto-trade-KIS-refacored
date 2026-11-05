#!/usr/bin/env python3
"""Explicitly approved KIS-practice GET-only v3 ATR PREP cache warmer.

Never calls any order/cancel endpoint. Cache errors cannot assert a SELL
trigger: the v3 Tick reads only independently validated completed bars.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date


_EXCHANGES = {"NASD": "NASDAQ", "NYSE": "NYSE", "AMEX": "AMEX", "NASDAQ": "NASDAQ"}


def _positions_from_complete_practice_balance(payload: dict) -> list[tuple[str, str]]:
    if (not isinstance(payload, dict)
            or payload.get("balance_complete") is not True
            or payload.get("balance_authoritative") is not True):
        raise RuntimeError("US_V3_ATR_BALANCE_INCOMPLETE")
    selected: dict[str, str] = {}
    for row in payload.get("output1") or []:
        if not isinstance(row, dict):
            raise RuntimeError("US_V3_ATR_BALANCE_ROW_INVALID")
        symbol = str(row.get("ovrs_pdno") or row.get("pdno") or "").upper().strip()
        exch = _EXCHANGES.get(str(row.get("ovrs_excg_cd") or "").upper().strip())
        try:
            count = float(row.get("ovrs_cblc_qty") or row.get("holding_qty") or row.get("qty") or 0)
        except (TypeError, ValueError):
            raise RuntimeError("US_V3_ATR_BALANCE_QTY_UNVERIFIED") from None
        if count <= 0:
            continue
        if not symbol or not exch:
            raise RuntimeError("US_V3_ATR_EXCHANGE_UNVERIFIED")
        if symbol == "TQQQ":
            continue
        if symbol in selected and selected[symbol] != exch:
            raise RuntimeError("US_V3_ATR_SYMBOL_EXCHANGE_AMBIGUOUS")
        selected[symbol] = exch
    if len(selected) > 30:
        raise RuntimeError("US_V3_ATR_HOLDING_SET_OVER_BUDGET")
    return sorted(selected.items())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--confirm-practice-readonly", action="store_true")
    args = parser.parse_args()
    date.fromisoformat(args.trade_date)
    if not (
        args.confirm_practice_readonly
        and os.getenv("KIS_ENV", "").lower() == "practice"
        and os.getenv("US_SWING_EXIT_MODE", "") == "V3_ACTIVE_PRACTICE"
        and os.getenv("US_SWING_V3_DEPLOY_APPROVED", "0") == "1"
    ):
        print(json.dumps({"status": "BLOCKED", "reason": "US_V3_PRACTICE_APPROVAL_MISSING"}))
        return 42
    from trader.us.execution.kis_us_client import KisUSClient
    from trader.us.market_calendar import previous_completed_us_session
    from trader.us.pb1.v3_atr_readonly_cache import write_verified_kis_daily_cache

    latest = previous_completed_us_session(args.trade_date).strftime("%Y%m%d")
    results = []
    try:
        broker = KisUSClient(env="practice")
        positions = _positions_from_complete_practice_balance(
            broker.get_us_balance(force_refresh=True)
        )
    except Exception as exc:
        print(json.dumps({
            "status": "NO_ATR_CACHE", "reason": type(exc).__name__,
            "orders_submitted": 0, "cancels_submitted": 0,
        }))
        return 43
    for symbol, exchange in positions:
        try:
            raw = broker.get_us_daily_price_history(
                symbol, exchange, as_of_date=latest, required_bars=80,
                max_pages=6,
            )
            result = write_verified_kis_daily_cache(
                raw_rows=raw, symbol=symbol,
                decision_date=args.trade_date, exchange=exchange,
            )
            results.append(result)
        except Exception as exc:
            # CL/MDLZ/GILD and stale/malformed OHLC remain quarantined.
            results.append({"symbol": symbol, "status": "ATR_UNVERIFIED",
                            "reason": type(exc).__name__})
    print(json.dumps({
        "status": "COMPLETE_WITH_QUARANTINE" if any(
            r["status"] == "ATR_UNVERIFIED" for r in results
        ) else "COMPLETE",
        "trade_date": args.trade_date,
        "checked": len(positions),
        "verified": sum(r["status"] == "KIS_DAILY_COMPLETED_CACHED" for r in results),
        "results": results, "orders_submitted": 0, "cancels_submitted": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
