#!/usr/bin/env python3
"""US353 practice KIS GET-only evidence; never posts orders or writes a DB.

Do not treat an empty historical KIS response as proof of order nonexistence.
KisUSClient.get_us_fills_today enforces the broker cursor/page contract.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from zoneinfo import ZoneInfo


def assert_readonly():
    expected = dict(KIS_ENV="practice", WSL_RUN_MARKET="US",
                    US_ORDER_ARMED="0", NO_TRADE="1", DRY_RUN="1")
    if any(os.getenv(k) != v for k, v in expected.items()):
        raise RuntimeError("READONLY_PRACTICE_US_CHILD_REQUIRED")


def selected_row(raw):
    """Whitelist raw evidence; never print CANO, tokens or complete KIS rows."""
    def first(*fields):
        return next((str(raw[k])[:80] for k in fields
                     if raw.get(k) not in (None, "")), "")
    return dict(symbol=first("pdno","ovrs_pdno","PDNO"),
                order_no=first("odno","ODNO","ord_no"),
                broker_date=first("ord_dt","ORD_DT"),
                broker_time=first("ord_tmd","ORD_TMD"),
                raw_side=first("sll_buy_dvsn_cd","sll_buy_dvsn"),
                raw_status=first("ord_stat_cd","ccld_nccs_dvsn"),
                requested=first("ft_ord_qty","ord_qty"),
                filled=first("ft_ccld_qty","tot_ccld_qty","ccld_qty"),
                remaining=first("nccs_qty","ord_remn_qty"))


def collect(client, start, end, normalizer, account_key):
    assert_readonly()
    raw = client.get_us_balance(force_refresh=True)
    normalized = normalizer(raw)
    if not (raw.get("balance_authoritative") is True
            and raw.get("balance_complete") is True
            and normalized.get("balance_parse_status") == "OK"):
        raise RuntimeError("KIS_BALANCE_NOT_COMPLETE")
    positions = [dict(symbol=p.get("symbol"), qty=p.get("qty"),
                      orderable_qty=p.get("orderable_qty"),
                      exchange=p.get("exchange"))
                 for p in normalized.get("positions", [])]
    orders = []
    day = start
    while day <= end:
        if day.weekday() < 5:
            rows = client.get_us_fills_today(trade_date=day.isoformat())
            if not isinstance(rows, list):
                raise RuntimeError("KIS_ORDER_RESPONSE_UNVERIFIED")
            orders.append(dict(us_trade_date=day.isoformat(), count=len(rows),
                               cursor_validated_by_client=True,
                               pages="NOT_EXPOSED",
                               rows=[selected_row(x) for x in rows]))
        day += timedelta(days=1)
    return dict(status="READONLY_BROKER_GET_SUCCEEDED",
                collected_utc=datetime.now(timezone.utc).isoformat(),
                account_hash_prefix=hashlib.sha256(
                    str(account_key).encode()).hexdigest()[:12],
                balance_exchanges=raw.get("queried_exchanges", []),
                balance_complete=True, positions=positions,
                orders=orders, orders_posted=0, db_writes=0,
                warning="Empty historical query is not original order absence proof")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--confirm-readonly", action="store_true")
    args = parser.parse_args()
    try:
        assert_readonly()
        if not args.confirm_readonly:
            raise RuntimeError("EXPLICIT_READONLY_CONFIRMATION_REQUIRED")
        first = date.fromisoformat(args.from_date)
        last = date.fromisoformat(args.to_date)
        if not (first <= last <= datetime.now(
                ZoneInfo("America/New_York")).date()):
            raise RuntimeError("DATE_RANGE_INVALID")
        if (last - first).days > 35:
            raise RuntimeError("DATE_RANGE_EXCEEDS_36_DAYS")
        from trader.account_state import get_account_key
        from trader.us.execution.kis_us_client import KisUSClient
        from trader.us.data_provider import normalize_us_balance
        report = collect(KisUSClient(env="practice"), first, last,
                         normalize_us_balance, get_account_key(env="practice"))
    except Exception as exc:
        # No exception message can be forwarded from KIS, which might include
        # a credential, full account number or complete HTTP request.
        print(json.dumps(dict(status="NOT_VERIFIED",
                              failure_class=type(exc).__name__,
                              orders_posted=0, db_writes=0)))
        return 42
    print(json.dumps(report, sort_keys=True, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
