"""Read-only broker replay fixtures for the 2026-10-21/22 KRX defects.

No network, live KIS client, or production DB is used.
"""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from trader.reconcile_kis import (
    _parse_date_time,
    _verified_kr_cumulative_buy_evidence,
    _verified_kr_cumulative_sell_evidence,
)


def _source(side, quantity, odno):
    return {"code": "251970", "side": side, "qty": quantity, "kis_odno": odno,
            "order_id": "replay-order"}


def _row(side, qty, total, odno, *, trade_day="20261021"):
    return {"pdno": "251970", "sll_buy_dvsn_cd": "01" if side == "SELL" else "02",
            "odno": odno, "ord_dt": trade_day, "ord_tmd": "151620",
            "tot_ccld_qty": str(qty), "tot_ccld_amt": str(total),
            "avg_prvs": str(total / qty)}


def test_kis_daily_trade_time_is_kst_not_naive_utc():
    morning = _parse_date_time({"ord_dt": "20261021", "ord_tmd": "093900"})
    closing = _parse_date_time({"ord_dt": "20261021", "ord_tmd": "151620"})
    assert morning.utcoffset().total_seconds() == 9 * 3600
    assert morning.isoformat() == "2026-10-21T09:39:00+09:00"
    assert closing.astimezone(ZoneInfo("UTC")).isoformat() == "2026-10-21T06:16:20+00:00"
    assert closing.date() == date(2026, 10, 21)


@pytest.mark.parametrize("qty,total,requested,odno", [
    (10, 685000, 17, "0000040226"),   # 251970 day-one partial, unfilled remainder 7
    (52, 836180, 79, "0000040083"),  # 253590 day-two partial, unfilled remainder 27
    (81, 1091430, 81, "0000040070"), # 144960 whole SELL, regression
])
def test_priced_cumulative_sell_accepts_broker_partial_not_quote(qty,total,requested,odno):
    row = _row("SELL", qty, total, odno)
    source = _source("SELL", requested, odno)
    source["code"] = row["pdno"]
    evidence = _verified_kr_cumulative_sell_evidence(
        row, source_order=source, side="SELL", trade_date=date(2026,10,21),
        kis_odno=odno, env="practice",
    )
    assert evidence["qty"] == qty
    assert evidence["notional"] == float(total)
    assert evidence["kind"] == "KIS_ORDER_CUMULATIVE_EXECUTION"
    row["tot_ccld_amt"] = "0"
    assert _verified_kr_cumulative_sell_evidence(
        row, source_order=source, side="SELL", trade_date=date(2026,10,21),
        kis_odno=odno, env="practice",
    ) is None


@pytest.mark.parametrize("first_qty,first_amt,next_qty,next_amt,odno", [
    (15, 1026000, 17, 1162800, "0000013196"),
    (22, 539000, 48, 1176000, "0000013221"),
])
def test_cumulative_buy_has_one_stable_evidence_id(first_qty,first_amt,next_qty,next_amt,odno):
    source = _source("BUY", next_qty, odno)
    first = _verified_kr_cumulative_buy_evidence(
        _row("BUY", first_qty, first_amt, odno), source_order=source, side="BUY",
        trade_date=date(2026, 10, 21), kis_odno=odno, env="practice",
    )
    second = _verified_kr_cumulative_buy_evidence(
        _row("BUY", next_qty, next_amt, odno), source_order=source, side="BUY",
        trade_date=date(2026, 10, 21), kis_odno=odno, env="practice",
    )
    assert first["evidence_id"] == second["evidence_id"]
    assert next_qty - first_qty in (2, 26)
    assert second["notional"] - first["notional"] == next_amt - first_amt


def test_no_cumulative_without_exact_durable_source_or_broker_notional():
    row = _row("BUY", 17, 1162800, "0000013196")
    assert _verified_kr_cumulative_buy_evidence(
        row, source_order=None, side="BUY", trade_date=date(2026,10,21),
        kis_odno="0000013196", env="practice",
    ) is None
    row["tot_ccld_amt"] = "0"
    assert _verified_kr_cumulative_buy_evidence(
        row, source_order=_source("BUY",17,"0000013196"), side="BUY",
        trade_date=date(2026,10,21), kis_odno="0000013196", env="practice",
    ) is None
