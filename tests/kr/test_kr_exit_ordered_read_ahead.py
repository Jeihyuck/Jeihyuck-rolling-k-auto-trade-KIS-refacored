"""Deterministic read-ahead concurrency and order semantics for protective SELL.

No real KIS, network, DB, scheduler, or order placement.
"""
from __future__ import annotations

from threading import Event, Thread

import pytest

from trader.kr.exit_io_pipeline import ordered_exit_ohlcv


def test_next_ohlcv_is_fetched_during_previous_serial_sell_and_order_is_unchanged():
    started = Event()
    allow_next = Event()
    actual = []
    quote_reads = []

    def fetch(code):
        quote_reads.append(code)
        if code == "B":
            started.set()
            if not allow_next.wait(timeout=3):
                raise AssertionError("test next read was never released")
        return code + "_bars"

    def run():
        for code, bars in ordered_exit_ohlcv(
            ["A", "B", "C"], needs_history=lambda _: True,
            fetch=fetch, empty=lambda: None, enabled=True,
        ):
            if code == "A":
                assert started.wait(timeout=3), "next quote must prefetch during A SELL"
                allow_next.set()
            actual.append((code, bars))
    worker = Thread(target=run, daemon=True)
    worker.start()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert actual == [("A", "A_bars"), ("B", "B_bars"), ("C", "C_bars")]
    assert quote_reads == ["A", "B", "C"]


@pytest.mark.parametrize("enabled", [False, True])
def test_same_day_holdings_do_not_trigger_history_and_order_preserved(enabled):
    reads = []
    result = list(ordered_exit_ohlcv(
        [("A", True), ("B", False), ("C", True)],
        needs_history=lambda h: h[1],
        fetch=lambda h: reads.append(h[0]) or h[0] + "_bars",
        empty=lambda: "same_day_no_fetch",
        enabled=enabled,
    ))
    assert result == [
        (("A", True), "A_bars"),
        (("B", False), "same_day_no_fetch"),
        (("C", True), "C_bars"),
    ]
    assert reads == ["A", "C"]


def test_history_fetch_failure_never_forges_empty_safe_policy_input():
    def fail(code):
        if code == "B":
            raise ValueError("quote provider failed")
        return code + "_bars"
    iterator = ordered_exit_ohlcv(
        ["A", "B"], needs_history=lambda _: True,
        fetch=fail, empty=lambda: None, enabled=True,
    )
    assert next(iterator) == ("A", "A_bars")
    with pytest.raises(ValueError, match="quote provider failed"):
        next(iterator)


def test_disabled_mode_does_not_read_next_symbol_before_current_sell():
    reads = []
    iterator = ordered_exit_ohlcv(
        ["A", "B"], needs_history=lambda _: True,
        fetch=lambda code: reads.append(code) or code,
        empty=lambda: None, enabled=False,
    )
    assert next(iterator) == ("A", "A")
    assert reads == ["A"]
    assert next(iterator) == ("B", "B")
    assert reads == ["A", "B"]


def test_real_pb1_exit_pass_keeps_one_serial_order_decision_per_holding(monkeypatch):
    from types import SimpleNamespace
    import pandas as pd
    from trader.pb1_engine import PB1Engine

    monkeypatch.setenv("KR_EXIT_ORDERED_OHLCV_READAHEAD", "1")
    submitted = []
    fetched = []

    class Holding:
        def __init__(self, code, holding_days):
            self.code, self.holding_qty, self.holding_days = code, 1, holding_days

        def to_position_dict(self):
            return {"code": self.code, "qty": 1, "holding_days": self.holding_days,
                    "market": "KOSPI", "avg_buy_price": 10000, "last_price": 10000}

    def fetch(code):
        fetched.append(code)
        return pd.DataFrame(), {"source": "offline_stub"}

    def decide(pos, features, df, window):
        submitted.append((pos["code"], window))
        return {"code": pos["code"], "exit_ok": False, "orderable_qty": 1,
                "submitted": 0, "submit_attempted": 0,
                "primary_reason": "NO_EXIT_SIGNAL"}

    ctx = SimpleNamespace(
        _exit_holdings_meta={"source": "KIS_BALANCE"},
        window_internal="day", _display_code=lambda code: code,
        _fetch_exit_ohlcv=fetch, _plan_exit_event=decide,
    )
    holdings = [Holding("111111", 1), Holding("222222", 1), Holding("333333", 0)]
    result = PB1Engine._run_exit_always(
        ctx, holdings_for_exit=holdings, marks_fallback={},
    )
    assert fetched == ["111111", "222222"]
    assert submitted == [("111111", "manage"), ("222222", "manage"), ("333333", "manage")]
    assert [r["code"] for r in result] == ["111111", "222222", "333333"]
    assert ctx._exit_summary_payload["checked"] == 3
    assert ctx._exit_summary_payload["submitted"] == 0
