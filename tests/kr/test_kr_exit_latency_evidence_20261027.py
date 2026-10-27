from trader.kr.exit_latency_evidence import classify_exit_evaluation_latency


def test_exit_latency_bands_are_disjoint():
    assert classify_exit_evaluation_latency(0) == "OK"
    assert classify_exit_evaluation_latency(10) == "OK"
    assert classify_exit_evaluation_latency(10.1) == "SLOW"
    assert classify_exit_evaluation_latency(30) == "SLOW"
    assert classify_exit_evaluation_latency(30.1) == "CRITICAL"


def test_exit_latency_rejects_invalid_times():
    assert classify_exit_evaluation_latency(-1) == "UNKNOWN"
    assert classify_exit_evaluation_latency(float("nan")) == "UNKNOWN"
    assert classify_exit_evaluation_latency(None) == "UNKNOWN"


def test_ordered_exit_history_wait_is_measured_without_lookahead_or_reordering():
    from trader.kr.exit_latency_evidence import timed_ordered_exit_history
    ticks = iter([1., 1.4, 2., 13., 14.])
    def clock():
        return next(ticks)
    observed = list(timed_ordered_exit_history([("holding1", 1), ("holding2", 2)], clock=clock))
    assert [value[1][0] for value in observed] == ["holding1", "holding2"]
    assert abs(observed[0][0] - 0.4) < 1e-9
    assert observed[1][0] == 11.0


def test_empty_ordered_exit_history_does_not_create_dummy_holdings():
    from trader.kr.exit_latency_evidence import timed_ordered_exit_history
    assert list(timed_ordered_exit_history([])) == []


def test_oct26_vinatek_frozen_day_contract_outranks_legacy_swing_book(monkeypatch):
    """Regression against the historical DAY_TRADE -> legacy SWING_BOOK misroute.

    The -2.19% observation is an input scenario from incident analysis, not
    proof of the precise KIS quote at 09:45.
    """
    from trader.pb1_engine import (
        _resolve_position_book, _resolve_position_horizon,
        _resolve_day_protect_exit,
    )
    monkeypatch.setenv("PB1_DAY_PROTECT_ENABLED", "1")
    monkeypatch.setenv("PB1_DAY_STOP_LOSS_PCT", "2.0")
    pos = {
        "code": "126340", "market": "KOSDAQ",
        "qty": 17, "orderable_qty": 17, "avg_buy_price": 113900,
        "stop_price": 104788,
        "entry_exit_plan_json": {
            "entry_style_selected": "ENTRY_MOMENTUM",
            "trade_horizon": "DAY_TRADE", "force_eod_close": True,
        },
        "position_meta": {"book": "SWING_BOOK"},
    }
    assert _resolve_position_horizon(pos) == "DAY_PROTECT"
    assert _resolve_position_book(pos) == "DAY_BOOK"
    event = _resolve_day_protect_exit(
        pos, mark=113900 * (1 - 0.0219), now_hhmm=945,
        ret_pct=-2.19, max_pnl_pct=0.0, stop_hit=False,
    )
    assert event["exit_ok"] is True
    assert event["reason"] == "EXIT_DAY_STOP_LOSS"
    assert event["qty"] == 17


def test_valid_new_swing_contract_not_forced_to_day_book():
    from trader.pb1_engine import _resolve_position_book, _resolve_position_horizon
    pos = {"code": "012750", "entry_exit_plan_json": {
        "entry_style_selected": "ENTRY_MOMENTUM_SWING",
        "trade_horizon": "SWING", "force_eod_close": False,
    }, "position_meta": {"book": "DAY_BOOK"}}
    assert _resolve_position_horizon(pos) == "SWING_CARRY"
    assert _resolve_position_book(pos) == "SWING_BOOK"
