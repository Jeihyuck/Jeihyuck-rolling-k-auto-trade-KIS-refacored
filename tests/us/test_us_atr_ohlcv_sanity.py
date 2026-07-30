from trader.us.universe_builder import _compute_atr_pct


def bars(high=102.0, low=99.0, close=100.0, count=20):
    return [{"date": f"202607{i+1:02d}", "high": high, "low": low, "close": close} for i in range(count)]


def test_invalid_zero_ohlc_is_skipped_but_valid_history_calculates():
    rows = bars()
    rows[5].update(high=0, low=0)
    value, status = _compute_atr_pct(rows, "AAPL", return_status=True)
    assert status == "OK" and 0.01 <= value <= 0.10


def test_all_zero_ohlc_has_no_atr():
    value, status = _compute_atr_pct(bars(high=0, low=0), "SPY", return_status=True)
    assert value is None and status == "INSUFFICIENT_VALID_TR"


def test_sanity_failure_is_distinct_from_missing_atr(monkeypatch):
    monkeypatch.setenv("US_ATR_SANITY_MAX_PCT", "0.01")
    value, status = _compute_atr_pct(bars(), "QQQ", return_status=True)
    assert value is None and status == "ERROR_ATR_SANITY_FAILED"


def test_large_cap_fixture_never_emits_ninety_percent_atr():
    for symbol in ("AAPL", "QQQ", "SPY"):
        value = _compute_atr_pct(bars(), symbol)
        assert value is not None and value < 0.90
