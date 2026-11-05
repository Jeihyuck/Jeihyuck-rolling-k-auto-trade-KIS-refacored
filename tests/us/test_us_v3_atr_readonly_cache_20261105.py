"""v3 ATR cache must be KIS raw, completed, recent, untampered, and order-free."""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json

import pytest

from trader.us.market_calendar import is_us_trading_day
from trader.us.pb1.v3_atr_readonly_cache import (
    normalize_kis_completed_bars, write_verified_kis_daily_cache,
    load_verified_kis_daily_cache,
)
from scripts.us_prewarm_v3_atr_readonly import _positions_from_complete_practice_balance


TRADE_DATE = "2026-11-05"
NOW = datetime(2026, 11, 5, 14, tzinfo=timezone.utc)


def raw_bars(n=60):
    cursor = date(2026, 11, 5) - timedelta(days=1)
    days = []
    while len(days) < n:
        if is_us_trading_day(cursor):
            days.append(cursor)
        cursor -= timedelta(days=1)
    return [{"date": d.strftime("%Y%m%d"), "high": 101, "low": 99,
             "close": 100} for d in reversed(days)]


def test_auth_practice_cache_roundtrip_only_complete_history(tmp_path, monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    result = write_verified_kis_daily_cache(
        raw_rows=raw_bars(), symbol="SPY", exchange="NYSE",
        decision_date=TRADE_DATE, root=tmp_path, now=NOW,
    )
    assert result["status"] == "KIS_DAILY_COMPLETED_CACHED"
    loaded = load_verified_kis_daily_cache(
        symbol="SPY", decision_date=TRADE_DATE, root=tmp_path, now=NOW,
    )
    assert len(loaded) == 60 and loaded[-1]["trade_date"] == "2026-11-04"
    assert all(d["source"] == "KIS_DAILY_VERIFIED" and d["complete"] for d in loaded)


def test_no_practice_no_cache_write(tmp_path, monkeypatch):
    monkeypatch.setenv("KIS_ENV", "real")
    with pytest.raises(RuntimeError, match="PRACTICE_ONLY"):
        write_verified_kis_daily_cache(
            raw_rows=raw_bars(), symbol="SPY", exchange="NYSE",
            decision_date=TRADE_DATE, root=tmp_path, now=NOW,
        )
    assert not list(Path(tmp_path).rglob("*.json"))


def test_partial_current_bar_removed_without_future_peeking():
    raw = raw_bars()
    raw.append({"date": "20261105", "high": 1000, "low": 1, "close": 800})
    bars, _ = normalize_kis_completed_bars(
        raw, symbol="SPY", decision_date=TRADE_DATE,
    )
    assert len(bars) == 60
    assert all(x["high"] != 1000 for x in bars)


def test_missing_completed_day_and_anomaly_symbols_are_quarantined():
    with pytest.raises(ValueError, match="LAST_COMPLETED"):
        normalize_kis_completed_bars(
            raw_bars()[:-1], symbol="SPY", decision_date=TRADE_DATE,
        )
    with pytest.raises(ValueError, match="QUARANTINE"):
        normalize_kis_completed_bars(
            raw_bars(), symbol="CL", decision_date=TRADE_DATE,
        )


def test_cache_tampering_and_staleness_never_authorize_atr(tmp_path, monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    write_verified_kis_daily_cache(
        raw_rows=raw_bars(), symbol="SPY", exchange="NYSE",
        decision_date=TRADE_DATE, root=tmp_path, now=NOW,
    )
    future = NOW + timedelta(days=3)
    assert not load_verified_kis_daily_cache(
        symbol="SPY", decision_date=TRADE_DATE, root=tmp_path, now=future,
    )
    path = next(Path(tmp_path).rglob("SPY.json"))
    payload = json.loads(path.read_text())
    payload["bars"][-1]["close"] = 9999
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert not load_verified_kis_daily_cache(
        symbol="SPY", decision_date=TRADE_DATE, root=tmp_path, now=NOW,
    )


def test_readonly_holdings_list_fails_closed_without_complete_balance():
    with pytest.raises(RuntimeError, match="BALANCE_INCOMPLETE"):
        _positions_from_complete_practice_balance({
            "balance_complete": False, "output1": [{"ovrs_pdno": "SPY"}],
        })
    rows = [
        {"ovrs_pdno": "SPY", "ovrs_excg_cd": "AMEX", "ovrs_cblc_qty": "2"},
        {"ovrs_pdno": "TQQQ", "ovrs_excg_cd": "NASD", "ovrs_cblc_qty": "9"},
        {"ovrs_pdno": "QQQ", "ovrs_excg_cd": "NASD", "ovrs_cblc_qty": "0"},
    ]
    assert _positions_from_complete_practice_balance({
        "balance_complete": True, "balance_authoritative": True, "output1": rows,
    }) == [("SPY", "AMEX")]


def test_symbol_path_traversal_not_permitted(tmp_path, monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    with pytest.raises(ValueError, match="SYMBOL_UNVERIFIED"):
        write_verified_kis_daily_cache(
            raw_rows=raw_bars(), symbol="../TMO", exchange="NYSE",
            decision_date=TRADE_DATE, root=tmp_path, now=NOW,
        )
