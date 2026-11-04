"""Regression for 2026-11-04 KR volume-proof cache staleness.

Only the cache source is repaired; intraday KIS volume proof still decides BUY.
"""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from trader.data import ohlcv_provider as mod
from trader.data.ohlcv_provider import (
    ChainOHLCVProvider, OHLCVResult, _completed_daily_cache_recent,
)


SESSION = datetime(2026, 11, 4, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))


def _bars(last: str) -> pd.DataFrame:
    dates = pd.bdate_range(end=last, periods=60)
    return pd.DataFrame({
        "date": dates,
        "open": [100.0] * 60,
        "high": [103.0] * 60,
        "low": [98.0] * 60,
        "close": [101.0] * 60,
        "volume": [1000] * 60,
    })


class FakeDBProvider:
    name = "db"

    def __init__(self):
        self.calls = 0

    def get_ohlcv(self, symbol, days, **kwargs):
        self.calls += 1
        return OHLCVResult(_bars("2026-11-03").tail(days), {
            "provider": "db", "source": "db", "volume_missing": False,
        })


def _chain(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "now_kst", lambda: SESSION)
    db = FakeDBProvider()
    chain = ChainOHLCVProvider([db], env="practice", cache_dir=tmp_path)
    monkeypatch.setattr(chain, "_persist_cache", lambda *_args: None)
    return chain, db


def test_completed_cache_date_excludes_incomplete_current_bar():
    with_today = pd.concat([
        _bars("2026-10-02"),
        pd.DataFrame([{"date": pd.Timestamp("2026-11-04"), "volume": 400}]),
    ], ignore_index=True)
    assert not _completed_daily_cache_recent(
        OHLCVResult(with_today, {}), session=date(2026, 11, 4),
    )
    assert _completed_daily_cache_recent(
        OHLCVResult(_bars("2026-11-03"), {}), session=date(2026, 11, 4),
    )


def test_stale_parquet_cache_falls_through_to_current_db(tmp_path, monkeypatch):
    chain, db = _chain(tmp_path, monkeypatch)
    monkeypatch.setattr(
        chain, "_load_cache",
        lambda *_args: OHLCVResult(
            _bars("2026-10-02"), {"source": "cache", "rows": 60, "volume_missing": False},
        ),
    )
    actual = chain.get_ohlcv("012750", 60, usage_context="trade")
    assert db.calls == 1
    assert actual.meta["source"] == "db"
    assert actual.df["date"].max().date() == date(2026, 11, 3)


def test_stale_memory_cache_does_not_bypass_trade_validation(tmp_path, monkeypatch):
    chain, db = _chain(tmp_path, monkeypatch)
    chain._memory_cache[("012750", 60)] = OHLCVResult(
        _bars("2026-10-02"), {"source": "cache", "rows": 60, "volume_missing": False},
    )
    monkeypatch.setattr(chain, "_load_cache", lambda *_args: None)
    actual = chain.get_ohlcv("012750", 60, usage_context="trade")
    assert db.calls == 1
    assert actual.meta["source"] == "db"
    assert actual.df["date"].max().date() == date(2026, 11, 3)


def test_preparation_cache_policy_unchanged(tmp_path, monkeypatch):
    chain, db = _chain(tmp_path, monkeypatch)
    old = OHLCVResult(
        _bars("2026-10-02"), {"source": "cache", "rows": 60, "volume_missing": False},
    )
    chain._memory_cache[("012750", 60)] = old
    actual = chain.get_ohlcv("012750", 60, usage_context=None)
    assert db.calls == 0
    assert actual is old


def test_stale_cache_cannot_become_order_approval(tmp_path, monkeypatch):
    from trader.kr.volume_proof import build_kr_volume_proof

    chain, _db = _chain(tmp_path, monkeypatch)
    monkeypatch.setattr(chain, "_load_cache", lambda *_args: OHLCVResult(
        _bars("2026-10-02"), {"source": "cache", "rows": 60, "volume_missing": False},
    ))
    result = chain.get_ohlcv("012750", 60, usage_context="trade")
    # A fresh completed baseline alone never approves BUY without KIS ticks.
    proof = build_kr_volume_proof(
        bars=result.df, snapshot={}, trade_date=date(2026, 11, 4),
        observed_at=SESSION, code="012750", market="KOSPI",
        entry_style="ENTRY_MOMENTUM",
        daily_source=result.meta["source"],
    )
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["failure_reason"] == "kis_cumulative_volume_missing"


def test_trade_ttl_cache_refreshes_from_db_without_kis_call(monkeypatch):
    monkeypatch.setattr(mod, "now_kst", lambda: SESSION)
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("TRADE_SKIP_KIS_DAILY_REFRESH", "1")
    used = {"db": 0, "kis": 0, "set": 0}

    class TTL:
        def get(self, _key):
            return OHLCVResult(
                _bars("2026-10-02"), {"source": "cache", "rows": 60, "volume_missing": False},
            )

        def set(self, *_args):
            used["set"] += 1

    class KIS:
        def get_ohlcv(self, *_args, **_kwargs):
            used["kis"] += 1
            raise AssertionError("Trade cache repair must use fresh DB; no KIS fetch")

    def rows(_engine, _symbol, _start, _end):
        used["db"] += 1
        return _bars("2026-11-03").to_dict("records")

    monkeypatch.setattr(mod, "daily_cache", TTL())
    monkeypatch.setattr(mod, "make_engine", lambda: object())
    monkeypatch.setattr(mod, "load_price_daily", rows)
    actual = mod.KISOHLCVProvider(KIS()).get_ohlcv(
        "012750", 60, usage_context="trade",
    )
    assert used == {"db": 1, "kis": 0, "set": 1}
    assert actual.meta["source"] == "db"
    assert actual.df["date"].max().date() == date(2026, 11, 3)
