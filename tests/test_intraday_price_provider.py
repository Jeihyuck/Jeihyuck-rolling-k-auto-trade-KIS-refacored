from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from trader.pb1_engine import PB1Engine


def _fake_engine(**overrides):
    now = datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul"))
    fake = SimpleNamespace(
        phase="pm_entry",
        price_allowed=True,
        _balance_price_map={},
        _current_price_cache={},
        _current_price_fetch_count=0,
        _now_kst=now,
        _to_float=PB1Engine._to_float,
        _parse_price_ts=PB1Engine._parse_price_ts,
        _fresh_current_price_from_features=PB1Engine._fresh_current_price_from_features,
        _mark_price=lambda code: 101.0,
    )
    fake.__dict__.update(overrides)
    return fake


def test_row_current_price_used_without_quote(monkeypatch):
    called = {"n": 0}
    fake = _fake_engine(_mark_price=lambda code: called.__setitem__("n", called["n"] + 1) or 101.0)
    price = PB1Engine._get_current_price_for_entry(fake, "005930", {"current_price": 100.0})
    assert price == 100.0
    assert called["n"] == 0


def test_cache_used_when_row_missing(monkeypatch):
    fake = _fake_engine(_current_price_cache={"005930": (99.0, datetime(2026, 6, 9, 13, 40, 45, tzinfo=ZoneInfo("Asia/Seoul")))})
    assert PB1Engine._get_current_price_for_entry(fake, "005930", {}) == 99.0


def test_quote_called_once_when_row_and_cache_missing(monkeypatch):
    calls = {"n": 0}
    def quote(_code):
        calls["n"] += 1
        return 101.0
    fake = _fake_engine(_mark_price=quote)
    assert PB1Engine._get_current_price_for_entry(fake, "005930", {}) == 101.0
    assert calls["n"] == 1


def test_stale_current_price_not_used_for_reclaim(caplog):
    now = datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul"))
    fake = _fake_engine(_now_kst=now, price_allowed=False)
    stale_ts = (now - timedelta(seconds=300)).isoformat()
    price = PB1Engine._get_current_price_for_entry(fake, "005930", {"current_price": 101.0, "current_price_ts": stale_ts})
    assert price is None


def test_missing_current_price_logs_reclaim_skip(caplog):
    fake = _fake_engine(price_allowed=False)
    with caplog.at_level("INFO"):
        price = PB1Engine._resolve_intraday_current_price_for_reclaim(fake, "005930", current_price=None, last_close=99.0, ma20_value=100.0, features={})
    assert price is None
    assert "[ENTRY][INTRADAY_RECLAIM][SKIP] code=005930 reason=current_price_missing" in "\n".join(r.getMessage() for r in caplog.records)
