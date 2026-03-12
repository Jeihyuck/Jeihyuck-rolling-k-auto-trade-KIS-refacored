from __future__ import annotations

from datetime import date, timedelta

from trader.data.ohlcv_provider import KISOHLCVProvider


class DummyKis:
    def get_daily_candles(self, *_args, **_kwargs):
        raise RuntimeError("timeout")


def _db_rows(n: int) -> list[dict]:
    today = date(2026, 3, 12)
    rows = []
    for i in range(n):
        d = today - timedelta(days=(n - i))
        rows.append(
            {
                "date": d,
                "open": 1000 + i,
                "high": 1010 + i,
                "low": 990 + i,
                "close": 1005 + i,
                "volume": 100000 + i,
            }
        )
    return rows


def test_kis_refresh_fail_soft_when_db_ready(monkeypatch):
    from trader.data import ohlcv_provider as mod

    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("TRADE_SKIP_KIS_DAILY_REFRESH", "0")
    monkeypatch.setattr(mod, "is_diag_mode", lambda: False)

    monkeypatch.setattr(mod, "make_engine", lambda: object())
    monkeypatch.setattr(mod, "load_price_daily", lambda *_args, **_kwargs: _db_rows(260))

    provider = KISOHLCVProvider(DummyKis())
    result = provider.get_ohlcv("005930", 120)

    assert not result.df.empty
    assert result.meta.get("source") == "db"
    assert result.meta.get("refresh_failed") is True
    assert result.meta.get("stale_ok") is True
