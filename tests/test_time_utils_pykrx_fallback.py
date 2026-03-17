from __future__ import annotations

from datetime import date

from trader import time_utils


def test_resolve_prev_trading_day_falls_back_to_last_weekday(monkeypatch) -> None:
    monkeypatch.setattr(time_utils, "_resolve_pykrx_previous_or_same", lambda _d: (_ for _ in ()).throw(ValueError("bad json")))

    resolved = time_utils.resolve_prev_trading_day(date(2026, 3, 16))

    assert resolved == date(2026, 3, 13)


def test_is_trading_date_uses_weekday_heuristic_when_pykrx_fails(monkeypatch) -> None:
    monkeypatch.setattr(time_utils, "_resolve_pykrx_previous_or_same", lambda _d: (_ for _ in ()).throw(ValueError("bad json")))

    assert time_utils.is_trading_date(date(2026, 3, 16)) is True
    assert time_utils.is_trading_date(date(2026, 3, 15)) is False


def test_resolve_pykrx_previous_or_same_is_cached(monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_resolver(_d):
        calls["count"] += 1
        return date(2026, 3, 14)

    time_utils._PYKRX_PREV_OR_SAME_CACHE.clear()
    monkeypatch.setattr(time_utils, "_suppress_noisy_external_loggers", lambda: time_utils.contextmanager(lambda: (yield))())
    monkeypatch.setattr(time_utils, "_log_pykrx_fail_once", lambda **_kwargs: None)
    monkeypatch.setattr("pykrx.stock.get_nearest_business_day_in_a_week", lambda *_args, **_kwargs: "20260314", raising=False)

    assert time_utils._resolve_pykrx_previous_or_same(date(2026, 3, 14)) == date(2026, 3, 14)
    assert time_utils._resolve_pykrx_previous_or_same(date(2026, 3, 14)) == date(2026, 3, 14)

    # External helper should be used only once because the second call is cached.
    assert time_utils._PYKRX_PREV_OR_SAME_CACHE["2026-03-14"] == date(2026, 3, 14)