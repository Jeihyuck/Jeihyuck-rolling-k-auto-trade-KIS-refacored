from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from trader.minervini.compute import compute_minervini_features_for_asof


def _make_candles(symbol: str, days: int, *, mode: str) -> list[dict]:
    start = date(2025, 1, 1)
    rows: list[dict] = []

    for i in range(days):
        current = start + timedelta(days=i)
        base = 100.0 + i * 0.12

        if mode == "strong":
            if i < 50:
                band, volume = 4.0, 1200.0
            elif i < 100:
                band, volume = 3.0, 1100.0
            elif i < 150:
                band, volume = 2.0, 900.0
            else:
                band, volume = 1.0, 500.0
        elif mode == "partial":
            if i < 50:
                band, volume = 4.0, 900.0
            elif i < 100:
                band, volume = 3.0, 920.0
            elif i < 150:
                band, volume = 2.0, 940.0
            else:
                band, volume = 1.2, 960.0
        else:
            band, volume = 3.0, 1000.0

        close = base
        if mode == "strong" and i >= days - 5:
            close = 120.0 + (0.03 if i % 2 else -0.02)

        rows.append(
            {
                "date": current.strftime("%Y%m%d"),
                "open": close,
                "high": close + band,
                "low": close - band,
                "close": close,
                "volume": volume,
                "value": close * volume,
            }
        )

    return rows


def test_derived_minervini_vcp_not_all_zero_and_has_vcp_object(monkeypatch):
    candles_by_symbol = {
        "229200": _make_candles("229200", 220, mode="partial"),
        "111111": _make_candles("111111", 220, mode="strong"),
        "222222": _make_candles("222222", 220, mode="partial"),
        "333333": _make_candles("333333", 220, mode="weak"),
    }

    def fake_load_price_daily(_engine, symbol: str, _start, _end):
        return candles_by_symbol.get(str(symbol).zfill(6), [])

    def fake_rank_rs(price_series, _bench_close, **_kwargs):
        rows = []
        for ticker in price_series.keys():
            rows.append({"ticker": ticker, "pctile": 0.9})
        return pd.DataFrame(rows)

    monkeypatch.setattr("trader.minervini.compute.load_price_daily", fake_load_price_daily)
    monkeypatch.setattr("trader.minervini.compute.rank_rs", fake_rank_rs)

    rows = compute_minervini_features_for_asof(
        engine=object(),
        symbols=["111111", "222222", "333333"],
        env="practice",
        as_of=date(2026, 2, 19),
        lookback_days=220,
    )

    assert len(rows) == 3

    scores = [row.get("vcp_score") for row in rows]
    assert any((score is not None) and (score > 0) for score in scores)

    for row in rows:
        vcp_obj = (row.get("features_json") or {}).get("vcp")
        assert isinstance(vcp_obj, dict)
        assert "score" in vcp_obj
        assert "ok" in vcp_obj
        assert "reason" in vcp_obj
