"""Regression for 2026-10-13 KR PREP dated universe failure."""
from __future__ import annotations

from datetime import date
import pandas as pd
import pytest

from trader.universe.providers import krx_marketcap_asof as module


def _df(n=180):
    codes = [f"{100000 + i:06d}" for i in range(n)]
    return pd.DataFrame(
        {"시가총액": [1000000000000 - i * 1000 for i in range(n)],
         "종목명": [f"sample-{i}" for i in range(n)]},
        index=codes,
    )


def test_dated_marketcap_uses_exact_session_and_150_per_market(monkeypatch):
    calls = []
    def fetch(market, requested, max_rollback_days=None):
        calls.append((market, requested, max_rollback_days))
        return _df(), requested, "ok"
    monkeypatch.setattr(module, "fetch_with_rollback", fetch)
    provider = module.KRXAsOfMarketcapProvider()
    for market in ("KOSPI", "KOSDAQ"):
        result = provider.get_marketcap_top_with_meta(market, "2026-10-12", 150)
        assert len(result) == 150
        assert len({x["code"] for x in result}) == 150
        assert all(x["market"] == market for x in result)
        assert all(x["actual_as_of"] == "2026-10-12" for x in result)
        assert all(x["market_cap"] > 0 for x in result)
    assert calls == [
        ("KOSPI", date(2026, 10, 12), 0),
        ("KOSDAQ", date(2026, 10, 12), 0),
    ]


@pytest.mark.parametrize("used_date,reason", [
    (date(2026, 10, 8), "rolled_back"),
    (date(2026, 10, 12), "rolled_back"),
])
def test_never_relabel_an_old_marketcap_session(monkeypatch, used_date, reason):
    monkeypatch.setattr(module, "fetch_with_rollback", lambda *a, **kw: (_df(), used_date, reason))
    with pytest.raises(RuntimeError, match="date_mismatch"):
        module.KRXAsOfMarketcapProvider().get_marketcap_top_with_meta("KOSPI", "2026-10-12", 150)


def test_provider_reported_date_mismatch_blocks(monkeypatch):
    frame = _df()
    frame.attrs["actual_as_of"] = "2026-10-08"
    monkeypatch.setattr(module, "fetch_with_rollback", lambda *a, **kw: (frame, date(2026,10,12), "ok"))
    with pytest.raises(RuntimeError, match="provider_date_mismatch"):
        module.KRXAsOfMarketcapProvider().get_marketcap_top_with_meta("KOSPI", "2026-10-12", 150)


def test_30_rows_cannot_be_promoted_to_150(monkeypatch):
    monkeypatch.setattr(module, "fetch_with_rollback", lambda *a, **kw: (_df(30), date(2026,10,12), "ok"))
    with pytest.raises(RuntimeError, match="insufficient"):
        module.KRXAsOfMarketcapProvider().get_marketcap_top_with_meta("KOSPI", "2026-10-12", 150)


def test_marketcap_column_and_market_validation(monkeypatch):
    monkeypatch.setattr(module, "fetch_with_rollback", lambda *a, **kw: (pd.DataFrame({"x":[1]}, index=["005930"]), date(2026,10,12), "ok"))
    with pytest.raises(RuntimeError, match="missing_market_cap"):
        module.KRXAsOfMarketcapProvider().get_marketcap_top_with_meta("KOSPI", "2026-10-12", 150)
    with pytest.raises(ValueError, match="invalid_market"):
        module.KRXAsOfMarketcapProvider().get_marketcap_top_with_meta("UNKNOWN", "2026-10-12", 150)
