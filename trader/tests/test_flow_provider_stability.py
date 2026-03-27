from __future__ import annotations

import json
import types
from datetime import date

import pandas as pd

from trader.prep_runner import _make_flow_provider
from trader.watchlist_builder import _build_flow_provenance


class _DummyKis:
    def __init__(self, *args, **kwargs):
        self.calls = 0
        self.refreshed = 0

    def inquire_investor(self, code: str, market: str = "KOSDAQ"):
        self.calls += 1
        return {"ok": False, "error": "FAST_FAIL breaker open for /uapi/domestic-stock/v1/quotations/inquire-investor"}

    def refresh_token(self):
        self.refreshed += 1


class _FailingStock:
    @staticmethod
    def get_market_net_purchases_of_equities_by_ticker(*args, **kwargs):
        raise json.JSONDecodeError("bad", "{}", 0)


def test_pykrx_jsondecodeerror_isolated(monkeypatch):
    fake_pykrx = types.SimpleNamespace(stock=_FailingStock)
    monkeypatch.setitem(__import__("sys").modules, "pykrx", fake_pykrx)
    fake_kis_mod = types.SimpleNamespace(KisAPI=_DummyKis)
    monkeypatch.setitem(__import__("sys").modules, "trader.kis_wrapper", fake_kis_mod)

    provider = _make_flow_provider(engine=None)
    foreign, inst, meta = provider("005930", date(2026, 3, 27), 20)

    assert isinstance(foreign, pd.DataFrame)
    assert isinstance(inst, pd.DataFrame)
    assert meta["ok"] is False
    assert meta["provider"] == "none"


def test_kis_breaker_open_run_level_disable(monkeypatch):
    fake_kis_mod = types.SimpleNamespace(KisAPI=_DummyKis)
    monkeypatch.setitem(__import__("sys").modules, "trader.kis_wrapper", fake_kis_mod)

    class _EmptyStock:
        @staticmethod
        def get_market_net_purchases_of_equities_by_ticker(*args, **kwargs):
            return pd.DataFrame()

    monkeypatch.setitem(__import__("sys").modules, "pykrx", types.SimpleNamespace(stock=_EmptyStock))

    provider = _make_flow_provider(engine=None)
    _, _, meta1 = provider("005930", date(2026, 3, 27), 20)
    _, _, meta2 = provider("000660", date(2026, 3, 27), 20)

    assert meta1["ok"] is False
    assert "kis:breaker_open" in meta1["reason"]
    assert "kis:disabled:breaker_open" in meta2["reason"]


def test_flow_provenance_genuine_zero_vs_imputed():
    genuine = _build_flow_provenance(
        flow_missing=False,
        flow_meta={"ok": True, "provider": "kis", "reason": ""},
        foreign_df=pd.DataFrame([{"date": "2026-03-27", "net_buy": 0.0}]),
        inst_df=pd.DataFrame([{"date": "2026-03-27", "net_buy": 0.0}]),
        flow_missing_reason="",
    )
    imputed = _build_flow_provenance(
        flow_missing=True,
        flow_meta={"ok": False, "provider": "none", "reason": "all_providers_failed"},
        foreign_df=pd.DataFrame(),
        inst_df=pd.DataFrame(),
        flow_missing_reason="provider_missing",
    )

    assert genuine["flow_score_imputed"] == 0
    assert genuine["flow_data_available"] == 1
    assert imputed["flow_score_imputed"] == 1
    assert imputed["flow_data_available"] == 0


def test_all_providers_failed_meta_contract(monkeypatch):
    fake_kis_mod = types.SimpleNamespace(KisAPI=_DummyKis)
    monkeypatch.setitem(__import__("sys").modules, "trader.kis_wrapper", fake_kis_mod)

    class _EmptyStock:
        @staticmethod
        def get_market_net_purchases_of_equities_by_ticker(*args, **kwargs):
            return pd.DataFrame()

    monkeypatch.setitem(__import__("sys").modules, "pykrx", types.SimpleNamespace(stock=_EmptyStock))
    provider = _make_flow_provider(engine=None)
    _, _, meta = provider("005930", date(2026, 3, 27), 20)

    assert meta["provider"] == "none"
    assert meta["ok"] is False
    assert "all_providers_failed" in meta["detail"]
