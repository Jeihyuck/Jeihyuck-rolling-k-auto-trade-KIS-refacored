from __future__ import annotations

import types
from datetime import date

import pandas as pd

from trader.prep_runner import _make_flow_provider


class _Http500Kis:
    def __init__(self, *args, **kwargs):
        self.calls = 0

    def inquire_investor(self, code: str, market: str = "KOSDAQ"):
        self.calls += 1
        return {
            "ok": False,
            "error": "HTTP 500 for /uapi/domestic-stock/v1/quotations/inquire-investor",
        }


class _EmptyPykrxStock:
    @staticmethod
    def get_market_net_purchases_of_equities_by_ticker(*args, **kwargs):
        return pd.DataFrame()


def test_investor_flow_http500_isolated_and_prep_provider_fails_soft(monkeypatch):
    """A KIS 5xx must become provider-missing evidence, never a prep exception."""
    fake_kis_mod = types.SimpleNamespace(KisAPI=_Http500Kis)
    monkeypatch.setitem(__import__("sys").modules, "trader.kis_wrapper", fake_kis_mod)
    monkeypatch.setitem(
        __import__("sys").modules,
        "pykrx",
        types.SimpleNamespace(stock=_EmptyPykrxStock),
    )

    provider = _make_flow_provider(engine=None)
    foreign, inst, meta = provider("005930", date(2026, 9, 7), 20)

    assert isinstance(foreign, pd.DataFrame)
    assert isinstance(inst, pd.DataFrame)
    assert foreign.empty
    assert inst.empty
    assert meta["ok"] is False
    assert meta["provider"] == "none"
    assert "kis:http_5xx" in str(meta.get("reason") or "")
    assert "all_providers_failed" in str(meta.get("detail") or "")


def test_external_flow_failure_does_not_claim_genuine_zero_flow(monkeypatch):
    """Missing provider data must remain missing/imputed, not be forged as zero flow."""
    fake_kis_mod = types.SimpleNamespace(KisAPI=_Http500Kis)
    monkeypatch.setitem(__import__("sys").modules, "trader.kis_wrapper", fake_kis_mod)
    monkeypatch.setitem(
        __import__("sys").modules,
        "pykrx",
        types.SimpleNamespace(stock=_EmptyPykrxStock),
    )

    provider = _make_flow_provider(engine=None)
    _, _, meta = provider("000660", date(2026, 9, 7), 20)
    assert meta["ok"] is False
    assert meta["provider"] == "none"
