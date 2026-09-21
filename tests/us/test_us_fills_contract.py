# -*- coding: utf-8 -*-
"""US KIS practice fills contract tests."""

import pytest


def test_get_us_fills_today_uses_official_practice_scope(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient

    captured = []

    class FakeResp:
        headers = {}
        def raise_for_status(self):
            return None
        def json(self):
            return {"rt_cd": "0", "output": [{"fill": "data"}]}

    def fake_get(url, headers=None, params=None, timeout=None, **kwargs):
        captured.append(dict(params or {}))
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)
    client = KisUSClient(env="practice")
    monkeypatch.setattr(client, "get_access_token", lambda: "TOKEN")
    result = client.get_us_fills_today("2026-05-05")

    assert result == [{"fill": "data"}]
    assert len(captured) == 1
    params = captured[0]
    assert params["PDNO"] == ""
    assert params["OVRS_EXCG_CD"] == ""
    assert params["ORD_DT"] == ""
    assert params["ORD_STRT_DT"] == "20260505"
    assert params["ORD_END_DT"] == "20260505"
    assert params["CCLD_NCCS_DVSN"] == "00"
    assert params["ORD_GNO_BRNO"] == ""
    assert params["ODNO"] == ""


def test_get_us_fills_today_contract_error_is_not_retried_with_invented_schema(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient, KisUSClientError

    calls = []

    class FakeResp:
        headers = {}
        def raise_for_status(self):
            return None
        def json(self):
            return {"rt_cd": "1", "msg1": "INPUT_FIELD_NAME", "output": []}

    def fake_get(url, headers=None, params=None, timeout=None, **kwargs):
        calls.append(dict(params or {}))
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)
    client = KisUSClient(env="practice")
    monkeypatch.setattr(client, "get_access_token", lambda: "TOKEN")
    with pytest.raises(KisUSClientError, match="KIS fills contract error"):
        client.get_us_fills_today("2026-05-05")
    assert len(calls) == 1


def test_get_us_fills_today_paginates_with_context(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient

    client = KisUSClient(env="practice", offline=False)
    monkeypatch.setattr(client, "_assert_not_offline", lambda *_: None)
    monkeypatch.setattr(client, "_build_headers", lambda *_: {"tr_id": "VTTS3035R"})
    calls = []

    def fake_get(path, headers, params, suppress_final_log=False):
        calls.append((dict(headers), dict(params)))
        if len(calls) == 1:
            return {
                "output": [{"odno": "1"}],
                "ctx_area_nk200": "NK",
                "ctx_area_fk200": "FK",
                "_response_meta": {"tr_cont": "M"},
            }
        return {"output": [{"odno": "2"}], "_response_meta": {"tr_cont": ""}}

    monkeypatch.setattr(client, "_get", fake_get)
    result = client.get_us_fills_today("2026-05-05")
    assert [row["odno"] for row in result] == ["1", "2"]
    assert calls[1][0]["tr_cont"] == "N"
    assert calls[1][1]["CTX_AREA_NK200"] == "NK"
    assert calls[1][1]["CTX_AREA_FK200"] == "FK"


def test_save_fills_sets_fill_idempotency_key_in_memory(monkeypatch):
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_FILLS.clear()
    fills = [{
        "symbol": "AMZN",
        "exchange": "NASDAQ",
        "side": "BUY",
        "qty": 7,
        "price_usd": 261.7450,
        "order_no": "34770",
        "client_order_key": "",
    }]
    inserted = repos.save_fills(fills, trade_date="2026-05-20")
    assert inserted == 1
    assert repos._MEM_FILLS[0]["fill_idempotency_key"] == "2026-05-20|AMZN|BUY|34770||7|261.745"
