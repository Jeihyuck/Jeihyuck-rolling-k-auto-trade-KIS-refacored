from __future__ import annotations

from unittest.mock import Mock

import pytest

from trader.us.execution import kis_us_client as mod


def _client():
    client = object.__new__(mod.KisUSClient)
    client._offline = False
    client._cano = "test"
    client._acnt_prdt_cd = "01"
    client._build_headers = lambda tr_id: {"tr_id": tr_id}
    return client


def test_balance_pagination_rejects_repeated_cursor(monkeypatch):
    monkeypatch.setattr(mod, "get_tr_info", lambda name: {"tr_id": "T", "path": "/balance"})
    client = _client()
    client._get = Mock(side_effect=[
        {
            "output1": [],
            "output2": {},
            "ctx_area_fk200": "FK2",
            "ctx_area_nk200": "NK2",
            "_response_meta": {"tr_cont": "M"},
        },
        {
            "output1": [],
            "output2": {},
            "ctx_area_fk200": "FK2",
            "ctx_area_nk200": "NK2",
            "_response_meta": {"tr_cont": "M"},
        },
    ])
    with pytest.raises(mod.KisUSTemporaryError, match="pagination stalled"):
        client._get_us_balance_single_exchange("NASD", max_pages=5)


def test_balance_pagination_cannot_silently_hit_page_limit(monkeypatch):
    monkeypatch.setattr(mod, "get_tr_info", lambda name: {"tr_id": "T", "path": "/balance"})
    client = _client()
    client._get = Mock(side_effect=[
        {
            "output1": [],
            "output2": {},
            "ctx_area_fk200": "FK2",
            "ctx_area_nk200": "NK2",
            "_response_meta": {"tr_cont": "M"},
        },
        {
            "output1": [],
            "output2": {},
            "ctx_area_fk200": "FK3",
            "ctx_area_nk200": "NK3",
            "_response_meta": {"tr_cont": "M"},
        },
    ])
    with pytest.raises(mod.KisUSTemporaryError, match="pagination incomplete"):
        client._get_us_balance_single_exchange("NASD", max_pages=2)


def test_today_orders_follow_cursor_even_without_header(monkeypatch):
    monkeypatch.setattr(mod, "get_tr_info", lambda name: {"tr_id": "T", "path": "/fills"})
    client = _client()
    client._get = Mock(side_effect=[
        {
            "output": [{"odno": "1"}],
            "ctx_area_fk200": "FK2",
            "ctx_area_nk200": "NK2",
        },
        {
            "output": [{"odno": "2"}],
            "ctx_area_fk200": "FK2",
            "ctx_area_nk200": "NK2",
            "_response_meta": {"tr_cont": "D"},
        },
    ])

    rows = client.get_us_fills_today("2026-09-21")
    assert [row["odno"] for row in rows] == ["1", "2"]
    assert client._get.call_count == 2
    second = client._get.call_args_list[1]
    assert second.kwargs["headers"]["tr_cont"] == "N"
    assert second.kwargs["params"]["CTX_AREA_FK200"] == "FK2"
    assert second.kwargs["params"]["CTX_AREA_NK200"] == "NK2"


def test_today_orders_reject_unknown_continuation_status(monkeypatch):
    monkeypatch.setattr(mod, "get_tr_info", lambda name: {"tr_id": "T", "path": "/fills"})
    client = _client()
    client._get = Mock(return_value={
        "output": [],
        "_response_meta": {"tr_cont": "?"},
    })
    with pytest.raises(mod.KisUSClientError, match="unknown tr_cont"):
        client.get_us_fills_today("2026-09-21")



def test_real_fills_use_real_tr_and_us_wide_params(monkeypatch):
    monkeypatch.setattr(mod, "get_tr_info", lambda name: {"tr_id": "REGISTRY", "path": "/fills"})
    client = _client()
    client.env = "real"
    headers = Mock(return_value={})
    client._build_headers = headers
    client._get = Mock(return_value={
        "output": [],
        "_response_meta": {"tr_cont": "D"},
    })

    assert client.get_us_fills_today("2026-09-21") == []
    headers.assert_called_once_with("TTTS3035R")
    params = client._get.call_args.kwargs["params"]
    assert params["PDNO"] == "%"
    assert params["OVRS_EXCG_CD"] == "NASD"


def test_real_balance_uses_real_tr_id(monkeypatch):
    monkeypatch.setattr(mod, "get_tr_info", lambda name: {"tr_id": "REGISTRY", "path": "/balance"})
    client = _client()
    client.env = "real"
    headers = Mock(return_value={})
    client._build_headers = headers
    client._get = Mock(return_value={
        "output1": [],
        "output2": {},
        "_response_meta": {"tr_cont": "D"},
    })

    result = client._get_us_balance_single_exchange("NASD", max_pages=2)
    assert result["output1"] == []
    headers.assert_called_once_with("TTTS3012R")


def test_real_balance_defaults_to_nasd_us_wide(monkeypatch):
    monkeypatch.delenv("US_BALANCE_EXCHANGES", raising=False)
    client = _client()
    client.env = "real"
    client._tick_context = None
    client._response_cache = {}
    client._stage_deadline = None
    client._stage_max_attempts = None
    client._balance_conflicts = []
    client._get_us_balance_single_exchange = Mock(return_value={
        "rt_cd": "0", "output1": [], "output2": {}
    })
    client._merge_duplicate_symbols = lambda rows: rows

    result = client.get_us_balance(force_refresh=True)
    assert result["queried_exchanges"] == ["NASD"]
    client._get_us_balance_single_exchange.assert_called_once_with("NASD")
