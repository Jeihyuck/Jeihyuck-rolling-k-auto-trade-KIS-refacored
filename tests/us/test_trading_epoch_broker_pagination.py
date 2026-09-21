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
