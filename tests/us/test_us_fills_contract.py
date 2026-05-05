# -*- coding: utf-8 -*-
"""US KIS fills contract tests."""


def test_get_us_fills_today_uses_ord_start_end_dates(monkeypatch):
    """get_us_fills_today should use ORD_STRT_DT and ORD_END_DT, not ORD_DT."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    captured = {}

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"rt_cd": "0", "output": []}

    def fake_get(url, headers=None, params=None, timeout=None):
        captured["params"] = params
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)

    client = KisUSClient(env="practice")
    monkeypatch.setattr(client, "get_access_token", lambda: "TOKEN")
    client.get_us_fills_today("2026-05-05")

    params = captured["params"]
    assert params["ORD_STRT_DT"] == "20260505"
    assert params["ORD_END_DT"] == "20260505"
    assert "ORD_DT" not in params
