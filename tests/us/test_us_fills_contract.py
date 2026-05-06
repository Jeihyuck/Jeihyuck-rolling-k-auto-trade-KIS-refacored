# -*- coding: utf-8 -*-
"""US KIS fills contract tests - schema fallback."""


def test_get_us_fills_today_all_dates_success(monkeypatch):
    """get_us_fills_today should try ALL_DATES first and succeed."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    captured = []

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"rt_cd": "0", "output": [{"fill": "data"}]}

    def fake_get(url, headers=None, params=None, timeout=None, **kwargs):
        captured.append(params.copy())
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)

    client = KisUSClient(env="practice")
    monkeypatch.setattr(client, "get_access_token", lambda: "TOKEN")
    result = client.get_us_fills_today("2026-05-05")

    # Should only call once with ALL_DATES
    assert len(captured) == 1
    params = captured[0]
    assert params["ORD_DT"] == "20260505"
    assert params["ORD_STRT_DT"] == "20260505"
    assert params["ORD_END_DT"] == "20260505"
    assert result == [{"fill": "data"}]


def test_get_us_fills_today_all_dates_fails_fallback_to_ord_dt(monkeypatch):
    """get_us_fills_today should fallback to ORD_DT if ALL_DATES fails with INPUT_FIELD_NAME."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    captured = []
    call_count = [0]

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call with ALL_DATES fails
                return {"rt_cd": "1", "msg1": "INPUT_FIELD_NAME", "output": []}
            else:
                # Second call with ORD_DT succeeds
                return {"rt_cd": "0", "output": [{"fill": "data"}]}

    def fake_get(url, headers=None, params=None, timeout=None, **kwargs):
        captured.append(params.copy())
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)

    client = KisUSClient(env="practice")
    monkeypatch.setattr(client, "get_access_token", lambda: "TOKEN")
    result = client.get_us_fills_today("2026-05-05")

    # Should call twice: ALL_DATES then ORD_DT
    assert len(captured) == 2
    
    # First call used ALL_DATES
    assert captured[0]["ORD_DT"] == "20260505"
    assert captured[0]["ORD_STRT_DT"] == "20260505"
    assert captured[0]["ORD_END_DT"] == "20260505"
    
    # Second call used ORD_DT only
    assert captured[1]["ORD_DT"] == "20260505"
    assert "ORD_STRT_DT" not in captured[1]
    assert "ORD_END_DT" not in captured[1]
    
    assert result == [{"fill": "data"}]


def test_get_us_fills_today_all_schemas_fail(monkeypatch):
    """get_us_fills_today should raise exception if all schemas fail."""
    from trader.us.execution.kis_us_client import KisUSClient, KisUSClientError
    import pytest
    
    captured = []

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"rt_cd": "1", "msg1": "SOME_OTHER_ERROR", "output": []}

    def fake_get(url, headers=None, params=None, timeout=None, **kwargs):
        captured.append(params.copy())
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)

    client = KisUSClient(env="practice")
    monkeypatch.setattr(client, "get_access_token", lambda: "TOKEN")
    
    with pytest.raises(KisUSClientError, match="all schemas failed"):
        client.get_us_fills_today("2026-05-05")

    # Should try all three schemas
    assert len(captured) == 3

