# -*- coding: utf-8 -*-
"""US KIS fills contract tests - schema fallback."""


def test_get_us_fills_today_ord_dt_success(monkeypatch):
    """get_us_fills_today should try ORD_DT first and succeed."""
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

    # Should only call once with ORD_DT
    assert len(captured) == 1
    params = captured[0]
    assert params["ORD_DT"] == "20260505"
    assert "ORD_STRT_DT" not in params
    assert "ORD_END_DT" not in params
    assert result == [{"fill": "data"}]


def test_get_us_fills_today_ord_dt_fails_fallback_to_ord_range(monkeypatch):
    """get_us_fills_today should fallback to ORD_RANGE if ORD_DT fails with INPUT_FIELD_NAME."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    captured = []
    call_count = [0]

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call with ORD_DT fails
                return {"rt_cd": "1", "msg1": "INPUT_FIELD_NAME", "output": []}
            else:
                # Second call with ORD_RANGE succeeds
                return {"rt_cd": "0", "output": [{"fill": "data"}]}

    def fake_get(url, headers=None, params=None, timeout=None, **kwargs):
        captured.append(params.copy())
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)

    client = KisUSClient(env="practice")
    monkeypatch.setattr(client, "get_access_token", lambda: "TOKEN")
    result = client.get_us_fills_today("2026-05-05")

    # Should call twice: ORD_DT then ORD_RANGE
    assert len(captured) == 2
    
    # First call used ORD_DT
    assert captured[0]["ORD_DT"] == "20260505"
    assert "ORD_STRT_DT" not in captured[0]
    
    # Second call used ORD_RANGE
    assert captured[1]["ORD_STRT_DT"] == "20260505"
    assert captured[1]["ORD_END_DT"] == "20260505"
    assert "ORD_DT" not in captured[1]
    
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

    # Should try both schemas
    assert len(captured) == 2

