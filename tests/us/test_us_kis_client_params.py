# -*- coding: utf-8 -*-
"""KIS US Client 파라미터 계약 테스트.

- get_us_orderable_cash: ITEM_CD, OVRS_ORD_UNPR 파라미터 전송 여부
- get_us_fills_today: CCLD_NCCS_DVSN, SORT_SQN 파라미터 전송 여부
"""
import inspect
import pytest


def test_get_us_orderable_cash_has_symbol_param():
    """get_us_orderable_cash 시그니처에 symbol, exchange, price가 있어야 한다."""
    from trader.us.execution.kis_us_client import KisUSClient
    sig = inspect.signature(KisUSClient.get_us_orderable_cash)
    params = list(sig.parameters.keys())
    assert "symbol" in params, "symbol parameter missing from get_us_orderable_cash"
    assert "exchange" in params, "exchange parameter missing from get_us_orderable_cash"
    assert "price" in params, "price parameter missing from get_us_orderable_cash"


def test_get_us_orderable_cash_sends_item_cd(monkeypatch):
    """get_us_orderable_cash 호출 시 ITEM_CD=symbol이 params에 포함되어야 한다."""
    from trader.us.execution.kis_us_client import KisUSClient

    captured = {}

    def fake_get(self, path, headers, params):
        captured["params"] = params
        return {"output": {"frcr_ord_psbl_amt1": "5000.00"}}

    monkeypatch.setattr(KisUSClient, "_get", fake_get)
    monkeypatch.setattr(KisUSClient, "_assert_not_offline", lambda self, m: None)
    monkeypatch.setattr(KisUSClient, "_build_headers", lambda self, tr_id: {"tr_id": tr_id})

    client = KisUSClient(env="practice", offline=False)
    client.get_us_orderable_cash(symbol="NVDA", exchange="NASDAQ", price=123.45)

    assert captured["params"]["ITEM_CD"] == "NVDA", (
        "ITEM_CD must be set to the symbol in get_us_orderable_cash"
    )
    assert captured["params"]["OVRS_ORD_UNPR"] == "123.45", (
        "OVRS_ORD_UNPR must be set to formatted price in get_us_orderable_cash"
    )


def test_get_us_orderable_cash_sends_excg_code(monkeypatch):
    """get_us_orderable_cash 호출 시 OVRS_EXCG_CD가 파라미터에 포함되어야 한다."""
    from trader.us.execution.kis_us_client import KisUSClient

    captured = {}

    def fake_get(self, path, headers, params):
        captured["params"] = params
        return {"output": {}}

    monkeypatch.setattr(KisUSClient, "_get", fake_get)
    monkeypatch.setattr(KisUSClient, "_assert_not_offline", lambda self, m: None)
    monkeypatch.setattr(KisUSClient, "_build_headers", lambda self, tr_id: {"tr_id": tr_id})

    client = KisUSClient(env="practice", offline=False)
    client.get_us_orderable_cash(symbol="AAPL", exchange="NASDAQ", price=150.0)

    assert "OVRS_EXCG_CD" in captured["params"], (
        "OVRS_EXCG_CD must be present in get_us_orderable_cash params"
    )


def test_get_us_fills_today_has_ccld_nccs_dvsn(monkeypatch):
    """get_us_fills_today 호출 시 CCLD_NCCS_DVSN이 params에 포함되어야 한다."""
    from trader.us.execution.kis_us_client import KisUSClient

    captured = {}

    def fake_get(self, path, headers, params, suppress_final_log=False):
        captured["params"] = params
        return {"output": []}

    monkeypatch.setattr(KisUSClient, "_get", fake_get)
    monkeypatch.setattr(KisUSClient, "_assert_not_offline", lambda self, m: None)
    monkeypatch.setattr(KisUSClient, "_build_headers", lambda self, tr_id: {"tr_id": tr_id})

    client = KisUSClient(env="practice", offline=False)
    client.get_us_fills_today()

    assert "CCLD_NCCS_DVSN" in captured["params"], (
        "CCLD_NCCS_DVSN must be present in get_us_fills_today params"
    )


def test_get_us_fills_today_has_sort_sqn(monkeypatch):
    """get_us_fills_today 호출 시 SORT_SQN이 params에 포함되어야 한다."""
    from trader.us.execution.kis_us_client import KisUSClient

    captured = {}

    def fake_get(self, path, headers, params, suppress_final_log=False):
        captured["params"] = params
        return {"output": []}

    monkeypatch.setattr(KisUSClient, "_get", fake_get)
    monkeypatch.setattr(KisUSClient, "_assert_not_offline", lambda self, m: None)
    monkeypatch.setattr(KisUSClient, "_build_headers", lambda self, tr_id: {"tr_id": tr_id})

    client = KisUSClient(env="practice", offline=False)
    client.get_us_fills_today()

    assert "SORT_SQN" in captured["params"], (
        "SORT_SQN must be present in get_us_fills_today params"
    )


def test_get_us_orderable_cash_source_has_item_cd():
    """kis_us_client.py 소스에 ITEM_CD 파라미터 설정 코드가 있어야 한다."""
    import trader.us.execution.kis_us_client as mod
    src_file = inspect.getfile(mod)
    with open(src_file, encoding="utf-8") as f:
        source = f.read()
    assert '"ITEM_CD"' in source or "'ITEM_CD'" in source, (
        "kis_us_client.py must set ITEM_CD in get_us_orderable_cash"
    )


def test_get_us_fills_today_source_has_ccld_nccs():
    """kis_us_client.py 소스에 CCLD_NCCS_DVSN 설정 코드가 있어야 한다."""
    import trader.us.execution.kis_us_client as mod
    src_file = inspect.getfile(mod)
    with open(src_file, encoding="utf-8") as f:
        source = f.read()
    assert "CCLD_NCCS_DVSN" in source, (
        "kis_us_client.py must set CCLD_NCCS_DVSN in get_us_fills_today"
    )


def test_cancel_us_order_uses_paper_rvsecncl_contract(monkeypatch):
    from trader.us.execution import kis_us_client as mod

    client = object.__new__(mod.KisUSClient)
    client._cano = "12345678"
    client._acnt_prdt_cd = "01"
    client._assert_not_offline = lambda *_args, **_kwargs: None
    client._build_headers = lambda tr_id: {"tr_id": tr_id}
    captured = {}

    def fake_post(path, headers, body):
        captured.update({"path": path, "headers": headers, "body": body})
        return {"rt_cd": "0", "output": {"ODNO": "cancel-ack"}}

    client._post = fake_post
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setattr(mod.us_cfg, "assert_us_paper_order_allowed", lambda: None)

    result = client.cancel_us_order(
        symbol="TQQQ", exchange="NASDAQ", order_no="original-broker-order"
    )
    assert result["rt_cd"] == "0"
    assert captured["headers"]["tr_id"] == "VTTT1004U"
    assert captured["path"] == "/uapi/overseas-stock/v1/trading/order-rvsecncl"
    assert captured["body"] == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "OVRS_EXCG_CD": "NASD",
        "PDNO": "TQQQ",
        "ORGN_ODNO": "original-broker-order",
        "RVSE_CNCL_DVSN_CD": "02",
        "ORD_QTY": "0",
        "OVRS_ORD_UNPR": "0",
        "MGCO_APTM_ODNO": "",
        "ORD_SVR_DVSN_CD": "0",
    }
