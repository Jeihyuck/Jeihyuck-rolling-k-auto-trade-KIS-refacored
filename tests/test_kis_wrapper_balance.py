from types import SimpleNamespace

import pytest

from trader.kis_wrapper import KisAPI, KisBalanceUnavailable
import trader.kis_wrapper as kis_wrapper


def test_inquire_balance_page_uses_meta_on_valid_account(monkeypatch):
    api = KisAPI.__new__(KisAPI)
    api.env = "practice"
    api.CANO = "12345678"
    api.ACNT_PRDT_CD = "01"

    monkeypatch.setattr(kis_wrapper, "_pick_tr", lambda env, name: ["VTTC8434R"])
    api._account_param_meta = lambda: {
        "env": "practice",
        "cano_len": 8,
        "acnt_prdt_cd_len": 2,
        "cano_masked": "***5678",
        "acnt_prdt_cd_masked": "**1",
    }
    api._validate_account_params = lambda: (True, "ok")
    api._headers = lambda tr: {"tr_id": tr}
    api._safe_request = lambda *args, **kwargs: SimpleNamespace(json=lambda: {"rt_cd": "0", "output1": [], "output2": {}})

    payload = api._inquire_balance_page("", "")

    assert payload["rt_cd"] == "0"


def test_inquire_balance_all_logs_stacktrace_on_page_exception(monkeypatch):
    api = KisAPI.__new__(KisAPI)

    monkeypatch.setattr(kis_wrapper, "kis_http_enabled", lambda: True)
    monkeypatch.setattr(kis_wrapper.time, "sleep", lambda *_args, **_kwargs: None)

    calls: list[tuple] = []

    def fake_exception(message, *args, **kwargs):
        calls.append((message, args, kwargs))

    monkeypatch.setattr(kis_wrapper.logger, "exception", fake_exception)
    api._inquire_balance_page = lambda fk, nk: (_ for _ in ()).throw(UnboundLocalError("meta"))

    with pytest.raises(KisBalanceUnavailable, match="meta"):
        api.inquire_balance_all(max_empty_retry=0)

    assert calls
    assert calls[0][0] == "[BALANCE][PAGE_EXCEPTION] ctx_fk=%s ctx_nk=%s retry=%s/%s"
    assert calls[0][1] == ("", "", 0, 0)


def test_summarize_balance_output2_prefers_orderable_cash_fields():
    summary = kis_wrapper._summarize_balance_output2(
        {
            "dnca_tot_amt": "1000000",
            "ord_psbl_cash": "750000",
            "scts_evlu_amt": "250000",
            "tot_evlu_amt": "1250000",
        }
    )

    assert summary["cash_total"] == 1000000
    assert summary["order_possible_cash"] == 750000
    assert summary["market_value"] == 250000
    assert summary["total_asset"] == 1250000
    assert summary["parser"] == "output2_dict"


def test_inquire_daily_ccld_timeout_fail_open(monkeypatch):
    api = KisAPI.__new__(KisAPI)
    api.env = "practice"
    api.CANO = "12345678"
    api.ACNT_PRDT_CD = "01"
    api._headers = lambda tr: {"tr_id": tr}
    api.refresh_token = lambda: None
    api._reset_session = lambda: None
    api.session = SimpleNamespace(
        request=lambda *args, **kwargs: (_ for _ in ()).throw(kis_wrapper.requests.exceptions.Timeout("boom"))
    )

    monkeypatch.setattr(kis_wrapper, "_pick_tr", lambda env, name: ["VTTC8001R"])
    monkeypatch.setattr(kis_wrapper, "kis_http_enabled", lambda: True)
    monkeypatch.setattr(kis_wrapper.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("KIS_CCLD_RETRY_MAX", "2")
    monkeypatch.setenv("KIS_CCLD_READ_TIMEOUT_SEC", "7")
    monkeypatch.setenv("KIS_CCLD_FAIL_OPEN", "1")

    payload = api.inquire_daily_ccld(start_date="20260605", end_date="20260605")

    assert payload["output1"] == []
    assert payload["_ccld_status"] == "TIMEOUT"
    assert payload["_fill_source"] == "daily_ccld_fail_open"