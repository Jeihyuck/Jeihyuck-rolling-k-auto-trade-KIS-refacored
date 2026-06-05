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