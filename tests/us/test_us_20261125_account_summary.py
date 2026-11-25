from trader.us.account_summary import parse_practice_present_balance
from trader.us.execution.kis_us_client import KisUSClient


def test_practice_present_balance_reports_krw_without_fabricating_usd_or_net_profit():
    result = parse_practice_present_balance({"rt_cd": "0", "output3": {
        "tot_asst_amt": "300000000", "tot_dncl_amt": "0", "tot_evlu_pfls_amt": "-5000",
        "frcr_evlu_tota": "20000",
    }})
    assert result["status"] == "OBSERVED_REPORT_ONLY"
    assert result["broker_reported_total_assets_krw"] == 300000000
    assert result["broker_reported_total_deposits_krw"] == 0
    assert result["broker_reported_valuation_pnl_krw"] == -5000
    assert result["cash_usd"] is None and result["account_equity_usd"] is None
    assert result["net_epoch_pnl_usd"] is None


def test_present_balance_empty_or_partial_is_not_truth():
    for raw in ({}, {"rt_cd": "0", "output3": {}},
                {"rt_cd": "0", "output3": {"tot_asst_amt": "0"}},
                {"rt_cd": "0", "_response_meta": {"tr_cont": "M"},
                 "output3": {"tot_asst_amt": "1000"}}):
        assert parse_practice_present_balance(raw)["status"] == "UNAVAILABLE"


def test_practice_present_balance_get_uses_official_endpoint(monkeypatch):
    client = KisUSClient(env="practice", offline=True)
    monkeypatch.setattr(client, "_assert_not_offline", lambda _: None)
    monkeypatch.setattr(client, "_build_headers", lambda tr: {"tr_id": tr})
    observed = {}
    def fake_get(path, headers, params, **kw):
        observed.update(path=path, headers=headers, params=params, kw=kw)
        return {"rt_cd": "0"}
    monkeypatch.setattr(client, "_get", fake_get)
    assert client.get_us_present_balance()["rt_cd"] == "0"
    assert observed["path"] == "/uapi/overseas-stock/v1/trading/inquire-present-balance"
    assert observed["headers"]["tr_id"] == "VTRP6504R"
    assert observed["params"]["WCRC_FRCR_DVSN_CD"] == "01"
    assert observed["params"]["NATN_CD"] == "840"


def test_explicit_usd_currency_row_is_reporting_evidence_only():
    raw = {"rt_cd": "0", "output3": {"tot_asst_amt": "300000000"},
           "output2": [{"crcy_cd": "USD", "frcr_dncl_amt_2": "1234.56",
                        "frcr_drwg_psbl_amt_1": "1000"},
                       {"crcy_cd": "HKD", "frcr_dncl_amt_2": "9999"}]}
    observed = parse_practice_present_balance(raw)
    assert observed["broker_reported_usd_deposits"] == 1234.56
    assert observed["broker_reported_usd_withdrawable"] == 1000
    assert observed["cash_usd"] is None and observed["account_equity_usd"] is None
    raw["output2"].append(dict(raw["output2"][0]))
    observed = parse_practice_present_balance(raw)
    assert observed["broker_reported_usd_deposits"] is None
    assert observed["usd_currency_observation_status"] == "UNAVAILABLE"


def test_malformed_pagination_metadata_cannot_supply_summary():
    assert parse_practice_present_balance({
        "rt_cd": "0", "output3": {"tot_asst_amt": "300000000"},
        "_response_meta": "malformed",
    })["status"] == "UNAVAILABLE"
