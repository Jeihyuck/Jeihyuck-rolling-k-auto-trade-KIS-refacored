from scripts.generate_portfolio_pnl_report import _pick_kr_cash_from_balance_output2


def test_kr_cash_fallback_skips_zero_nxdy_auto_rdpt_amt():
    output2 = {
        "nxdy_auto_rdpt_amt": "0",
        "dnca_tot_amt": "100770713",
        "nxdy_excc_amt": "100887216",
        "prvs_rcdl_excc_amt": "100887216",
    }

    cash, source = _pick_kr_cash_from_balance_output2(output2)

    assert cash == 100887216.0
    assert source == "kis_balance.nxdy_excc_amt"


def test_kr_cash_uses_dnca_when_next_day_cash_missing():
    output2 = {
        "nxdy_auto_rdpt_amt": "0",
        "nxdy_excc_amt": "0",
        "prvs_rcdl_excc_amt": "0",
        "dnca_tot_amt": "100770713",
    }

    cash, source = _pick_kr_cash_from_balance_output2(output2)

    assert cash == 100770713.0
    assert source == "kis_balance.dnca_tot_amt"


def test_kr_cash_returns_none_when_all_zero():
    output2 = {
        "nxdy_auto_rdpt_amt": "0",
        "nxdy_excc_amt": "0",
        "prvs_rcdl_excc_amt": "0",
        "dnca_tot_amt": "0",
        "tot_evlu_amt": "0",
    }

    cash, source = _pick_kr_cash_from_balance_output2(output2)

    assert cash is None
    assert source == "unavailable"
