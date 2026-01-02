from trader.kis_wrapper import KisAPI


def _parse(out2):
    api = KisAPI.__new__(KisAPI)
    return api._parse_cash_from_output2(out2)


def test_negative_dnca_tot_amt_is_clamped_to_zero():
    cash, meta = _parse({"dnca_tot_amt": "-1000"})

    assert cash == 0
    assert meta["clamp_applied"] is True
    assert meta["selected_key"] == "dnca_tot_amt"


def test_ord_psbl_cash_is_preferred_over_dnca_tot_amt():
    cash, meta = _parse({"ord_psbl_cash": "5000", "dnca_tot_amt": "-9999"})

    assert cash == 5000
    assert meta["clamp_applied"] is False
    assert meta["selected_key"] == "ord_psbl_cash"
