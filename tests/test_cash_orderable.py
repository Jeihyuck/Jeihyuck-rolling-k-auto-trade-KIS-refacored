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


def test_negative_values_with_commas_are_clamped_to_zero():
    cash, meta = _parse({"dnca_tot_amt": "-1,000"})

    assert cash == 0
    assert meta["clamp_applied"] is True
    assert meta["selected_key"] == "dnca_tot_amt"


def test_psbl_order_parser_prefers_ord_psbl_cash():
    api = KisAPI.__new__(KisAPI)
    cash, meta = api._parse_cash_from_psbl_order({"output": {"ord_psbl_cash": "10,000", "ord_psbl_amt": "9000"}})

    assert cash == 10000
    assert meta["selected_key"] == "ord_psbl_cash"
    assert meta["clamp_applied"] is False


def test_get_orderable_cash_prefers_psbl_order(monkeypatch):
    api = KisAPI.__new__(KisAPI)
    api._last_cash = None
    api.CANO = ""
    api.ACNT_PRDT_CD = ""
    api.env = "practice"

    def fake_psbl(code, price):
        return {"output": {"ord_psbl_cash": "7,000", "dnca_tot_amt": "100"}}

    def fake_balance():
        return {"output2": {"dnca_tot_amt": "123"}}

    api._inquire_psbl_order = fake_psbl  # type: ignore[method-assign]
    api.inquire_balance_all = fake_balance  # type: ignore[method-assign]

    cash, meta = api.get_orderable_cash(code_hint="000001", price_hint=100.0)

    assert cash == 7000
    assert meta["source"] == "psbl_order"
    assert meta["selected_key"] == "ord_psbl_cash"
