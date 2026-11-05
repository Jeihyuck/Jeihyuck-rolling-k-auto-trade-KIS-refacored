from scripts.us_original_order_readonly_probe import classify_original_order


DATE = "2026-10-30"
NO = "0000036019"


def row(*, filled="0", remaining="1", status=None):
    d = {"odno": NO, "pdno": "TMO", "sll_buy_dvsn_cd": "01",
         "ft_ord_qty": "1", "ft_ccld_qty": filled, "nccs_qty": remaining,
         "ord_dt": "20261030", "ord_tmd": "093010",
         "dmst_ord_dt": "20261030"}
    if status is not None:
        d["ord_sttus"] = status
    return d


def result(rows):
    return classify_original_order(rows, order_no=NO, symbol="TMO",
                                   expected_qty=1, trade_date=DATE)


def test_tmo_open_original_broker_evidence():
    assert result([row()])["classification"] == "PROVEN_OPEN"


def test_unmatched_or_ambiguous_remains_unknown():
    assert result([])["classification"] == "UNKNOWN"
    assert result([row(), row()])["classification"] == "UNKNOWN"


def test_terminal_requires_original_broker_quantity_and_status():
    assert result([row(filled="1", remaining="0")])["classification"] == "PROVEN_FILLED"
    assert result([row(filled="0", remaining="0", status="CANCELLED")])["classification"] == "PROVEN_CANCELLED"
    assert result([row(filled="0", remaining="0")])["classification"] == "UNKNOWN"
    assert result([row(filled="x", remaining="0")])["classification"] == "UNKNOWN"


def test_never_reattributes_wrong_order_or_symbol():
    bad = row()
    bad["pdno"] = "AMD"
    assert result([bad])["classification"] == "UNKNOWN"
