from scripts.us_original_order_readonly_probe import classify_original_order


DATE = "2026-10-30"
NO = "0000036019"


def row(*, filled="0", remaining="1", status=None):
    d = {"odno": NO, "pdno": "TMO", "sll_buy_dvsn_cd": "01",
         "ft_ord_qty": "1", "ft_ccld_qty": filled, "nccs_qty": remaining,
         "ord_dt": "20261030", "ord_tmd": "223010",
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


def test_original_date_mismatch_cannot_prove_broker_order():
    wrong_day = row(filled="1", remaining="0")
    wrong_day["ord_dt"] = "20261029"
    assert result([wrong_day])["classification"] == "UNKNOWN"


def test_cancel_request_row_is_not_original_order_terminal_proof():
    cancel_request = row(filled="0", remaining="0", status="CANCELLED")
    cancel_request["rvse_cncl_dvsn_cd"] = "02"
    cancel_request["orgn_odno"] = "0000088888"
    assert result([cancel_request])["classification"] == "UNKNOWN"


def test_missing_raw_remaining_qty_must_not_use_synthetic_open_qty():
    unverified = row(filled="0", remaining="1")
    unverified.pop("nccs_qty")
    assert result([unverified])["classification"] == "UNKNOWN"

def test_impossible_kst_clock_for_us_trade_session_is_unknown():
    bad = row(filled="1", remaining="0")
    bad["ord_tmd"] = "093010"  # belongs to preceding US session
    assert result([bad])["classification"] == "UNKNOWN"
