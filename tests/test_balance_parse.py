from trader.balance_utils import extract_dnca_tot_amt, sanitize_balance_snapshot
from trader.pb1_engine import _extract_dnca_tot_amt


def test_extract_dnca_tot_amt_from_output2_list():
    snapshot = {"output2": [{"dnca_tot_amt": "1,234"}]}

    assert extract_dnca_tot_amt(snapshot) == 1234
    assert _extract_dnca_tot_amt(snapshot) == 1234


def test_sanitize_balance_snapshot_does_not_mutate_original():
    original = {"output2": [{"dnca_tot_amt": "1000"}], "output1": [{"pdno": "005930"}]}
    copy_before = {"output2": [{"dnca_tot_amt": "1000"}], "output1": [{"pdno": "005930"}]}

    sanitized = sanitize_balance_snapshot(original)

    assert original == copy_before
    assert sanitized != original
